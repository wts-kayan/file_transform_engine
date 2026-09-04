package com.bnp.str.utilities.audit

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.sql.Timestamp
import java.util.UUID

/**
 * Reusable, MODULE-AGNOSTIC run auditor for the whole jar. Any module (tseadfwd, climatetables,
 * addons, excelor, …) records its execution to the shared `run_history` ORC external table with
 * one call at start and one at the end:
 *
 * {{{
 *   val audit = RunAudit.start("tseadfwd", auditConfig, usedConf = confPath)(spark)
 *   try {
 *     run()
 *     audit.succeeded()
 *   } catch { case e: Throwable => audit.failed(e); throw e }
 * }}}
 *
 * Storage is an EXTERNAL Hive table stored as ORC, PARTITIONED BY (module_name, run_id) — see
 * [[RunAuditStore]]. A row is written at start (end_date/duration null) and replaced in place with
 * the finalized row at the end. All audit IO is guarded: a failure only logs a warning and never
 * breaks the job.
 *
 * Identity/metadata that a launcher supplies is resolved per field:
 * explicit override > `-Drun.*` system property > `RUN_*` env var > config > default.
 */
class RunAudit private(private var record: RunAuditRecord,
                       table: String,
                       location: String,
                       enabled: Boolean,
                       startNanos: Long)
                      (implicit spark: SparkSession) {

  import RunAudit.log

  /** Unique id of this run (echo it back to the launcher / logs to correlate). */
  def runId: String = record.runId

  /** The current (possibly not-yet-finished) record. */
  def current: RunAuditRecord = record

  /** Mark the run SUCCESS and persist the final record. */
  def succeeded(): RunAuditRecord = finish(RunStatus.SUCCESS, None)

  /** Mark the run FAILED (logs the throwable) and persist the final record. */
  def failed(t: Throwable): RunAuditRecord = finish(RunStatus.FAILED, Option(t))

  private def finish(status: String, error: Option[Throwable]): RunAuditRecord = {
    val endTs      = new Timestamp(System.currentTimeMillis())
    val durationMs = (System.nanoTime() - startNanos) / 1000000L
    error.foreach(e => log.error(s"[audit] run ${record.runId} (${record.moduleName}) failed", e))
    record = record.copy(
      status   = status,
      endDate  = Some(endTs),
      duration = Some(RunAudit.formatDuration(durationMs))
    )
    persist()
    record
  }

  /** Write the current record to its (module_name, run_id) ORC partition. Guarded: never propagates. */
  private def persist(): Unit = {
    if (!enabled) return
    try {
      RunAuditStore.write(record, table, location)
      log.info(s"[audit] run ${record.runId} (${record.moduleName}) status=${record.status} -> $table")
    } catch {
      case e: Throwable =>
        log.warn(s"[audit] failed to write run_history for run ${record.runId} " +
          s"(${record.moduleName}); continuing without audit: " +
          s"${e.getClass.getSimpleName}: ${e.getMessage}")
    }
  }
}

object RunAudit {

  private val log = LoggerFactory.getLogger(classOf[RunAudit])

  /** Format an elapsed millisecond count as e.g. "0h 20mn 27s". */
  def formatDuration(ms: Long): String = {
    val totalSec = ms / 1000
    val h = totalSec / 3600
    val m = (totalSec % 3600) / 60
    val s = totalSec % 60
    s"${h}h ${m}mn ${s}s"
  }

  /**
   * Begin auditing a run: build the record, resolve launcher-supplied metadata, and write the
   * initial row (end_date/duration null) so an interrupted run is still visible. Returns a handle
   * to finish with [[succeeded]] / [[failed]].
   *
   * @param moduleName       module that owns the run (PARTITION), e.g. "tseadfwd", "climatetables"
   * @param auditConfig      the `audit { }` config block (keys: enabled, table, database, root/location,
   *                         runId, userLauncher, motor, usedJar — all optional). When `database` is set
   *                         the table LOCATION is derived from that Hive database's own warehouse dir
   *                         (`<db.locationUri>/<table>`); otherwise `root`/`location` is used.
   * @param usedConf         path of the application.conf used
   * @param runId            override for the run id (else config `runId`, else a generated UUID)
   * @param userLauncher     override for the launching user (else -D/env/config, else JVM user.name)
   * @param motor            override for the compute motor (else config `motor`, else "UNKNOWN")
   * @param usedJar          override for the launched jar name (else config `usedJar`, else auto-detected)
   * @param projectionDates  module-specific projection years string, e.g. "[2030, 2040, 2050]"
   * @param scenarios        module-specific scenarios string, e.g. "[\"FW\", \"NZ50\"]"
   * @param baseFolderName   module-specific base folder name
   * @param spark            the SparkSession (Hive-enabled on the cluster) used to write the ORC table
   */
  def start(moduleName: String,
            auditConfig: Config,
            usedConf: String = "",
            runId: Option[String] = None,
            userLauncher: Option[String] = None,
            motor: Option[String] = None,
            usedJar: Option[String] = None,
            projectionDates: Option[String] = None,
            scenarios: Option[String] = None,
            baseFolderName: Option[String] = None)
           (implicit spark: SparkSession): RunAudit = {

    val enabled  = getBool(auditConfig, "enabled", default = true)
    val database = getOpt(auditConfig, "database")
    val rawTable = getStr(auditConfig, "table", default = RunAuditStore.DEFAULT_TABLE)
    val (table, location) = resolveTableAndLocation(auditConfig, database, rawTable)

    val resolvedRunId =
      runId.filter(nonBlank)
        .orElse(if (auditConfig.hasPath("runId")) Some(auditConfig.getString("runId")).filter(nonBlank) else None)
        .getOrElse(UUID.randomUUID().toString)

    val record = RunAuditRecord(
      runId         = resolvedRunId,
      applicationId = try spark.sparkContext.applicationId catch { case _: Throwable => "UNKNOWN" },
      moduleName    = moduleName,
      usedJar       = usedJar.filter(nonBlank)
                        .orElse(Option(System.getProperty("run.usedJar")).filter(nonBlank))
                        .orElse(Option(System.getenv("RUN_USED_JAR")).filter(nonBlank))
                        .orElse(getOpt(auditConfig, "usedJar"))
                        .orElse(detectJar)
                        .getOrElse("UNKNOWN"),
      usedConf      = usedConf,
      userLauncher  = resolve(userLauncher, "run.userLauncher", "RUN_USER_LAUNCHER",
                              auditConfig, "userLauncher", System.getProperty("user.name", "UNKNOWN")),
      status        = RunStatus.RUNNING,
      creationDate  = new Timestamp(System.currentTimeMillis()),
      endDate       = None,
      duration      = None,
      motor         = resolve(motor, "run.motor", "RUN_MOTOR", auditConfig, "motor", "UNKNOWN"),
      projectionDates = projectionDates.filter(nonBlank),
      scenarios       = scenarios.filter(nonBlank),
      baseFolderName  = baseFolderName.filter(nonBlank)
    )

    val audit = new RunAudit(record, table, location, enabled, System.nanoTime())
    audit.persist()
    audit
  }

  // ---- config / identity resolution helpers ----

  /**
   * Resolve the (qualified table name, storage LOCATION) pair used to write the audit ORC.
   *
   * When a Hive `database` is configured, the table lives inside that database and its LOCATION is
   * derived dynamically from the database's own warehouse directory — `<db.locationUri>/<table>` —
   * so nothing is hard-coded and the ORC lands where Hive expects the database's tables:
   * {{{
   *   val dbPath   = spark.catalog.getDatabase(database).locationUri
   *   val location = dbPath + "/" + table.toLowerCase
   * }}}
   * Falls back to the config `root`/`location` (normalized to an absolute local path) with an
   * unqualified table name when no database is set, or when the metastore lookup fails (e.g. a local
   * box without Hive).
   */
  private def resolveTableAndLocation(cfg: Config, database: Option[String], table: String)
                                     (implicit spark: SparkSession): (String, String) =
    database.filter(nonBlank) match {
      case Some(db) =>
        try {
          val dbPath   = spark.catalog.getDatabase(db).locationUri
          val location = s"${dbPath.stripSuffix("/")}/${table.toLowerCase}"
          (s"$db.$table", location)
        } catch {
          case e: Throwable =>
            log.warn(s"[audit] could not resolve location from database '$db' " +
              s"(${e.getClass.getSimpleName}: ${e.getMessage}); falling back to config root")
            (table, normalizeLocation(resolveRoot(cfg)))
        }
      case None =>
        (table, normalizeLocation(resolveRoot(cfg)))
    }

  /** Audit table LOCATION: `root` (preferred) or legacy `location` key. */
  private def resolveRoot(cfg: Config): String =
    if (cfg.hasPath("root")) cfg.getString("root")
    else if (cfg.hasPath("location")) cfg.getString("location")
    else "run_history"

  /** Make a scheme-less relative path absolute so Hive's LOCATION resolves against the local FS. */
  private def normalizeLocation(root: String): String =
    if (root.contains("://")) root
    else new java.io.File(root).getAbsolutePath.replace('\\', '/')

  /** override > -D<sysProp> > env(<envVar>) > config(<cfgKey>) > default. Blanks are ignored. */
  private def resolve(overrideVal: Option[String],
                      sysProp: String,
                      envVar: String,
                      cfg: Config,
                      cfgKey: String,
                      default: String): String =
    overrideVal.filter(nonBlank)
      .orElse(Option(System.getProperty(sysProp)).filter(nonBlank))
      .orElse(Option(System.getenv(envVar)).filter(nonBlank))
      .orElse(getOpt(cfg, cfgKey))
      .getOrElse(default)

  private def getOpt(cfg: Config, key: String): Option[String] =
    if (cfg.hasPath(key)) Some(cfg.getString(key)).filter(nonBlank) else None

  private def nonBlank(s: String): Boolean = s != null && s.trim.nonEmpty

  private def getStr(cfg: Config, key: String, default: String): String =
    if (cfg.hasPath(key)) cfg.getString(key) else default

  private def getBool(cfg: Config, key: String, default: Boolean): Boolean =
    if (cfg.hasPath(key)) cfg.getBoolean(key) else default

  /**
   * The jar this code was launched from, as the DISTRIBUTED-FILESYSTEM PATH IT WAS UPLOADED TO —
   * `hdfs://…` (or any other non-`file:` scheme, so a federated `viewfs://` or an `s3a://` bucket
   * reads the same way). That path identifies the artefact for whoever reads `run_history` later: it
   * can be fetched, checksummed and compared across runs.
   *
   * Nothing else is ever recorded. In particular the classloader code source is NOT, even though it
   * is the obvious place to look: on YARN it reports the container-local copy
   * (`/hadoop/yarn/nm/usercache/…/filecache/<id>/…`, or the bare `__app__.jar` placeholder in cluster
   * mode), and that is per-node and per-run — the NodeManager cache slot is reused for unrelated
   * resources — so it identifies nothing after the fact. It is read only to learn the jar's LOCAL
   * NAME, which is what the upload entry is matched on.
   *
   * Returns None when no upload can be found, and the caller then records "UNKNOWN" rather than
   * something local: an unrunnable path is worse than an honest blank. This is the expected outcome
   * off-cluster (an IDE run, a plain `java -jar`), where there is no upload to name. Pin it there —
   * or anywhere detection comes up short — with `-Drun.usedJar` / `RUN_USED_JAR` / `audit.usedJar`.
   */
  private def detectJar(implicit spark: SparkSession): Option[String] = uploadedJarLocation

  /**
   * Spark configuration keys carrying the ORIGINAL, pre-localization location of the distributed
   * jars, most authoritative first.
   *
   * `spark.yarn.cache.filenames` is the upload record itself: YARN writes one
   * `<source uri>#<localized name>` entry per resource it distributed, so it maps the container-local
   * file straight back to the HDFS path it was uploaded to. The other two carry what was submitted,
   * which is the same URI whenever the jar was already on HDFS.
   */
  private val JarLocationKeys = Seq("spark.yarn.cache.filenames", "spark.jars", "spark.yarn.dist.jars")

  /** A URI with a scheme other than `file:` — i.e. somewhere the jar was uploaded, not a local copy. */
  private def isRemoteUri(s: String): Boolean =
    s.indexOf("://") > 0 && !s.toLowerCase.startsWith("file://")

  /** `<uri>#<link>` -> `<uri>`; an entry with no fragment is the URI itself. */
  private def uriPart(entry: String): String = {
    val i = entry.indexOf('#')
    if (i >= 0) entry.substring(0, i) else entry
  }

  /** `<uri>#<link>` -> `<link>`; with no fragment, the localized name is the URI's own basename. */
  private def linkPart(entry: String): String = {
    val i = entry.indexOf('#')
    if (i >= 0) entry.substring(i + 1) else jarBaseName(entry)
  }

  /**
   * The location the running jar was UPLOADED to, e.g.
   * `hdfs://ns/user/x/.sparkStaging/application_1773889567248_10449/str-file-transform-engine.jar`.
   *
   * On YARN the classloader only ever sees the container-local copy — say
   * `/hadoop/yarn/nm/usercache/<user>/filecache/25008/str-file-transform-engine-1.4.2-RELEASE.jar`,
   * or the bare `__app__.jar` placeholder in cluster mode. That path is per-container and per-run:
   * the `filecache/25008` slot is a NodeManager cache id that means nothing on another node and is
   * reused for something else later, so it cannot be resolved back to a build. The distributed-cache
   * entry can, which is why it is preferred over both fallbacks.
   *
   * Matching is by localized name, so a run with `--jars` does not report a dependency jar as the
   * application one. When nothing matches by name the result is None and the caller falls back to
   * the basename, which is imprecise but never wrong.
   */
  private def uploadedJarLocation(implicit spark: SparkSession): Option[String] =
    try {
      val conf = spark.sparkContext.getConf
      resolveUploadedJar(codeSourceName, JarLocationKeys.flatMap(k => conf.getOption(k).toSeq))
    } catch { case _: Throwable => None }

  /**
   * Pure core of [[uploadedJarLocation]]: pick the remote URI that the localized jar came from.
   *
   * @param localName the container-local jar name the classloader reports (may be `__app__.jar`)
   * @param confValues raw values of [[JarLocationKeys]], in preference order, each comma-separated
   */
  private[audit] def resolveUploadedJar(localName: Option[String], confValues: Seq[String]): Option[String] = {
    val name = localName.map(jarBaseName).filter(nonBlank)
    val entries = confValues
      .flatMap(_.split(","))
      .map(_.trim)
      .filter(nonBlank)
      .filter(e => uriPart(e).toLowerCase.endsWith(".jar"))
      .filter(e => isRemoteUri(uriPart(e)))

    // The localized name matches either the `#link` YARN gave the resource or the source basename —
    // `__app__.jar` only ever matches the former, a real name usually both.
    def sameJar(e: String, n: String): Boolean = linkPart(e) == n || jarBaseName(uriPart(e)) == n

    name match {
      case Some(n) => entries.find(e => sameJar(e, n)).map(uriPart)
      // No local name to match on: only safe when a single remote jar was distributed, otherwise we
      // would be guessing which of them the application was launched from.
      case None => entries.map(uriPart).distinct match {
        case Seq(only) => Some(only)
        case _ => None
      }
    }
  }

  /**
   * The jar's LOCAL name, from the classloader code source — `getProtectionDomain.getCodeSource`,
   * the one place the running JVM knows which file it loaded this class from.
   *
   * Used ONLY to match the upload entry, never as a recorded value: on YARN this is the container
   * copy (`…/filecache/<id>/app.jar`) or the `__app__.jar` placeholder, and the path around the name
   * is meaningless off that node. The placeholder is deliberately kept rather than filtered out —
   * cluster mode has nothing else to match on, and YARN's own cache entry is tagged `#__app__.jar`,
   * so the placeholder is exactly the right key there.
   */
  private def codeSourceName: Option[String] =
    try {
      val path = classOf[RunAudit].getProtectionDomain.getCodeSource.getLocation.toURI.getPath
      Some(jarBaseName(path)).filter(nonBlank)
    } catch { case _: Throwable => None }

  /** Basename of a path: everything after the last '/' or '\' (the whole path when neither is present). */
  private def jarBaseName(path: String): String = {
    if (path == null || path.isEmpty) return ""
    var idx = path.lastIndexOf('/')
    if (idx == -1) idx = path.lastIndexOf('\\')
    path.substring(idx + 1)
  }
}
