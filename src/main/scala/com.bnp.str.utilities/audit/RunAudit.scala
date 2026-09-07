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
   * On YARN the submitted application jar is localized on the cluster under the fixed placeholder
   * name `__app__.jar`, so the classloader code source reports that placeholder instead of the real
   * file name. Treat it as "not a real name" so detection falls through to the Spark config.
   */
  private val YarnAppJarPlaceholder = "__app__.jar"

  /**
   * Best-effort FILE NAME of the jar this code was launched from — always a bare name, never a path.
   * Preference order:
   *   1. the classloader code source, following the `__app__.jar` SYMLINK when that is what it
   *      reports — an IDE, a plain `java -jar`, a YARN client run, and YARN CLUSTER mode;
   *   2. the same symlink read straight from the container working directory, for when the code
   *      source is unavailable or reports something else entirely;
   *   3. the application jar named on YARN's distributed cache, `spark.yarn.cache.filenames`;
   *   4. the submitted jar recovered from `spark.jars` / `spark.yarn.dist.jars`.
   *
   * The symlink levels exist because everything else can come up empty on a CLUSTER run, which is
   * how `used_jar` reached "UNKNOWN". There the code source is the `__app__.jar` placeholder;
   * `ApplicationMaster` deletes the `spark.yarn.cache.*` keys from the SparkConf as soon as it has
   * built the executor resources, so level 3 finds nothing by the time user code runs; and Spark
   * does not always copy the primary application resource into `spark.jars`, leaving level 4 empty
   * too. YARN's symlink is the one thing still in place: see [[codeSourceJar]].
   *
   * Only the NAME is kept at every level. The path around it is dropped deliberately — on YARN it is
   * the container-local copy (`/hadoop/yarn/nm/usercache/…/filecache/<id>/…`), whose directory is
   * per-node and per-run because the NodeManager cache slot gets reused, so it resolves nowhere for
   * whoever reads `run_history` later. The name still says which BUILD ran, which is the part worth
   * keeping.
   *
   * Returns None only when all three come up empty, in which case the caller falls back to "UNKNOWN".
   * A launcher can always bypass detection via `-Drun.usedJar` / `RUN_USED_JAR` / config.
   */
  private def detectJar(implicit spark: SparkSession): Option[String] = {
    val detected = codeSourceJar.orElse(appJarSymlinkName).orElse(yarnCacheJar).orElse(jarFromSparkConf)
    if (detected.isEmpty) log.warn(s"[audit] used_jar not detected, recording UNKNOWN. $jarDiagnostics")
    detected
  }

  /**
   * Why detection came up empty, for the run log — every input the three levels read, verbatim.
   *
   * Emitted only on the UNKNOWN path, so it costs nothing on a healthy run. It exists because this
   * cannot be reproduced off-cluster: the answer is in the submitted job's own configuration, and
   * without it any fix is guesswork. Each value is truncated, and the whole thing is guarded — a
   * diagnostic must never be the reason an audit fails.
   */
  private def jarDiagnostics(implicit spark: SparkSession): String = {
    def clip(s: String): String = if (s.length <= 500) s else s.take(500) + "…(truncated)"
    def show(v: Option[String]): String = v.filter(nonBlank).map(x => s"'${clip(x)}'").getOrElse("<absent>")

    val codeSource =
      try Option(classOf[RunAudit].getProtectionDomain.getCodeSource.getLocation.toURI.getPath)
      catch { case e: Throwable => Some(s"<unavailable: ${e.getClass.getSimpleName}>") }

    // Both halves matter: a code source of '__app__.jar' whose canonical path is the SAME string
    // means the symlink is missing or unreadable, which is a different problem from not having one.
    val resolved = codeSource.map(canonicalPath)
    val symlink =
      try {
        val link = new java.io.File(YarnAppJarPlaceholder)
        if (link.exists()) Some(canonicalPath(YarnAppJarPlaceholder)) else Some("<no __app__.jar in cwd>")
      } catch { case e: Throwable => Some(s"<unreadable: ${e.getClass.getSimpleName}>") }

    val confKeys = Seq("spark.yarn.cache.filenames", "spark.jars", "spark.yarn.dist.jars",
                       "spark.submit.deployMode", "spark.master")
    val confDump =
      try {
        val conf = spark.sparkContext.getConf
        confKeys.map(k => s"$k=${show(conf.getOption(k))}").mkString(", ")
      } catch { case e: Throwable => s"<spark conf unavailable: ${e.getClass.getSimpleName}>" }

    s"codeSource=${show(codeSource)}, codeSourceResolved=${show(resolved)}, " +
      s"appJarSymlink=${show(symlink)}, cwd=${show(Option(System.getProperty("user.dir")))}, $confDump. " +
      "Set audit.usedJar (or -Drun.usedJar / RUN_USED_JAR) to record it explicitly."
  }

  /**
   * File name of the jar this code was loaded from, resolving the YARN `__app__.jar` placeholder
   * through the filesystem.
   *
   * YARN localizes the application jar into the NodeManager cache under its REAL name and puts a
   * SYMLINK named `__app__.jar` in the container working directory:
   * {{{
   *   __app__.jar -> /hadoop/yarn/nm/usercache/<user>/filecache/<id>/str-…-RELEASE.jar
   * }}}
   * so following the link recovers the name the placeholder hides. That is the only local source of
   * it under cluster mode: `spark.yarn.cache.filenames` carries it too, but `ApplicationMaster`
   * DELETES those keys from the SparkConf once it has built the executor resources, well before any
   * user code runs — which is why detection reached "UNKNOWN" with the cache lookup in place.
   *
   * The link is followed ONLY when the name is the placeholder. Everywhere else the code source is
   * already the real file (an IDE, a plain `java -jar`, a YARN client run), and resolving it would
   * only turn a perfectly good name into a different one if the deployment happens to symlink its
   * jars.
   */
  private def codeSourceJar: Option[String] =
    try {
      val path = classOf[RunAudit].getProtectionDomain.getCodeSource.getLocation.toURI.getPath
      jarNameFrom(path, canonicalPath)
    } catch { case _: Throwable => None }

  /**
   * The `__app__.jar` symlink in the container working directory, read directly.
   *
   * Same trick as [[codeSourceJar]], but starting from the well-known link name rather than from the
   * classloader — it still answers when the code source is unavailable or reports something else
   * entirely (a shaded launcher, an unusual classloader).
   */
  private def appJarSymlinkName: Option[String] =
    try {
      val link = new java.io.File(YarnAppJarPlaceholder)
      if (link.exists()) jarNameFrom(YarnAppJarPlaceholder, canonicalPath) else None
    } catch { case _: Throwable => None }

  /**
   * Pure core of both: the jar's file name, following `resolve` only when the name is the
   * `__app__.jar` placeholder. Returns None when the result is still the placeholder or is blank —
   * a link that resolves to nothing is not a name.
   *
   * `resolve` is a parameter so the symlink-following branch is testable without creating one, which
   * needs privileges on Windows.
   */
  private[audit] def jarNameFrom(path: String, resolve: String => String): Option[String] = {
    val direct = jarBaseName(path)
    val name = if (direct == YarnAppJarPlaceholder) jarBaseName(resolve(path)) else direct
    Some(name).filter(nonBlank).filterNot(_ == YarnAppJarPlaceholder)
  }

  /** Symlink-resolving absolute path; the input unchanged when it cannot be resolved. */
  private def canonicalPath(path: String): String =
    try new java.io.File(path).getCanonicalPath catch { case _: Throwable => path }

  /**
   * The application jar's name as YARN itself recorded it when distributing the job.
   *
   * `spark.yarn.cache.filenames` is YARN's upload record: one `<source uri>#<localized name>` entry
   * per resource it shipped. The primary application jar is always the one localized as
   * `__app__.jar`, so its entry gives back the real file name that the placeholder hides —
   * `hdfs://…/str-file-transform-engine-1.4.2-RELEASE.jar#__app__.jar` yields
   * `str-file-transform-engine-1.4.2-RELEASE.jar`. Only that basename is taken; the URI it came from
   * is not stored.
   *
   * Deliberately strict: ONLY the entry tagged `#__app__.jar` counts. Falling back to "the first jar
   * in the list" would report a `--jars` dependency as the application, which is worse than the
   * "UNKNOWN" this replaces — a wrong build name reads as fact, a missing one reads as missing.
   */
  private def yarnCacheJar(implicit spark: SparkSession): Option[String] =
    try appJarNameFromCache(spark.sparkContext.getConf.getOption("spark.yarn.cache.filenames").toSeq)
    catch { case _: Throwable => None }

  /** Pure core of [[yarnCacheJar]]: the `#__app__.jar` entry's source basename. */
  private[audit] def appJarNameFromCache(confValues: Seq[String]): Option[String] =
    confValues
      .flatMap(_.split(","))
      .map(_.trim).filter(nonBlank)
      .find(e => linkPart(e) == YarnAppJarPlaceholder)
      .map(e => jarBaseName(uriPart(e)))
      .filter(_.toLowerCase.endsWith(".jar"))
      .filterNot(_ == YarnAppJarPlaceholder)

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
   * Recover the real submitted jar name from the Spark configuration, for the YARN-cluster case where
   * the code source is `__app__.jar`. Scans `spark.jars` then `spark.yarn.dist.jars` for the first
   * `*.jar` basename that is not the placeholder. Best-effort: returns None if nothing usable is found.
   */
  private def jarFromSparkConf(implicit spark: SparkSession): Option[String] =
    try {
      val conf = spark.sparkContext.getConf
      Seq("spark.jars", "spark.yarn.dist.jars")
        .flatMap(k => conf.getOption(k).toSeq)
        .flatMap(_.split(","))
        .map(_.trim).filter(nonBlank)
        .map(jarBaseName)
        .filter(_.toLowerCase.endsWith(".jar"))
        .filterNot(_ == YarnAppJarPlaceholder)
        .headOption
    } catch { case _: Throwable => None }

  /** Basename of a path: everything after the last '/' or '\' (the whole path when neither is present). */
  private[audit] def jarBaseName(path: String): String = {
    if (path == null || path.isEmpty) return ""
    var idx = path.lastIndexOf('/')
    if (idx == -1) idx = path.lastIndexOf('\\')
    path.substring(idx + 1)
  }
}
