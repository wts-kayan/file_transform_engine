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

  /** Mark the run finished (success) and persist the final record. */
  def succeeded(): RunAuditRecord = finish(None)

  /** Mark the run finished after a failure (logs the throwable) and persist the final record. */
  def failed(t: Throwable): RunAuditRecord = finish(Option(t))

  /** Finalize end_date + duration and persist. (No status column in this schema.) */
  def end(): RunAuditRecord = finish(None)

  private def finish(error: Option[Throwable]): RunAuditRecord = {
    val endTs      = new Timestamp(System.currentTimeMillis())
    val durationMs = (System.nanoTime() - startNanos) / 1000000L
    error.foreach(e => log.error(s"[audit] run ${record.runId} (${record.moduleName}) failed", e))
    record = record.copy(
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
      log.info(s"[audit] run ${record.runId} (${record.moduleName}) " +
        s"${if (record.endDate.isDefined) "ended" else "started"} -> $table")
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
   * @param auditConfig      the `audit { }` config block (keys: enabled, table, root/location,
   *                         runId, userLauncher, motor, usedJar — all optional except root when enabled)
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
    val table    = getStr(auditConfig, "table", default = RunAuditStore.DEFAULT_TABLE)
    val location = normalizeLocation(resolveRoot(auditConfig))

    val resolvedRunId =
      runId.filter(nonBlank)
        .orElse(if (auditConfig.hasPath("runId")) Some(auditConfig.getString("runId")).filter(nonBlank) else None)
        .getOrElse(UUID.randomUUID().toString)

    val record = RunAuditRecord(
      runId         = resolvedRunId,
      applicationId = try spark.sparkContext.applicationId catch { case _: Throwable => "UNKNOWN" },
      moduleName    = moduleName,
      usedJar       = usedJar.filter(nonBlank).getOrElse(
                        getOpt(auditConfig, "usedJar").getOrElse(detectJar.getOrElse("UNKNOWN"))),
      usedConf      = usedConf,
      userLauncher  = resolve(userLauncher, "run.userLauncher", "RUN_USER_LAUNCHER",
                              auditConfig, "userLauncher", System.getProperty("user.name", "UNKNOWN")),
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

  /** Best-effort: the jar file this code was loaded from (None in an IDE/classes-dir run). */
  private def detectJar: Option[String] =
    try {
      val loc = classOf[RunAudit].getProtectionDomain.getCodeSource.getLocation
      val name = new java.io.File(loc.toURI).getName
      if (name.toLowerCase.endsWith(".jar")) Some(name) else None
    } catch { case _: Throwable => None }
}
