package com.bnp.str.utilities.audit

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.io.{PrintWriter, StringWriter}
import java.net.InetAddress
import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.UUID

/**
 * Reusable, JOB-AGNOSTIC run auditor for the whole jar. Any driver (tseadfwd or a future sibling
 * module) records its execution to the shared `run_history` ORC external table with one call at
 * start and one at the end:
 *
 * {{{
 *   val audit = RunAudit.start("tseadfwd", "TS_EAD_FWD", getClass.getName, auditConfig)(spark)
 *   try {
 *     val df = run()
 *     audit.succeeded(outputCount = Some(df.count()), outputPath = out)
 *   } catch { case e: Throwable => audit.failed(e); throw e }
 * }}}
 *
 * Design notes:
 *  - STORAGE: an EXTERNAL Hive table stored as ORC, PARTITIONED BY (module, runid) — see
 *    [[RunAuditStore]]. Each run owns one partition; the RUNNING row written at start is replaced
 *    in place by the final SUCCESS/FAILED row (dynamic partition overwrite), and concurrent
 *    runs/IHM never touch each other's partition.
 *  - NEVER BREAKS THE JOB: all audit IO is guarded; a failure to write only logs a warning.
 *  - IDENTITY (who / which IHM) is resolved per field: explicit override > `-Drun.*` system property
 *    > `RUN_*` env var > config > sensible default — so each IHM stamps its identity via JVM args or
 *    env without any code change here.
 */
class RunAudit private(private var record: RunAuditRecord,
                       table: String,
                       location: String,
                       enabled: Boolean,
                       startNanos: Long)
                      (implicit spark: SparkSession) {

  import RunAudit.log

  /** Unique id of this run (echo it back to the IHM / logs to correlate). */
  def runId: String = record.runId

  /** The current (possibly not-yet-finished) record. */
  def current: RunAuditRecord = record

  /** Mark the run SUCCESS and persist the final record. Returns the finalized record. */
  def succeeded(outputCount: Option[Long] = None,
                inputCount: Option[Long] = None,
                outputPath: String = null): RunAuditRecord =
    finish(RunStatus.SUCCESS, None, outputCount, inputCount, outputPath)

  /** Mark the run FAILED (capturing the throwable) and persist the final record. */
  def failed(t: Throwable,
             outputCount: Option[Long] = None,
             inputCount: Option[Long] = None,
             outputPath: String = null): RunAuditRecord =
    finish(RunStatus.FAILED, Option(t), outputCount, inputCount, outputPath)

  private def finish(status: String,
                     error: Option[Throwable],
                     outputCount: Option[Long],
                     inputCount: Option[Long],
                     outputPath: String): RunAuditRecord = {
    val end = Instant.now()
    record = record.copy(
      status          = status,
      endTs           = Some(RunAudit.iso(end)),
      durationMs      = Some((System.nanoTime() - startNanos) / 1000000L),
      outputCount     = outputCount.orElse(record.outputCount),
      inputCount      = inputCount.orElse(record.inputCount),
      outputPath      = Option(outputPath).getOrElse(record.outputPath),
      errorType       = error.map(_.getClass.getName),
      errorMessage    = error.flatMap(e => Option(e.getMessage)),
      errorStacktrace = error.map(e => RunAudit.stackTrace(e))
    )
    persist()
    record
  }

  /** Write the current record to its (module, runid) ORC partition. Guarded: never propagates. */
  private def persist(): Unit = {
    if (!enabled) return
    try {
      RunAuditStore.write(record, table, location)
      log.info(s"[audit] run ${record.runId} (${record.module}/${record.jobName}) " +
        s"status=${record.status} -> $table")
    } catch {
      case e: Throwable =>
        log.warn(s"[audit] failed to write run_history for run ${record.runId} " +
          s"(${record.module}/${record.jobName}); continuing without audit: " +
          s"${e.getClass.getSimpleName}: ${e.getMessage}")
    }
  }
}

object RunAudit {

  private val log = LoggerFactory.getLogger(classOf[RunAudit])
  private val ISO = DateTimeFormatter.ISO_INSTANT

  private def iso(i: Instant): String = ISO.format(i)

  private def stackTrace(t: Throwable, maxChars: Int = 8000): String = {
    val sw = new StringWriter()
    t.printStackTrace(new PrintWriter(sw))
    val s = sw.toString
    if (s.length <= maxChars) s else s.substring(0, maxChars) + "...[truncated]"
  }

  /**
   * Begin auditing a run: builds the record, stamps identity/environment, and writes an initial
   * RUNNING row so an interrupted run is still visible. Returns a handle to finish with
   * [[RunAudit.succeeded]] / [[RunAudit.failed]].
   *
   * @param module         logical module/package that owns the job (PARTITION), e.g. "tseadfwd"
   * @param jobName        logical job name, e.g. "TS_EAD_FWD"
   * @param jobClass       fully-qualified driver class (usually `this.getClass.getName`)
   * @param auditConfig    the `audit { }` config block (keys: enabled, table, root/location,
   *                       environment, appVersion, triggeredBy, launchChannel)
   * @param configPath     path of the application.conf used (for traceability); optional
   * @param paramsJson     compact JSON snapshot of key run params (build with [[AuditJson.obj]])
   * @param inputCount     input record/file count if known at start
   * @param outputPath     intended output path if known at start
   * @param triggeredBy    override for the launching user/service (else resolved from -D/env/config)
   * @param launchChannel  override for the launching IHM/entry point (else resolved from -D/env/config)
   * @param spark          the SparkSession (Hive-enabled) used to write the ORC external table
   */
  def start(module: String,
            jobName: String,
            jobClass: String,
            auditConfig: Config,
            configPath: String = "",
            paramsJson: String = "{}",
            inputCount: Option[Long] = None,
            outputPath: String = "",
            triggeredBy: Option[String] = None,
            launchChannel: Option[String] = None)
           (implicit spark: SparkSession): RunAudit = {

    val enabled  = getBool(auditConfig, "enabled", default = true)
    val table    = getStr(auditConfig, "table", default = RunAuditStore.DEFAULT_TABLE)
    val location = normalizeLocation(resolveRoot(auditConfig))
    val now      = Instant.now()

    val record = RunAuditRecord(
      module        = module,
      runId         = UUID.randomUUID().toString,
      jobName       = jobName,
      jobClass      = jobClass,
      appVersion    = getStr(auditConfig, "appVersion", default = "UNKNOWN"),
      status        = RunStatus.RUNNING,
      triggeredBy   = resolve(triggeredBy, "run.triggeredBy", "RUN_TRIGGERED_BY",
                              auditConfig, "triggeredBy", System.getProperty("user.name", "UNKNOWN")),
      launchChannel = resolve(launchChannel, "run.launchChannel", "RUN_LAUNCH_CHANNEL",
                              auditConfig, "launchChannel", "UNKNOWN"),
      environment   = resolve(None, "run.env", "RUN_ENV", auditConfig, "environment", "UNKNOWN"),
      hostname      = hostName,
      configPath    = configPath,
      params        = if (paramsJson == null || paramsJson.trim.isEmpty) "{}" else paramsJson,
      inputCount    = inputCount,
      outputCount   = None,
      outputPath    = outputPath,
      startTs       = iso(now),
      endTs         = None,
      durationMs    = None,
      errorType     = None,
      errorMessage  = None,
      errorStacktrace = None,
      runDate       = now.atOffset(ZoneOffset.UTC).toLocalDate.toString
    )

    val audit = new RunAudit(record, table, location, enabled, System.nanoTime())
    audit.persist() // initial RUNNING row
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
      .orElse(if (cfg.hasPath(cfgKey)) Some(cfg.getString(cfgKey)).filter(nonBlank) else None)
      .getOrElse(default)

  private def nonBlank(s: String): Boolean = s != null && s.trim.nonEmpty

  private def getStr(cfg: Config, key: String, default: String): String =
    if (cfg.hasPath(key)) cfg.getString(key) else default

  private def getBool(cfg: Config, key: String, default: Boolean): Boolean =
    if (cfg.hasPath(key)) cfg.getBoolean(key) else default

  private def hostName: String =
    try InetAddress.getLocalHost.getHostName
    catch { case _: Throwable => Option(System.getenv("HOSTNAME")).getOrElse("UNKNOWN") }
}
