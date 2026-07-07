package com.bnp.str.utilities.audit

/** Lifecycle status of a single job run recorded in run_history. */
object RunStatus {
  final val RUNNING = "RUNNING" // written at start; a leftover RUNNING row = a run that never finished (killed / crashed JVM)
  final val SUCCESS = "SUCCESS"
  final val FAILED  = "FAILED"
}

/**
 * One row of the `run_history` audit — a single execution of ANY job/driver packaged in the jar.
 * Job-agnostic on purpose: every field is generic so the same table serves every job launched by
 * every IHM. Persisted as an ORC-backed external Hive table, PARTITIONED BY (module, runid) — see
 * [[RunAuditStore]].
 *
 * SCHEMA (column : type — meaning)
 * ---------------------------------------------------------------------------------------------
 *  module           : string        logical module/package that owns the job, e.g. "tseadfwd" (PARTITION)
 *  runid            : string(UUID)  unique id of this run (PARTITION, primary key)
 *  job_name         : string        logical job, e.g. "TS_EAD_FWD"
 *  job_class        : string        fully-qualified driver class that ran
 *  app_version      : string        application / jar version
 *  status           : string        RUNNING | SUCCESS | FAILED
 *  triggered_by     : string        user / service account that launched the run
 *  launch_channel   : string        which IHM / entry point launched it (e.g. "IHM_RISK", "SCHEDULER", "CLI")
 *  environment      : string        DEV | UAT | PROD | LOCAL
 *  hostname         : string        machine that executed the driver
 *  config_path      : string        path of the application.conf used
 *  params           : string(JSON)  snapshot of the key run parameters (free-form per job)
 *  input_count      : bigint?       number of input records/files read (null if not measured)
 *  output_count     : bigint?       number of output rows written (null if not measured)
 *  output_path      : string        where the job wrote its output
 *  start_ts         : string(ISO)   run start instant (UTC ISO-8601)
 *  end_ts           : string(ISO)?  run end instant (null while RUNNING)
 *  duration_ms      : bigint?       wall-clock duration (null while RUNNING)
 *  error_type       : string?       exception class on failure (null otherwise)
 *  error_message    : string?       exception message on failure (null otherwise)
 *  error_stacktrace : string?       truncated stack trace on failure (null otherwise)
 *  run_date         : string(date)  yyyy-MM-dd of start (regular column, handy for filtering)
 * ---------------------------------------------------------------------------------------------
 */
case class RunAuditRecord(
                           module: String,
                           runId: String,
                           jobName: String,
                           jobClass: String,
                           appVersion: String,
                           status: String,
                           triggeredBy: String,
                           launchChannel: String,
                           environment: String,
                           hostname: String,
                           configPath: String,
                           params: String,
                           inputCount: Option[Long],
                           outputCount: Option[Long],
                           outputPath: String,
                           startTs: String,
                           endTs: Option[String],
                           durationMs: Option[Long],
                           errorType: Option[String],
                           errorMessage: Option[String],
                           errorStacktrace: Option[String],
                           runDate: String
                         )
