package com.bnp.str.utilities.audit

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import java.util.Collections

/**
 * Storage layer for the `run_history` audit: ORC files laid out as Hive-style partitions
 * `module=…/runid=…` under a fixed location, exposed as an EXTERNAL Hive table PARTITIONED BY
 * (module, runid). One partition per run.
 *
 * Two concerns are deliberately split:
 *  1. DATA WRITE — Spark's ORC datasource with `partitionBy("module","runid")` and dynamic
 *     partition overwrite (spark.sql.sources.partitionOverwriteMode = dynamic). This needs NO
 *     metastore, so it works the same locally and on the cluster, and the RUNNING row written at
 *     start is replaced in place by the final SUCCESS/FAILED row (only that run's partition).
 *  2. TABLE REGISTRATION — `CREATE EXTERNAL TABLE IF NOT EXISTS … STORED AS ORC` + `ADD PARTITION`.
 *     BEST-EFFORT and guarded: if the metastore is unavailable (e.g. a local box without Hadoop
 *     native IO), the ORC data is still written and the run still audited; only the catalog entry
 *     is skipped (with a warning). On a Hive-enabled cluster the table and partitions register.
 *
 * The table is EXTERNAL, so dropping it never deletes the ORC data and a fresh metastore can be
 * re-pointed at the same location.
 */
object RunAuditStore {

  private val log = LoggerFactory.getLogger(getClass)

  final val DEFAULT_TABLE = "run_history"

  /** Non-partition (data) columns, in table order. */
  val dataSchema: StructType = StructType(Seq(
    StructField("job_name",         StringType),
    StructField("job_class",        StringType),
    StructField("app_version",      StringType),
    StructField("status",           StringType),
    StructField("triggered_by",     StringType),
    StructField("launch_channel",   StringType),
    StructField("environment",      StringType),
    StructField("hostname",         StringType),
    StructField("config_path",      StringType),
    StructField("params",           StringType),
    StructField("input_count",      LongType),
    StructField("output_count",     LongType),
    StructField("output_path",      StringType),
    StructField("start_ts",         StringType),
    StructField("end_ts",           StringType),
    StructField("duration_ms",      LongType),
    StructField("error_type",       StringType),
    StructField("error_message",    StringType),
    StructField("error_stacktrace", StringType),
    StructField("run_date",         StringType)
  ))

  /** Full write schema: data columns first, then the partition columns (module, runid) LAST. */
  val schema: StructType = dataSchema.add("module", StringType).add("runid", StringType)

  private def sqlType(dt: DataType): String = dt match {
    case LongType => "BIGINT"
    case _        => "STRING"
  }

  private def ddlColumns: String =
    dataSchema.fields.map(f => s"`${f.name}` ${sqlType(f.dataType)}").mkString(",\n  ")

  /** DDL that registers the EXTERNAL ORC table (partitioned by module, runid) at `location`. */
  def createTableDDL(table: String, location: String): String =
    s"""CREATE EXTERNAL TABLE IF NOT EXISTS $table (
       |  $ddlColumns
       |)
       |PARTITIONED BY (`module` STRING, `runid` STRING)
       |STORED AS ORC
       |LOCATION '$location'""".stripMargin

  /** Write (or overwrite) this run's ORC partition, then best-effort register it in the metastore. */
  def write(record: RunAuditRecord, table: String, location: String)(implicit spark: SparkSession): Unit = {
    def box(o: Option[Long]): java.lang.Long = o.map(java.lang.Long.valueOf).orNull

    val row = Row(
      record.jobName, record.jobClass, record.appVersion, record.status,
      record.triggeredBy, record.launchChannel, record.environment, record.hostname,
      record.configPath, record.params,
      box(record.inputCount), box(record.outputCount), record.outputPath,
      record.startTs, record.endTs.orNull, box(record.durationMs),
      record.errorType.orNull, record.errorMessage.orNull, record.errorStacktrace.orNull,
      record.runDate,
      record.module, record.runId
    )

    val df = spark.createDataFrame(Collections.singletonList(row), schema)
    // ORC datasource write; dynamic overwrite -> only this (module, runid) partition is replaced.
    df.write.mode("overwrite").format("orc").partitionBy("module", "runid").save(location)

    registerPartition(table, location, record.module, record.runId)
  }

  /** Best-effort: ensure the external table exists and this run's partition is registered. */
  private def registerPartition(table: String, location: String, module: String, runid: String)
                               (implicit spark: SparkSession): Unit =
    try {
      spark.sql(createTableDDL(table, location))
      spark.sql(s"ALTER TABLE $table ADD IF NOT EXISTS PARTITION (`module`='$module', `runid`='$runid')")
    } catch {
      case e: Throwable =>
        log.warn(s"[audit] ORC written to $location but external table '$table' registration skipped " +
          s"(no metastore?): ${e.getClass.getSimpleName}: ${e.getMessage}")
    }

  /** Read the full run_history via the catalog (all modules, all runs). Requires the table to exist. */
  def read(table: String = DEFAULT_TABLE)(implicit spark: SparkSession): DataFrame =
    spark.table(table)

  /** Read the run_history directly from ORC files at `location` (no metastore needed). */
  def readFiles(location: String)(implicit spark: SparkSession): DataFrame =
    spark.read.schema(schema).orc(location)
}
