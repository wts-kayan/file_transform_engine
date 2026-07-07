package com.bnp.str.utilities

import com.bnp.str.utilities.audit.{AuditJson, RunAudit, RunAuditStore}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

/**
 * Standalone smoke test for the shared run-audit component (no tseadfwd pipeline involved).
 * Simulates TWO modules launched by TWO different IHM — one SUCCESS, one FAILED — writing into
 * the ORC-partitioned `run_history` layout, then reads the ORC back and prints rows + the
 * external-table DDL. Uses a plain (non-Hive) session so it verifies the metastore-free data
 * path; the external table registration itself is exercised on a Hive-enabled cluster.
 *
 *   java -cp "target/classes;target/test-classes;$(cat cp.txt)" com.bnp.str.utilities.RunAuditSmokeTest
 */
object RunAuditSmokeTest {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("run-audit-smoke").master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic") // overwrite only a run's own partition
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val table = "run_history_smoke"
    val root  = s"${System.getProperty("user.dir")}/localRun/run_history_smoke"
    org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      .delete(new org.apache.hadoop.fs.Path(root), true)

    val auditConfig = ConfigFactory.parseMap(Map(
      "enabled" -> "true", "table" -> table, "root" -> root,
      "environment" -> "UAT", "appVersion" -> "9.9.9"
    ).asJava)

    // Module A / JOB_A — success, launched by IHM_RISK / alice
    val a = RunAudit.start("modA", "JOB_A", "com.example.JobA", auditConfig,
      paramsJson = AuditJson.obj("as_of" -> "2025Q4", "n" -> 3),
      triggeredBy = Some("alice"), launchChannel = Some("IHM_RISK"))
    a.succeeded(outputCount = Some(1234L), outputPath = "/out/a")

    // Module B / JOB_B — failure, launched by SCHEDULER / svc_batch
    val b = RunAudit.start("modB", "JOB_B", "com.example.JobB", auditConfig,
      triggeredBy = Some("svc_batch"), launchChannel = Some("SCHEDULER"))
    b.failed(new IllegalStateException("boom; with ; and \"quotes\" and\nnewline"))

    println(s"\n>>> wrote runs: A=${a.runId} (SUCCESS, modA), B=${b.runId} (FAILED, modB)")

    val hist = RunAuditStore.readFiles(root).orderBy("module")
    println(s">>> run_history rows (read from ORC): ${hist.count()}")
    hist.select("module", "runid", "job_name", "status", "triggered_by", "launch_channel",
      "output_count", "error_type", "run_date").show(false)
    println(">>> params + error_message round-trip:")
    hist.select("job_name", "params", "error_message").show(false)

    println(">>> ORC partition directories on disk:")
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.globStatus(new org.apache.hadoop.fs.Path(s"$root/module=*/runid=*"))
      .foreach(st => println(s"   ${st.getPath}"))

    println("\n>>> external table DDL (run on the cluster to register it):")
    println(RunAuditStore.createTableDDL(table, root))

    spark.stop()
  }
}
