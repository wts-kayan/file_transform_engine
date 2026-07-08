package com.bnp.str.utilities

import com.bnp.str.utilities.audit.{RunAudit, RunAuditStore}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

/**
 * Standalone smoke test for the shared run-audit component (no tseadfwd pipeline involved).
 * Simulates TWO modules — climatetables (with the module-specific projection_dates / scenarios /
 * base_folder_name) and tseadfwd — writing into the ORC-partitioned `run_history` layout, then
 * reads the ORC back and prints rows + the external-table DDL. Uses a plain (non-Hive) session so
 * it verifies the metastore-free data path.
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
    val root  = s"${System.getProperty("user.dir")}/localRun/run_history_smoke".replace('\\', '/')
    org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      .delete(new org.apache.hadoop.fs.Path(root), true)

    val auditConfig = ConfigFactory.parseMap(Map(
      "enabled" -> "true", "table" -> table, "root" -> root
    ).asJava)

    // climatetables — with the module-specific columns filled in (matches the ticket example)
    val a = RunAudit.start("climatetables", auditConfig,
      usedConf        = "/Projects/STCreditRisk_UAT/Test_climat_tables/run_1/application_climate_tables_run_2.conf",
      userLauncher    = Some("j03627"),
      motor           = Some("iris"),
      usedJar         = Some("str-file-transform-engine-1.0-RELEASE-Climate-Tables.jar"),
      projectionDates = Some("[2030, 2040, 2050]"),
      scenarios       = Some("[\"FW\", \"NZ50\", \"DT\", \"NDC\"]"),
      baseFolderName  = Some("ICAAP_TR_2025Q1_251016_v1"))
    Thread.sleep(1200) // let some wall-clock elapse so duration is visible
    a.succeeded()

    // tseadfwd — module-specific columns left null; ends via failure path
    val b = RunAudit.start("tseadfwd", auditConfig,
      usedConf = "localRun/tseadfwd/application.conf", userLauncher = Some("svc_batch"), motor = Some("tseadfwd"))
    b.failed(new IllegalStateException("boom"))

    println(s"\n>>> wrote runs: A=${a.runId} (climatetables), B=${b.runId} (tseadfwd)")

    val hist = RunAuditStore.readFiles(root).orderBy("module_name")
    println(s">>> run_history rows (read from ORC): ${hist.count()}")
    println(">>> schema:"); hist.printSchema()
    hist.select("run_id", "application_id", "module_name", "used_jar", "used_conf",
      "user_launcher", "creation_date", "end_date", "duration", "motor",
      "projection_dates", "scenarios", "base_folder_name").show(false)

    println(">>> ORC partition directories on disk:")
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.globStatus(new org.apache.hadoop.fs.Path(s"$root/module_name=*/run_id=*"))
      .foreach(st => println(s"   ${st.getPath}"))

    println("\n>>> external table DDL (run on the cluster to register it):")
    println(RunAuditStore.createTableDDL(table, root))

    spark.stop()
  }
}
