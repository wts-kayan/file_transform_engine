package com.bnp.str.climatetables.sessionmanager

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object StrSparkSessionManager {

  private val log = LoggerFactory.getLogger(this.getClass)

  def fetchSparkSession(appName: String): SparkSession = {
    val isLocalhost = new SparkConf().getOption("spark.master").forall(_.startsWith("local"))
    log.info(s"isLocal $isLocalhost")

    if (!isLocalhost) {
      SparkSession
        .builder()
        .config("hive.execution.engine", "spark")
        .config("spark.sql.autoBroadcastJoinThreshold", 1073741824)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.codegen.maxFields", 900)
        .config("spark.sql.shuffle.partitions", "1200")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .appName(appName)
        .enableHiveSupport()
        .getOrCreate()

    }
    else {
      val WAREHOUSE_PATH = "/out/warehouse"
      SparkSession
        .builder()
        .config("spark.broadcast.compress", "false")
        .config("spark.master", "local")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.codegen.wholeStage", "false")
        .config("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:db;create=true")
        .config("spark.sql.warehouse.dir", System.getProperty("user.dir") + WAREHOUSE_PATH)
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.debug.maxToStringFields", 1000)
        .appName(appName)
        .enableHiveSupport()
        .master("local[*]")
        .getOrCreate()

    }

  }

}
