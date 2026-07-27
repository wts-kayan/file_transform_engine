package com.bnp.str.ageing.sessionmanager

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object StrSparkSessionManager {

  def fetchSparkSession(appName: String): SparkSession = {

    val isLocal = new SparkConf().getOption("spark.master").forall(_.startsWith("local"))

    val builder = SparkSession
      .builder()
      .appName(appName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")

    val tuned =
      if (isLocal) {
        val warehouse = s"${System.getProperty("user.dir")}/out/warehouse"
        builder
          .master("local[*]")
          .config("spark.driver.bindAddress", "0.0.0.0")
          .config("spark.driver.host", "127.0.0.1")
          .config("spark.broadcast.compress", "false")
          .config("spark.sql.codegen.wholeStage", "false")
          .config("spark.debug.maxToStringFields", 1000)
          .config("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:db;create=true")
          .config("spark.sql.warehouse.dir", warehouse)
          .config("hive.metastore.warehouse.dir", warehouse)
      } else {
        builder
          .config("hive.execution.engine", "spark")
          .config("spark.sql.autoBroadcastJoinThreshold", 1073741824L)
      }

    tuned
      .enableHiveSupport()
      .getOrCreate()
  }

}
