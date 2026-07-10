package com.bnp.str.addons.writer

import com.bnp.str.addons.utility.PrimaryUtilities
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

class PrimaryWriter()(implicit sparkSession: SparkSession, conf: Config) {

  private val log = LoggerFactory.getLogger(this.getClass)

  def write(dataframe: DataFrame,
            partitionColumns: String,
            tableName: String,
            mode: String,
            format: String,
            numPartition:Int,
            path: String): Unit = {

    PrimaryUtilities.writeDataframe(dataframe,
      partitionColumns,
      tableName,
      mode,
      format,
      numPartition,
      path)
  }

  def write(dataframe: DataFrame,
            tableName: String)(sparkSession: SparkSession, conf: Config): Unit = {

    PrimaryUtilities.writeDataframe(dataframe, tableName)(sparkSession, conf)
  }

}
