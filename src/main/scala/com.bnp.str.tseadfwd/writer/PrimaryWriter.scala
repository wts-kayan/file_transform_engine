package com.bnp.str.tseadfwd.writer

import com.bnp.str.tseadfwd.utility.PrimaryUtilities
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
class PrimaryWriter()(implicit sparkSession: SparkSession, conf: Config) {

  private val log = LoggerFactory.getLogger(this.getClass)

  def write(
             dataframe: DataFrame,
             tableName: String
           )(sparkSession: SparkSession, conf: Config): Unit = {

    PrimaryUtilities
      .writeDataframe(dataframe, tableName)(sparkSession, conf)
  }
}