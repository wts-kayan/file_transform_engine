package com.bnp.str.addons.writer

import com.bnp.str.addons.utility.PrimaryUtilities
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class PrimaryWriter()(implicit sparkSession: SparkSession, conf: Config) {

  def write(dataframe: DataFrame,
            tableName: String)(sparkSession: SparkSession, conf: Config): Unit = {

    PrimaryUtilities.writeDataframe(dataframe, tableName)(sparkSession, conf)
  }

}
