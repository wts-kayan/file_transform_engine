package com.bnp.str.ageing.writer

import com.bnp.str.ageing.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Appends aged scenario DataFrames, one sheet at a time, to the output macroeconomic workbook whose
 * path is resolved from the module config.
 */
class PrimaryWriter()(implicit sparkSession: SparkSession, conf: Config) {

  private val outputPath: String =
    conf.getConfig(PrimaryConstants.APP_CONF).getString(PrimaryConstants.OUTPUT_PATH)

  def write(dataframe: DataFrame, scenario: String): Unit =
    PrimaryUtilities.writeScenario(dataframe, outputPath, scenario)

}
