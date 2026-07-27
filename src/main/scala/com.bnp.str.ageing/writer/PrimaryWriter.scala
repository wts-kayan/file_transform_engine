package com.bnp.str.ageing.writer

import com.bnp.str.ageing.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class PrimaryWriter()(implicit sparkSession: SparkSession, conf: Config) {

  private val outputPath: String =
    conf.getConfig(PrimaryConstants.APP_CONF).getString(PrimaryConstants.OUTPUT_PATH)

  def write(dataframe: DataFrame, scenario: String): Unit =
    PrimaryUtilities.writeScenario(dataframe, outputPath, scenario)

  def writeCsv(dataframe: DataFrame, scenario: String): Unit =
    PrimaryUtilities.writeScenarioCsv(dataframe, csvPathFor(scenario))

  private def csvPathFor(scenario: String): String = {
    val base = outputPath.replaceAll(PrimaryConstants.XLSX_EXTENSION_REGEX, "")
    s"${base}_$scenario.${PrimaryConstants.CSV_EXTENSION}"
  }

}
