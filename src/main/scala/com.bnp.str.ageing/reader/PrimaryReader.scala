package com.bnp.str.ageing.reader

import com.bnp.str.ageing.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Reads the macroeconomic scenario workbook: exposes its sheet (scenario) names and materializes
 * one scenario sheet at a time as a DataFrame. The input path is resolved from the module config.
 */
class PrimaryReader()(implicit sparkSession: SparkSession, conf: Config) {

  private val appConf = conf.getConfig(PrimaryConstants.APP_CONF)

  val macroInputPath: String = appConf.getString(PrimaryConstants.MACRO_INPUT_PATH)

  /** The scenario sheet names present in the workbook. */
  lazy val scenarioNames: Seq[String] = PrimaryUtilities.sheetNames(macroInputPath)

  /** Read a single scenario sheet by name. */
  def read(scenario: String): DataFrame =
    PrimaryUtilities.readScenario(macroInputPath, scenario)
}
