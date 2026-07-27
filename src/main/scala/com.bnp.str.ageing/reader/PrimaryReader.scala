package com.bnp.str.ageing.reader

import com.bnp.str.ageing.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class PrimaryReader()(implicit sparkSession: SparkSession, conf: Config) {

  private val appConf = conf.getConfig(PrimaryConstants.APP_CONF)

  val macroInputPath: String = appConf.getString(PrimaryConstants.MACRO_INPUT_PATH)

  lazy val scenarioNames: Seq[String] = PrimaryUtilities.sheetNames(macroInputPath)

  def read(scenario: String): DataFrame =
    PrimaryUtilities.readScenario(macroInputPath, scenario)
}
