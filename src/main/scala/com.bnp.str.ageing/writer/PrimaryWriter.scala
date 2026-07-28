package com.bnp.str.ageing.writer

import com.bnp.str.ageing.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

class PrimaryWriter()(implicit sparkSession: SparkSession, conf: Config) {

  private val log = LoggerFactory.getLogger(this.getClass)

  private val appConf = conf.getConfig(PrimaryConstants.APP_CONF)

  private val outputPath: String = appConf.getString(PrimaryConstants.OUTPUT_PATH)

  /**
   * Where the single all-scenarios CSV goes. Configured via `output.macroeconomic.csv_path`;
   * when that key is absent it is derived from the workbook path, so configs written before the
   * key existed keep working.
   */
  private val csvPath: String =
    if (appConf.hasPath(PrimaryConstants.OUTPUT_CSV_PATH)) {
      appConf.getString(PrimaryConstants.OUTPUT_CSV_PATH)
    } else {
      val derived = s"${outputPath.replaceAll(PrimaryConstants.XLSX_EXTENSION_REGEX, "")}.${PrimaryConstants.CSV_EXTENSION}"
      log.info(s"No '${PrimaryConstants.OUTPUT_CSV_PATH}' configured; writing the CSV next to the workbook, to $derived")
      derived
    }

  def write(dataframe: DataFrame, scenario: String): Unit =
    PrimaryUtilities.writeScenario(dataframe, outputPath, scenario)

  /** Writes all the scenarios into a single CSV, each row tagged with its scenario. */
  def writeCsv(scenarios: Seq[(String, DataFrame)]): Unit =
    PrimaryUtilities.writeScenariosCsv(scenarios, csvPath)

}
