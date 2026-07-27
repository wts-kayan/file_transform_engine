package com.bnp.str.ageing.job

import com.bnp.str.ageing.common.PrimaryRunner
import com.bnp.str.ageing.reader.PrimaryReader
import com.bnp.str.ageing.sessionmanager.StrSparkSessionManager
import com.bnp.str.ageing.utility.{PrimaryConstants, PrimaryUtilities}
import com.bnp.str.ageing.writer.PrimaryWriter
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Ageing entry point: read the macroeconomic scenario workbook, age every non-central scenario by
 * applying the central-derived, date-shifted shock, then append each scenario sheet to the output
 * workbook.
 *
 * Usage: `spark-submit --class com.bnp.str.ageing.job.MainDriver <application.conf>`
 */
object MainDriver {

  private val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    require(args.nonEmpty, "Missing argument: path to the application.conf")
    val absoluteConfigPath = args(0)

    log.info(s"Start ${PrimaryConstants.APPLICATION_NAME} (${this.getClass.getName})")

    implicit val sparkSession: SparkSession =
      StrSparkSessionManager.fetchSparkSession(PrimaryConstants.APPLICATION_NAME)

    try {
      val configReader = PrimaryUtilities.getHdfsReader(absoluteConfigPath)(sparkSession.sparkContext)
      implicit val config: Config = ConfigFactory.parseReader(configReader)

      val primaryReader = new PrimaryReader()
      val primaryWriter = new PrimaryWriter()

      // fail fast if the output workbook already exists (writes append)
      val outputPath = config.getConfig(PrimaryConstants.APP_CONF).getString(PrimaryConstants.OUTPUT_PATH)
      val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
      if (fs.exists(new Path(outputPath)))
        throw new IllegalStateException(s"the file $outputPath already exists")

      val aged = new PrimaryRunner(primaryReader).run_ageing_runner()

      log.info("Writing aged scenarios")
      primaryReader.scenarioNames.foreach(scenario => primaryWriter.write(aged(scenario), scenario))

      log.info(s"End ${PrimaryConstants.APPLICATION_NAME} (${this.getClass.getName})")
    } catch {
      case e: Throwable =>
        log.error(s"${PrimaryConstants.APPLICATION_NAME} failed: ${e.getMessage}", e)
        throw e
    }
  }
}
