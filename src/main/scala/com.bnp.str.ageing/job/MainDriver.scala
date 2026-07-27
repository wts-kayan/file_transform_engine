package com.bnp.str.ageing.job

import com.bnp.str.ageing.audit.AgeingAudit
import com.bnp.str.ageing.common.PrimaryRunner
import com.bnp.str.ageing.reader.PrimaryReader
import com.bnp.str.ageing.sessionmanager.StrSparkSessionManager
import com.bnp.str.ageing.utility.{PrimaryConstants, PrimaryUtilities}
import com.bnp.str.ageing.writer.PrimaryWriter
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object MainDriver {

  private val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    require(args.nonEmpty, "Missing argument: path to the application.conf")
    val absoluteConfigPath = args(0)

    log.info(s"Start ${PrimaryConstants.APPLICATION_NAME} (${this.getClass.getName})")

    implicit val sparkSession: SparkSession =
      StrSparkSessionManager.fetchSparkSession(PrimaryConstants.APPLICATION_NAME)

    val configReader = PrimaryUtilities.getHdfsReader(absoluteConfigPath)(sparkSession.sparkContext)
    implicit val config: Config = ConfigFactory.parseReader(configReader)

    val primaryReader = new PrimaryReader()
    val primaryWriter = new PrimaryWriter()

    val outputPath = config.getConfig(PrimaryConstants.APP_CONF).getString(PrimaryConstants.OUTPUT_PATH)
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val outPath = new Path(outputPath)
    if (fs.exists(outPath)) {
      log.info(s"Output $outputPath already exists; deleting it before the run")
      fs.delete(outPath, true)
    }

    val audit = AgeingAudit.start(config, usedConf = absoluteConfigPath, primaryReader.scenarioNames)(sparkSession)
    log.info(s"Run audit started: runId=${audit.runId}")

    try {
      val aged = new PrimaryRunner(primaryReader).run_ageing_runner()

      log.info("Writing aged scenarios (Excel workbook + one CSV per scenario)")
      primaryReader.scenarioNames.foreach { scenario =>
        primaryWriter.write(aged(scenario), scenario)
        primaryWriter.writeCsv(aged(scenario), scenario)
      }

      audit.succeeded()

      log.info(s"End ${PrimaryConstants.APPLICATION_NAME} (${this.getClass.getName})")
    } catch {
      case e: Throwable =>
        audit.failed(e)
        log.error(s"Run ${audit.runId} FAILED", e)
        throw e
    }
  }
}
