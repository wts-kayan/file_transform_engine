package com.bnp.str.climatetables.job

import com.bnp.str.climatetables.common.PrimaryRunner
import com.bnp.str.climatetables.reader.PrimaryReader
import com.bnp.str.climatetables.sessionmanager.StrSparkSessionManager
import com.bnp.str.climatetables.utility.{FrameMeta, PrimaryConstants, PrimaryUtilities}
import com.bnp.str.climatetables.writer.PrimaryWriter
import com.bnp.str.utilities.audit.ClimatetablesAudit
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.collection.parallel.ForkJoinTaskSupport

object ClimateTablesDriver {

  private val log = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    log.info(" Start %s ".format(PrimaryConstants.APPLICATION_NAME), this.getClass.getName)

    val absoluteConfigPath = args(0)

    val sparkSession = StrSparkSessionManager.fetchSparkSession(PrimaryConstants.APPLICATION_NAME)

    val configPath = PrimaryUtilities.getHdfsReader(absoluteConfigPath)(sparkSession.sparkContext)
    val config = ConfigFactory.parseReader(configPath)

    val audit = ClimatetablesAudit.start(config, usedConf = absoluteConfigPath)(sparkSession)

    log.info(s"Run audit started: runId=${audit.runId}")

    try {
      val primaryReader = new PrimaryReader()(sparkSession, config)

      val climateProjectionDates = primaryReader.getClimateProjectionDates
      val scenarios = primaryReader.getScenarios
      val climateBaseDate = primaryReader.getClimateBaseDate
      val defaultOriginationDate = primaryReader.getDefaultOriginationDate
      val targetVariables = primaryReader.getTargetVariables
      val activateChrAtOrigination = primaryReader.getActivateChrAtOrigination
      val activateCdNivRisqChrOrigin = primaryReader.getActivateCdNivRisqChrOrigin

      // Cartesian product of dates × scenarios and process in parallel
      // Build the parallel collection, set its task support, then iterate
      val parallelismLevelPath = s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.INPUT_CONSTANTS}.${PrimaryConstants.PARALLELISM_LEVEL}"
      val parallelismLevel = if (config.hasPath(parallelismLevelPath)) config.getInt(parallelismLevelPath) else 1
      val taskSupport = new ForkJoinTaskSupport(new java.util.concurrent.ForkJoinPool(parallelismLevel))

      log.info(s"Run parallelism level = ${parallelismLevel}")

      val dateScenarioPairs = (for {
        date <- climateProjectionDates
        scenario <- scenarios
      } yield (date, scenario)).par

      dateScenarioPairs.tasksupport = taskSupport

      dateScenarioPairs.foreach { case (date, scenario) =>
        val activation = primaryReader.getStepActivation(date)
        val meta = FrameMeta(
          date = date,
          scenario = scenario,
          baseDate = climateBaseDate,
          defaultOriginationDate = defaultOriginationDate,
          targetVariables = targetVariables,
          eadFactorStep = activation(PrimaryConstants.EAD_FACTOR_STEP),
          ratingDeteriorationStep = activation(PrimaryConstants.RATING_DETERIORATION_STEP),
          pdDeteriorationStep = activation(PrimaryConstants.PD_DETERIORATION_STEP),
          originationDateStep = activation(PrimaryConstants.ORIGINATION_DATE_STEP),
          activateChrAtOrigination = activateChrAtOrigination,
          activateCdNivRisqChrOrigin = activateCdNivRisqChrOrigin
        )
        val results = new PrimaryRunner(primaryReader, meta)(sparkSession, config).climate_tables_runner()
        log.info(
          s"""    |
             | ---- Part 6 : Writing result for date=$date, scenario=$scenario ----
             |  dataTypeDf1 = ${results.dataTypeDf1}
             |  dataTypeDf2 = ${results.dataTypeDf2}""".stripMargin)

        val primaryWriter = new PrimaryWriter()(sparkSession, config)
        primaryWriter.write(results)(sparkSession, config)

        // Unpersist join fem tiers table after writer
        results.fullDf.unpersist()

      }

      audit.succeeded()
      log.info(s"End ${PrimaryConstants.APPLICATION_NAME}")
    }
    catch {
      case e: Throwable =>
        audit.failed(e)
        log.error(s"${PrimaryConstants.APPLICATION_NAME} failed: ${e.getMessage}", e)
        throw e
    }
  }
}
