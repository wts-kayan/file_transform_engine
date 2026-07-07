package com.bnp.str.tseadfwd.job

import com.bnp.str.tseadfwd.utility.PrimaryUtilities.getHdfsReader
import com.typesafe.config.ConfigFactory
import com.bnp.str.tseadfwd.sessionmanager.SparkSessionManager
import com.bnp.str.tseadfwd.common.PrimaryRunner
import com.bnp.str.tseadfwd.reader.PrimaryReader
import com.bnp.str.tseadfwd.utility.{PrimaryConstants, PrimaryUtilities}
import com.bnp.str.tseadfwd.writer.PrimaryWriter
import com.bnp.str.utilities.audit.{AuditJson, RunAudit}
import org.slf4j.LoggerFactory

object MainDriver {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    *
    * @param args jar parameters
    */
  def main(args: Array[String]): Unit = {

    logger.info(s"Start ${PrimaryConstants.APPLICATION_NAME} (${this.getClass.getName})")

    val absoluteConfigPath = args(0)

    val sparkSession =
      SparkSessionManager.fetchSparkSession(
        PrimaryConstants.APPLICATION_NAME
      )

    val configPath =
      PrimaryUtilities.getHdfsReader(absoluteConfigPath)(sparkSession.sparkContext)

    val config =
      ConfigFactory.parseReader(configPath)

    val primaryReader =
      new PrimaryReader()(sparkSession, config)

    val primaryWriter =
      new PrimaryWriter()(sparkSession, config)

    val outputTableName =
      PrimaryConstants.OUTPUT_EAD_FWD

    // ---- run audit: record this execution in run_history (shared, job-agnostic) ----
    val auditConfig =
      config.getConfig(s"${PrimaryConstants.APP_CONF}.audit")
    val runParams = config.getConfig(s"${PrimaryConstants.APP_CONF}.parameters")
    val paramsJson = AuditJson.obj(
      "as_of_date_quarter"        -> runParams.getString("as_of_date_quarter"),
      "projection_horizon"        -> runParams.getString("last_quarter_projection_horizon"),
      "apply_rate_to_shock"       -> runParams.getBoolean("apply_rate_to_shock"),
      "macro_delta_scale"         -> runParams.getDouble("macro_delta_scale"),
      "exclude_ead_ra_rate_ge_1"  -> runParams.getBoolean("exclude_ead_ra_rate_ge_1")
    )
    val audit = RunAudit.start(
      module       = "tseadfwd",
      jobName      = outputTableName,
      jobClass     = this.getClass.getName,
      auditConfig  = auditConfig,
      configPath   = absoluteConfigPath,
      paramsJson   = paramsJson
    )(sparkSession)
    logger.info(s"Run audit started: runId=${audit.runId}")

    try {
      val df =
        new PrimaryRunner(primaryReader, outputTableName)(sparkSession, config)
          .run_tseadfwd_runner()

      logger.info(s"OUTPUT - $outputTableName (final term structure)")
      df.show(false)

      val outputCount = df.count()
      primaryWriter.write(df, outputTableName)(sparkSession, config)

      val outConf = config.getConfig(s"${PrimaryConstants.APP_CONF}.$outputTableName")
      val outPath = s"${outConf.getString("tmpPath")}/${outConf.getString("tableName")}"
      audit.succeeded(outputCount = Some(outputCount), outputPath = outPath)

      logger.info(s"End ${PrimaryConstants.APPLICATION_NAME} (${this.getClass.getName})")
    } catch {
      case e: Throwable =>
        audit.failed(e)
        logger.error(s"Run ${audit.runId} FAILED", e)
        throw e
    }
  }
}
