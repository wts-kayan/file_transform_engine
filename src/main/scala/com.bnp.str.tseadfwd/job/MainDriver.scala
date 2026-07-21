package com.bnp.str.tseadfwd.job

import com.bnp.str.tseadfwd.utility.PrimaryUtilities.getHdfsReader
import com.typesafe.config.ConfigFactory
import com.bnp.str.tseadfwd.sessionmanager.SparkSessionManager
import com.bnp.str.tseadfwd.common.PrimaryRunner
import com.bnp.str.tseadfwd.reader.PrimaryReader
import com.bnp.str.tseadfwd.utility.{PrimaryConstants, PrimaryUtilities}
import com.bnp.str.tseadfwd.writer.PrimaryWriter
import com.bnp.str.tseadfwd.audit.TseadfwdAudit
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

    // ---- run audit: record this execution in run_history (shared, module-agnostic) ----
    val audit = TseadfwdAudit.start(config, usedConf = absoluteConfigPath)(sparkSession)
    logger.info(s"Run audit started: runId=${audit.runId}")

    try {
      val df =
        new PrimaryRunner(primaryReader, outputTableName)(sparkSession, config)
          .run_tseadfwd_runner()

      logger.info(s"OUTPUT - $outputTableName (final term structure)")
      df.show(false)

      primaryWriter.write(df, outputTableName)(sparkSession, config)

      audit.succeeded()

      logger.info(s"End ${PrimaryConstants.APPLICATION_NAME} (${this.getClass.getName})")
    } catch {
      case e: Throwable =>
        audit.failed(e)
        logger.error(s"Run ${audit.runId} FAILED", e)
        throw e
    }
  }
}
