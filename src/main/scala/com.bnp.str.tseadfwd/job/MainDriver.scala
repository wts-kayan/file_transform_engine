package com.bnp.str.tseadfwd.job

import com.bnp.str.tseadfwd.utility.PrimaryUtilities.getHdfsReader
import com.typesafe.config.ConfigFactory
import com.bnp.str.tseadfwd.sessionmanager.SparkSessionManager
import com.bnp.str.tseadfwd.common.PrimaryRunner
import com.bnp.str.tseadfwd.reader.PrimaryReader
import com.bnp.str.tseadfwd.utility.{PrimaryConstants, PrimaryUtilities}
import com.bnp.str.tseadfwd.writer.PrimaryWriter
import com.bnp.str.tseadfwd.audit.TseadfwdAudit
import com.bnp.str.tseadfwd.consistency.{CheckConfig, CheckWriter, ConsistencyCheckMapper}
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

      // ---- business consistency check ----
      // The mapper now emits EVERY computed term, so the rules see the complete curve. The rules
      // themselves only ever REPORT (ConsistencyCheckMapper never writes); the removal below is the main
      // job's, applied once, to the frame it is about to write. An HTML report naming what was taken
      // out is written alongside the output.
      val checks = CheckConfig.from(config)
      val toWrite =
        if (!checks.enabled) {
          logger.info("CONSISTENCY_CHECK.enabled = false -> rules not evaluated, every computed row written")
          df
        } else {
          val outcome =
            new ConsistencyCheckMapper(checks)(sparkSession)
              .apply(df, source = outputTableName, runId = audit.runId,
                outputFile = checks.outputFile) // the file this very frame is about to be written to
          logger.info(outcome.report.summaryLine)
          CheckWriter.writeHtml(checks.htmlPath, outcome.report)(sparkSession)
          outcome.cleaned
        }

      logger.info(s"OUTPUT - $outputTableName (final term structure)")
      toWrite.show(false)

      primaryWriter.write(toWrite, outputTableName)(sparkSession, config)

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
