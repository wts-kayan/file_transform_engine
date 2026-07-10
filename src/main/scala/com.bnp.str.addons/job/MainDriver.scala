package com.bnp.str.addons.job

import com.bnp.str.addons.common.PrimaryRunner
import com.bnp.str.addons.reader.PrimaryReader
import com.bnp.str.addons.sessionmanager.StrSparkSessionManager
import com.bnp.str.addons.utility.PrimaryUtilities.harmonizeFilesInDirectory
import com.bnp.str.addons.utility.{PrimaryConstants, PrimaryUtilities}
import com.bnp.str.addons.writer.PrimaryWriter
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

/**
 * Addons (IRBA) entry point: read the three add-on Excel sheets, join them into
 * `crr_param_add_on_ste`, then — when the output is enabled — write the CSV and harmonize the
 * Spark part-file into the final `<tableName>_<quarter>_<ddMMyyyy>.<format>` name.
 *
 * Usage: `spark-submit --class com.bnp.str.addons.job.MainDriver <application.conf>`
 */
object MainDriver {

  private val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    require(args.nonEmpty, "Missing argument: path to the application.conf")
    val absoluteConfigPath = args(0)

    log.info(s"Start ${PrimaryConstants.APPLICATION_NAME} (${this.getClass.getName})")

    val sparkSession = StrSparkSessionManager.fetchSparkSession(PrimaryConstants.APPLICATION_NAME)

    try {
      val configReader = PrimaryUtilities.getHdfsReader(absoluteConfigPath)(sparkSession.sparkContext)
      val config = ConfigFactory.parseReader(configReader)

      val primaryReader = new PrimaryReader()(sparkSession, config)
      val primaryWriter = new PrimaryWriter()(sparkSession, config)

      val outputTableName = PrimaryConstants.CRR_PARAM_ADD_ON_STE
      val outConfig = config.getConfig(s"${PrimaryConstants.APP_CONF}.$outputTableName")

      val df =
        new PrimaryRunner(primaryReader, outputTableName)(sparkSession, config).run_addons_runner()

      if (outConfig.getBoolean("enable")) {
        primaryWriter.write(df, outputTableName)(sparkSession, config)
        // Harmonize the just-written part-file into <tableName>_<quarter>_<ddMMyyyy>.<format>.
        // Guarded by `enable`: harmonization renames the write output, so it must not run when
        // nothing was written (otherwise the part-file lookup would fail).
        harmonizeFilesInDirectory(
          outputTableName,
          outConfig.getString("tmpPath"),
          outConfig.getString("path"),
          deleteTmpPath = outConfig.getBoolean("deleteTmpPath"),
          overwrite = true)(sparkSession, config)
      } else {
        log.info(s"Output '$outputTableName' disabled (enable=false); skipping write and harmonization")
      }

      log.info(s"End ${PrimaryConstants.APPLICATION_NAME} (${this.getClass.getName})")
    } catch {
      case e: Throwable =>
        log.error(s"${PrimaryConstants.APPLICATION_NAME} failed: ${e.getMessage}", e)
        throw e
    }
  }
}
