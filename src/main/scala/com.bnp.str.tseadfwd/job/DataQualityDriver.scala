package com.bnp.str.tseadfwd.job

import com.bnp.str.tseadfwd.dataquality.{DataQualityMapper, DqConfig, DqWriter}
import com.bnp.str.tseadfwd.sessionmanager.SparkSessionManager
import com.bnp.str.tseadfwd.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Standalone data-quality REPORT job.
 *
 * It re-runs the business rules against an ALREADY PRODUCED term-structure CSV and rewrites the HTML
 * report — nothing is computed, and no output row is ever changed. Use it to regenerate a report, or
 * to check a file produced elsewhere; the daily report comes out of [[MainDriver]], which runs the
 * same [[DataQualityMapper]] on the frame it is about to write.
 *
 * Single argument: the path to `application.conf`. Everything else is read from
 * `tseadfwd_app.DATA_QUALITY`:
 *   enabled    — when false the job does nothing
 *   sourcePath — CSV to inspect (default: the configured TS_EAD_FWD output file)
 *   htmlPath   — report to write (default: `DQ_<tableName>.html` next to that output)
 *
 * One caveat worth knowing: rule R01 (all terms equal to 1) can only report what is still in the
 * file. When the main run already removed those lines — the default — a standalone pass over its
 * output correctly finds nothing. The report that names them is the one MainDriver wrote.
 */
object DataQualityDriver {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val confPath = args.lift(0).getOrElse("localRun/tseadfwd/application.conf")

    implicit val spark: SparkSession = SparkSessionManager.fetchSparkSession("tseadfwd-data-quality")

    // Read the conf through Hadoop's FileSystem (like MainDriver), so the same call resolves a
    // local path, a `--files`-shipped conf (basename in the working dir), or an HDFS path.
    val config: Config = {
      val reader = PrimaryUtilities.getHdfsReader(confPath)(spark.sparkContext)
      try ConfigFactory.parseReader(reader) finally reader.close()
    }

    val dq = DqConfig.from(config)

    if (!dq.enabled) {
      logger.info("DATA_QUALITY.enabled = false -> report skipped.")
      spark.stop()
      return
    }

    logger.info(s"Data quality (standalone) reading ${dq.sourcePath}")

    val df = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .csv(dq.sourcePath)

    // `source` names the dataset, `outputFile` the exact file read — the report has to say which
    // file it judged, and `sourcePath` may well point at a vintage other than the latest output.
    val report = new DataQualityMapper(dq)
      .reportOnly(df, source = PrimaryConstants.OUTPUT_EAD_FWD, runId = "standalone",
        outputFile = dq.sourcePath)

    logger.info(report.summaryLine)
    DqWriter.writeHtml(dq.htmlPath, report)

    println(s"\n>>> Data-quality report written: ${dq.htmlPath}\n    ${report.summaryLine}")

    spark.stop()
  }
}
