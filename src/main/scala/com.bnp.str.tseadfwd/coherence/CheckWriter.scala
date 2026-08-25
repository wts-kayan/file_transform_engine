package com.bnp.str.tseadfwd.coherence

import com.bnp.str.tseadfwd.utility.PrimaryUtilities
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/** Persists the coherence-check report. */
object CheckWriter {

  private val log = LoggerFactory.getLogger(this.getClass)

  /**
   * Write the HTML report to `path`, which goes through Hadoop's FileSystem — so the same call
   * takes a local path (`localRun/...`) or an HDFS one (`hdfs:///user/...`) with no change.
   */
  def writeHtml(path: String, report: CheckReport)(implicit spark: SparkSession): Unit = {
    PrimaryUtilities.writeStringToHdfs(path, CheckHtmlView.render(report))(spark.sparkContext)
    log.info(s"Coherence-check report written -> $path")
  }
}
