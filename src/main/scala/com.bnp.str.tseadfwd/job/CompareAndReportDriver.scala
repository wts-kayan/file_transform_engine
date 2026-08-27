package com.bnp.str.tseadfwd.job

import com.bnp.str.tseadfwd.mapping.RaCompareView
import com.bnp.str.tseadfwd.mapping.RaCompareView.{CellStatus, RaCompareResult, Series}
import com.bnp.str.tseadfwd.reader.{RaCompareReader, RaSheetConfig}
import com.bnp.str.tseadfwd.sessionmanager.SparkSessionManager
import com.bnp.str.tseadfwd.utility.{PrimaryConstants, PrimaryUtilities}
import com.bnp.str.tseadfwd.writer.RaCompareExcelWriter
import com.bnp.str.tseadfwd.writer.RaCompareExcelWriter.WriteOptions
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * RA input comparison report — ticket 977.
 *
 * Compares TWO `INPUTS_RA` workbooks (the one the Risk team is about to use against the one used at
 * the previous reference) and writes the `%change` of the four metrics, for every perimeter and
 * segment the two files share, over the three stress legs and every month, with a chart per
 * (metric x segment) superimposing the two curves. The Risk team produces this by hand today.
 *
 * It touches nothing in the production pipeline: [[MainDriver]] is unchanged, and this job only
 * READS the input workbooks. Single argument, as every driver here: the path to `application.conf`.
 * Everything else comes out of `tseadfwd_app.COMPARE_AND_REPORT` — see [[CompareAndReportConfig]].
 *
 * Design and the answers behind each rule: `docs/tseadfwd/977/RA_COMPARISON_REPORT_DESIGN.md`.
 */
object CompareAndReportDriver {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val confPath = args.lift(0).getOrElse("localRun/tseadfwd/application.conf")

    implicit val spark: SparkSession =
      SparkSessionManager.fetchSparkSession("tseadfwd-compare-and-report")

    try {
      val config: Config = {
        val reader = PrimaryUtilities.getHdfsReader(confPath)(spark.sparkContext)
        try ConfigFactory.parseReader(reader) finally reader.close()
      }

      val cfg = CompareAndReportConfig.from(config)
      if (!cfg.enabled) {
        logger.info(s"${CompareAndReportConfig.BLOCK}.enabled = false -> report skipped.")
        return
      }

      run(cfg, config)
    } finally spark.stop()
  }

  /** Read both workbooks, compare, write. Split out of `main` so a test can drive it. */
  def run(cfg: CompareAndReportConfig, config: Config)(implicit spark: SparkSession): RaCompareResult = {
    // The SAME discovery rules the production run uses, so a tab the engine skips is skipped here
    // too. The RA block is optional in the conf; fall back to the discovery defaults if absent.
    val sheetCfg = RaSheetConfig.from(config).getOrElse(
      RaSheetConfig(paths = Vector.empty, sheetPattern = RaSheetConfig.DEFAULT_PATTERN,
        requireColumns = RaSheetConfig.DEFAULT_REQUIRED, include = Vector.empty, exclude = Vector.empty))

    logger.info(s"RA comparison: new=${cfg.newPath} old=${cfg.oldPath}")
    val loadedNew = RaCompareReader.load(cfg.newPath, sheetCfg)
    val loadedOld = RaCompareReader.load(cfg.oldPath, sheetCfg)

    val full = RaCompareView.compare(loadedNew.series, loadedOld.series,
      loadedNew.monthCount, loadedOld.monthCount)

    // A configured perimeter list narrows the report; empty means every common perimeter (Q15).
    val result =
      if (cfg.perimeters.isEmpty) full
      else {
        val wanted = cfg.perimeters.map(_.trim.toUpperCase).toSet
        full.copy(compared = full.compared.filter(k => wanted.contains(k.perimeter.toUpperCase)))
      }

    if (result.compared.isEmpty)
      throw new IllegalStateException(
        s"the two workbooks share no (perimeter, segment, rate type, FWL type, metric) key" +
          (if (cfg.perimeters.nonEmpty) s" among the configured perimeters ${cfg.perimeters.mkString(", ")}" else "") +
          s". New: ${loadedNew.summary}. Previous: ${loadedOld.summary}.")

    logger.info(result.summary)
    if (result.onlyNew.nonEmpty || result.onlyOld.nonEmpty)
      logger.warn(s"excluded from the tables and listed on COMPARE INFO: " +
        s"${result.onlyNew.size} key(s) only in the new file, ${result.onlyOld.size} only in the previous one")

    val opts = WriteOptions(
      newLabel = cfg.newLabel, oldLabel = cfg.oldLabel,
      segmentOrder = cfg.segmentOrder, safeDiv = cfg.safeDiv,
      sourceNew = cfg.newPath, sourceOld = cfg.oldPath)

    val sheets = RaCompareExcelWriter.write(cfg.xlsxPath, result, loadedNew.series, loadedOld.series, opts)

    cfg.csvPath.foreach(p => writeFlatCsv(p, result, loadedNew.series, loadedOld.series))

    println(s"\n>>> RA comparison report written: ${cfg.xlsxPath}" +
      s"\n    ${result.summary}" +
      s"\n    sheets: ${sheets.mkString(", ")}" +
      cfg.csvPath.map(p => s"\n    reconciliation csv: $p").getOrElse(""))

    result
  }

  /**
   * The flat per-cell CSV of design §6.3 — one row per compared cell.
   *
   * Not asked for by the US, but it is what turns "test the generated reports against the manual
   * calculations" into a diff rather than a reading exercise, and it matches what `EadFwdCompare`
   * already writes for the output side.
   */
  private def writeFlatCsv(path: String, result: RaCompareResult, newSeries: Series,
                           oldSeries: Series)(implicit spark: SparkSession): Unit = {
    val sb = new StringBuilder
    sb.append("PERIMETER;SEGMENT;RATE_TYPE;FWL_TYPE;METRIC;MONTH;NEW;OLD;PCT_CHANGE;STATUS\n")

    // Decimal comma, like every other CSV this module writes.
    def num(v: Option[Double]): String = v.map(d => f"$d%.9f".replace('.', ',')).getOrElse("")

    RaCompareView.cells(result, newSeries, oldSeries).foreach { c =>
      sb.append(c.key.perimeter).append(';').append(c.key.segment).append(';')
        .append(c.key.rateType).append(';').append(c.key.fwlType).append(';')
        .append(c.key.metric).append(';').append("M").append(c.month).append(';')
        .append(num(c.newValue)).append(';').append(num(c.oldValue)).append(';')
        .append(num(c.pctChange)).append(';').append(c.status.code).append('\n')
    }

    val p = new Path(path)
    val fs = p.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val out = fs.create(p, true)
    try out.write(sb.toString.getBytes("UTF-8")) finally out.close()
    logger.info(s"reconciliation CSV written to $path")
  }
}

/**
 * Settings of `tseadfwd_app.COMPARE_AND_REPORT`.
 *
 * The block name is the business's (977 Q1), spelled `UPPER_SNAKE` like every other block in
 * `application.conf` — HOCON is case-sensitive, and a lone lowercase block would be the odd one out.
 *
 * There is deliberately NO `newStart` / `oldStart`: the report carries no calendar dates, months are
 * `M1..Mn` throughout, and the two files are aligned by month index (Q3/Q4).
 */
final case class CompareAndReportConfig(enabled: Boolean,
                                        newPath: String,
                                        oldPath: String,
                                        newLabel: String,
                                        oldLabel: String,
                                        perimeters: Seq[String],
                                        segmentOrder: Seq[String],
                                        xlsxPath: String,
                                        csvPath: Option[String],
                                        safeDiv: Boolean)

object CompareAndReportConfig {

  final val BLOCK = "COMPARE_AND_REPORT"

  /** Default table order; a segment it does not name still appears, after it, alphabetically (Q14). */
  final val DEFAULT_SEGMENT_ORDER: Seq[String] =
    Seq("MORTGAGE", "INVEST_PRO", "INVEST_CORP", "CONSO")

  def from(config: Config): CompareAndReportConfig = {
    import scala.collection.JavaConverters._

    val appConf = config.getConfig(PrimaryConstants.APP_CONF)
    if (!appConf.hasPath(BLOCK))
      throw new IllegalArgumentException(
        s"${PrimaryConstants.APP_CONF}.$BLOCK is absent: the comparison job needs the two workbooks " +
          s"to compare (newPath / oldPath). See docs/tseadfwd/977/RA_COMPARISON_REPORT_DESIGN.md.")
    val c = appConf.getConfig(BLOCK)

    def str(key: String, default: String) = if (c.hasPath(key)) c.getString(key) else default
    def bool(key: String, default: Boolean) = if (c.hasPath(key)) c.getBoolean(key) else default
    def list(key: String, default: Seq[String]) =
      if (c.hasPath(key)) c.getStringList(key).asScala.toVector else default

    def required(key: String): String =
      if (c.hasPath(key)) c.getString(key)
      else throw new IllegalArgumentException(
        s"${PrimaryConstants.APP_CONF}.$BLOCK.$key is required (the workbook to compare).")

    val out = str("xlsxPath", "Compare_RA.xlsx")

    CompareAndReportConfig(
      enabled = bool("enabled", default = true),
      newPath = required("newPath"),
      oldPath = required("oldPath"),
      newLabel = str("newLabel", "Updated"),
      oldLabel = str("oldLabel", "Previous"),
      perimeters = list("perimeters", Vector.empty),
      segmentOrder = list("segmentOrder", DEFAULT_SEGMENT_ORDER),
      xlsxPath = out,
      // Optional: the report is the deliverable, the flat CSV is for reconciliation (design 6.3).
      csvPath = if (c.hasPath("csvPath")) Some(c.getString("csvPath")) else None,
      safeDiv = bool("safeDiv", default = true))
  }
}
