package com.bnp.str.tseadfwd.reader

import com.bnp.str.tseadfwd.mapping.RaCompareView.{RaKey, Series}
import com.bnp.str.tseadfwd.utility.PrimaryConstants._
import com.bnp.str.tseadfwd.utility.PrimaryUtilities
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Loads ONE `INPUTS_RA` workbook into the flat series map the comparison core consumes.
 *
 * Sheet selection is [[RaSheetDiscovery]] — the very same two gates the production run uses: the
 * sheet NAME must match the pattern, and its CONTENT must carry the five key columns. Reusing it is
 * the point: a tab the engine skips (the divider literally named `Inputs RA ->`) must be skipped
 * here too, for the same stated reason, or the comparison and the calculation would disagree about
 * what the workbook contains.
 *
 * Values are read through [[PrimaryUtilities.readExcelSheet]], so `inferSchema = true` applies and
 * a numeric cell arrives as its stored double rather than as the text Excel happens to display —
 * the bug that silently divided every CRD by a thousand when a workbook was formatted `#,##0`.
 */
object RaCompareReader {

  private val log = LoggerFactory.getLogger(this.getClass)

  /**
   * @param series     key -> monthly values, `M1` first
   * @param monthCount how many `M<n>` columns the workbook carries
   * @param sheets     the sheets actually loaded, for the report's INFO page
   * @param skipped    what was passed over and why — a missing perimeter must be answerable
   */
  final case class LoadedRa(series: Series, monthCount: Int,
                            sheets: Seq[String], skipped: Seq[String]) {
    def summary: String =
      s"${series.size} series over $monthCount month(s) from ${sheets.mkString(", ")}" +
        (if (skipped.isEmpty) "" else s"; skipped ${skipped.mkString(", ")}")
  }

  /** Read every RA sheet of one workbook and union them into a single series map. */
  def load(path: String, cfg: RaSheetConfig)(implicit spark: SparkSession): LoadedRa = {
    val single = cfg.copy(paths = Vector(path))
    val selection = RaSheetDiscovery.discover(single, spark.sparkContext.hadoopConfiguration)
    log.info(s"$path -> ${selection.summary}")

    val skipped = selection.skipped.map(s => s"${s.sheet} (${s.reason})")
    var months = 0
    val acc = scala.collection.mutable.Map.empty[RaKey, Array[Double]]
    val loaded = Vector.newBuilder[String]

    selection.selected.foreach { s =>
      val df = PrimaryUtilities.readExcelSheet(s.path, s.sheet, label = s"RA compare '${s.sheet}'")
      val missing = RaSheetDiscovery.missingColumns(df.columns.toSeq, cfg.requireColumns)
      if (missing.nonEmpty) {
        log.warn(s"sheet '${s.sheet}' in ${s.path} skipped (missing ${missing.mkString(", ")})")
      } else {
        val monthCols = monthColumns(df.columns.toSeq)
        months = math.max(months, monthCols.length)
        val cols = Seq(COL_PERIMETER, COL_SEGMENT, COL_RATE_TYPE, COL_FWL_TYPE, COL_METRIC) ++ monthCols

        df.select(cols.head, cols.tail: _*).collect().foreach { r =>
          val key = RaKey(str(r, 0), str(r, 1), str(r, 2), str(r, 3), str(r, 4))
          if (key.perimeter.nonEmpty && key.metric.nonEmpty) {
            val values = Array.tabulate(monthCols.length)(i => num(r.get(5 + i)))
            // A duplicated key would otherwise be silently overwritten by whichever sheet was read
            // last, and the comparison would quietly describe a series nobody can point at.
            if (acc.contains(key)) log.warn(s"duplicate RA key $key in ${s.path}; keeping the first")
            else acc(key) = values
          }
        }
        loaded += s.sheet
      }
    }

    val result = LoadedRa(acc.toMap, months, loaded.result(), skipped)
    log.info(s"$path -> ${result.summary}")
    result
  }

  /** `M1 … Mn` in numeric order — a workbook is not obliged to hold them in column order. */
  private def monthColumns(columns: Seq[String]): Seq[String] =
    columns
      .filter(c => c.length > 1 && (c.charAt(0) == 'M' || c.charAt(0) == 'm') && c.drop(1).forall(_.isDigit))
      .sortBy(_.drop(1).toInt)

  private def str(r: Row, i: Int): String =
    Option(r.get(i)).map(_.toString.trim).getOrElse("")

  /**
   * Cell value as a double. `inferSchema = true` means this is normally already a number; the
   * string branch stays for a column Excel typed as text, and reuses the same locale-tolerant
   * reading the engine applies (comma decimal, grouping separators, an accounting dash for nil).
   */
  private def num(v: Any): Double = v match {
    case null => 0.0
    case d: java.lang.Double => d.doubleValue()
    case n: java.lang.Number => n.doubleValue()
    case other =>
      val t = other.toString.trim.replaceAll("\\h", "")
      if (t.isEmpty || t == "-" || t == "–" || t == "—") 0.0
      else {
        val lc = t.lastIndexOf(','); val ld = t.lastIndexOf('.')
        val norm =
          if (lc >= 0 && ld >= 0) { if (lc > ld) t.replace(".", "").replace(',', '.') else t.replace(",", "") }
          else if (lc >= 0) t.replace(',', '.')
          else t
        try norm.toDouble catch { case _: NumberFormatException => 0.0 }
      }
  }
}
