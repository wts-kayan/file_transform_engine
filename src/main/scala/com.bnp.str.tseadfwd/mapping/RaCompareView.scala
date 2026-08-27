package com.bnp.str.tseadfwd.mapping

import com.bnp.str.tseadfwd.utility.PrimaryConstants._

/**
 * Pure computation core of the RA input comparison report (ticket 977).
 *
 * No Spark, no IO — the same shape as [[PrimaryView]], and for the same reason: the arithmetic and
 * the scope rules are the part the business signs off, so they have to be unit-testable without a
 * session or a workbook. Reading the two files is [[com.bnp.str.tseadfwd.reader.RaCompareReader]];
 * writing the report is [[com.bnp.str.tseadfwd.writer.RaCompareExcelWriter]].
 *
 * What it computes, per compared key and month (design §5):
 * {{{
 *   pctChange(metric, i) = (new(metric, i) - old(metric, i)) / old(metric, i)
 * }}}
 *
 * and nothing else. The manual Risk workbook also carries two DERIVED rows — `RA = RA STAT + RA FI
 * + RE` and `%RA = RA x 12 / -CRD` — which the business dropped on 2026-08-27 (OPEN_QUESTIONS_977
 * Q6/Q11): the report works only from the four metrics present in the input. That is why nothing
 * here sums or annualises.
 *
 * Volume is small — two files x 6 perimeters x 48 series x 361 months is ~200k cells — so this runs
 * driver-side on collected rows, as `EadFwdCompare` does. Spark reads the sheets; it does not
 * shuffle them.
 */
object RaCompareView {

  /** The full RA key. Everything is compared on all five parts; nothing is matched on a subset. */
  final case class RaKey(perimeter: String, segment: String, rateType: String,
                         fwlType: String, metric: String) {
    /** `PERIMETER/SEGMENT/RATE_TYPE` — the table a cell belongs to, used when naming dropped keys. */
    def tableId: String = s"$perimeter/$segment/$rateType"
    override def toString: String = s"$perimeter|$segment|$rateType|$fwlType|$metric"
  }

  object RaKey {
    implicit val ordering: Ordering[RaKey] =
      Ordering.by(k => (k.perimeter, k.segment, k.rateType, k.fwlType, k.metric))
  }

  /** One RA series: a key and its monthly values, `M1` first. */
  type Series = Map[RaKey, Array[Double]]

  /**
   * What happened to one compared cell. These are the `STATUS` values of the flat CSV (design §6.3).
   *
   * `Ok` covers the both-zero case as well, with no percentage: zero against zero is not a failure
   * to compute, it is genuinely no change, so it is blanked but never listed as a problem. Only
   * `NotComputable` — a zero previous value against a non-zero new one — is a real division that
   * could not be done.
   */
  sealed abstract class CellStatus(val code: String)
  object CellStatus {
    case object Ok extends CellStatus("OK")
    case object NotComputable extends CellStatus("NOT_COMPUTABLE")
    case object OnlyNew extends CellStatus("ONLY_NEW")
    case object OnlyOld extends CellStatus("ONLY_OLD")
  }

  /**
   * One compared cell.
   *
   * @param month    1-based month index — `1` is the `M1` column of both files. The two sides are
   *                 aligned by INDEX, never by date (Q3/Q4): `M1`-of-new is compared with
   *                 `M1`-of-old whatever calendar month each represents.
   * @param pctChange `None` whenever the cell is blank in the report, whatever the reason; read
   *                  `status` to tell "no change" from "could not be computed".
   */
  final case class ComparedCell(key: RaKey, month: Int,
                                newValue: Option[Double], oldValue: Option[Double],
                                pctChange: Option[Double], status: CellStatus)

  /**
   * Outcome of a comparison.
   *
   * @param compared keys present in BOTH files, sorted
   * @param onlyNew  keys the new file has and the old one does not — excluded from the tables and
   * @param onlyOld  named in the report, never silently dropped (design §4)
   * @param months   the common horizon actually compared, `min` over both files
   */
  final case class RaCompareResult(compared: Seq[RaKey], onlyNew: Seq[RaKey], onlyOld: Seq[RaKey],
                                   months: Int, newTruncated: Boolean, oldTruncated: Boolean) {

    def comparedCount: Int = compared.size
    def droppedCount: Int = onlyNew.size + onlyOld.size

    /** Perimeters that actually produce a sheet: those with at least one compared key. */
    def perimeters: Seq[String] = compared.map(_.perimeter).distinct.sorted

    /** One log line, every key accounted for. */
    def summary: String =
      s"RA comparison: ${compared.size} key(s) compared over $months month(s); " +
        s"${onlyNew.size} only in the new file, ${onlyOld.size} only in the previous one" +
        (if (newTruncated || oldTruncated) s" (horizon truncated to $months)" else "")
  }

  // ---- computation ----------------------------------------------------------

  /**
   * `(new - old) / old`, or `None` when the report must leave the cell blank.
   *
   * Deliberately NOT guarded with `abs()`: the ratio of two same-signed values is the correct
   * relative change whatever that sign is, and taking absolute values would silently flip the sign
   * of every move on a negative metric. The `CRD` reaching this function is already positive — the
   * report negates it for display ([[displayValueAt]]) — but the guarantee has to hold on the raw
   * value too, since the flip is presentation and this is the arithmetic.
   */
  def pctChange(newValue: Option[Double], oldValue: Option[Double]): (Option[Double], CellStatus) =
    (newValue, oldValue) match {
      case (None, None)       => (None, CellStatus.Ok)          // nothing on either side
      case (Some(_), None)    => (None, CellStatus.OnlyNew)
      case (None, Some(_))    => (None, CellStatus.OnlyOld)
      case (Some(n), Some(o)) =>
        if (o == 0.0 && n == 0.0) (None, CellStatus.Ok)         // no change to report
        else if (o == 0.0) (None, CellStatus.NotComputable)     // division by zero, and it matters
        else {
          val p = (n - o) / o
          // A non-finite result can only come from an overflow; treat it as not computable rather
          // than writing Infinity into a business report.
          if (p.isNaN || p.isInfinite) (None, CellStatus.NotComputable) else (Some(p), CellStatus.Ok)
        }
    }

  /**
   * Compare two loaded files.
   *
   * The horizon is the shortest thing available anywhere: the month columns of either file, and the
   * shortest series in either. A file with fewer months is not an error — it truncates the
   * comparison, and [[RaCompareResult.newTruncated]] says so, so the report can state it.
   */
  def compare(newSeries: Series, oldSeries: Series,
              newMonthCount: Int, oldMonthCount: Int): RaCompareResult = {
    val newKeys = newSeries.keySet
    val oldKeys = oldSeries.keySet

    val compared = (newKeys intersect oldKeys).toSeq.sorted
    val onlyNew = (newKeys diff oldKeys).toSeq.sorted
    val onlyOld = (oldKeys diff newKeys).toSeq.sorted

    val shortestNew = if (newSeries.isEmpty) 0 else newSeries.values.map(_.length).min
    val shortestOld = if (oldSeries.isEmpty) 0 else oldSeries.values.map(_.length).min
    val months = Seq(newMonthCount, oldMonthCount, shortestNew, shortestOld).min max 0

    RaCompareResult(compared, onlyNew, onlyOld, months,
      newTruncated = months < newMonthCount, oldTruncated = months < oldMonthCount)
  }

  /**
   * Every compared cell, in report order — the flat CSV of design §6.3, and what a numeric
   * reconciliation diffs. Keys only in one file are NOT included: they have no counterpart to
   * compare against and are named separately in [[RaCompareResult]].
   */
  def cells(result: RaCompareResult, newSeries: Series, oldSeries: Series): Seq[ComparedCell] =
    for {
      key <- result.compared
      i <- 0 until result.months
    } yield {
      // The displayed values, so a CSV row reconciles against the cell it describes.
      val n = displayValueAt(newSeries, key, i)
      val o = displayValueAt(oldSeries, key, i)
      val (pct, status) = pctChange(n, o)
      ComparedCell(key, i + 1, n, o, pct, status)
    }

  /** Value of one series at a 0-based month, or `None` when the series is short or absent. */
  def valueAt(series: Series, key: RaKey, i: Int): Option[Double] =
    series.get(key).flatMap(a => if (i >= 0 && i < a.length) Some(a(i)) else None)

  // ---- display sign ---------------------------------------------------------

  /**
   * Metrics the report shows with their sign reversed.
   *
   * `CRD` is an outstanding and `INPUTS_RA` carries it as a negative number (it is an exposure).
   * The business asked on 2026-08-27 for it to READ positive — literally `-1 x` the input value —
   * so the curves point the way a reader expects and the `%change` is read against a positive base.
   *
   * This is presentation only, and it changes no arithmetic: `(-n - -o) / -o == (n - o) / o`
   * exactly, so every percentage in the report is the number it was before the flip. It is applied
   * in ONE place — [[displayValueAt]] — so the workbook and the reconciliation CSV can never
   * disagree about the sign of a cell.
   */
  val NegatedMetrics: Set[String] = Set(METRIC_CRD)

  /** `-1` for a metric the report negates, `1` otherwise. */
  def displaySign(metric: String): Double = if (NegatedMetrics.contains(metric)) -1.0 else 1.0

  /** [[valueAt]] with the report's sign convention applied — what a cell actually shows. */
  def displayValueAt(series: Series, key: RaKey, i: Int): Option[Double] =
    valueAt(series, key, i).map { v =>
      val flipped = v * displaySign(key.metric)
      // Negating a zero gives IEEE -0.0, which Excel and the CSV both render as "-0". It is the
      // same number, and reading it as a signed one in a report is a distraction.
      if (flipped == 0.0) 0.0 else flipped
    }

  // ---- table layout ---------------------------------------------------------

  /** The four metrics of the ticket, in the row order of a segment table (design §6.1). */
  val MetricRows: Seq[String] = Seq(METRIC_CRD, METRIC_RA_STAT, METRIC_RA_FI, METRIC_RE)

  /** The three stress legs, in sheet order, and the suffix each one gives its sheet. */
  val FwlOrder: Seq[String] = Seq(FWL_BASELINE, FWL_STRESS_MINUS, FWL_STRESS_PLUS)

  /** `BCEF BASELINE`, `BCEF -100`, `BCEF +100` — the manual workbook's own sheet naming. */
  def sheetName(perimeter: String, fwlType: String): String = {
    val suffix = fwlType match {
      case FWL_STRESS_MINUS => "-100"
      case FWL_STRESS_PLUS  => "+100"
      case other            => other
    }
    s"$perimeter $suffix"
  }

  /**
   * The `(SEGMENT, RATE_TYPE)` tables of one perimeter, in display order.
   *
   * `order` comes from the configuration (Q14). A segment it does not name still appears — after
   * the named ones, alphabetically — so a segment newly added to `INPUTS_RA` can never be silently
   * left out of the report because nobody updated a list.
   */
  def segmentTables(compared: Seq[RaKey], perimeter: String, order: Seq[String]): Seq[(String, String)] = {
    val pairs = compared.filter(_.perimeter == perimeter).map(k => (k.segment, k.rateType)).distinct
    val named = order.flatMap(s => pairs.filter(_._1 == s).sortBy(_._2))
    val rest = pairs.filterNot(p => order.contains(p._1)).sortBy(p => (p._1, p._2))
    named ++ rest
  }

  /** `M1 … Mn` — the column headers of every block. The report carries no calendar dates (Q3/Q4). */
  def monthLabels(months: Int): Seq[String] = (1 to months).map("M" + _)
}
