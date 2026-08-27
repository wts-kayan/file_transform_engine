package com.bnp.str.tseadfwd

import com.bnp.str.tseadfwd.mapping.RaCompareView
import com.bnp.str.tseadfwd.mapping.RaCompareView._
import com.bnp.str.tseadfwd.utility.PrimaryConstants._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Acceptance tests T1, T2 and T4 of the RA comparison design (docs/tseadfwd/977, §9).
 *
 * No Spark and no workbook: [[RaCompareView]] is the pure core, and these are the rules the
 * business signs off — the %change arithmetic, what a degenerate cell does, and which keys are
 * compared rather than dropped. T3 was struck when the derived rows were dropped (Q6/Q11); T5 and
 * T7 need a produced workbook and live with the writer.
 */
class RaCompareViewSpec extends AnyFunSuite with Matchers {

  private def key(seg: String, metric: String, perim: String = "BCEF",
                  rate: String = "TF", fwl: String = FWL_BASELINE) =
    RaKey(perim, seg, rate, fwl, metric)

  private def series(entries: (RaKey, Seq[Double])*): Series =
    entries.map { case (k, v) => k -> v.toArray }.toMap

  // ---- T1: the %change formula ---------------------------------------------

  test("T1 pctChange is (new - old) / old") {
    pctChange(Some(110.0), Some(100.0))._1.get shouldBe (0.1 +- 1e-12)
    pctChange(Some(90.0), Some(100.0))._1.get shouldBe (-0.1 +- 1e-12)
    pctChange(Some(100.0), Some(100.0))._1.get shouldBe 0.0
  }

  test("T1 a negative CRD is compared as written, with no abs()") {
    // CRD is negative (it is an exposure). -92368 against -92925 is a SHRINKING exposure, so the
    // relative change is negative; taking absolute values would flip the sign of every CRD move.
    val (pct, status) = pctChange(Some(-92368.0), Some(-92925.0))
    status shouldBe CellStatus.Ok
    pct.get should be < 0.0
    pct.get shouldBe ((-92368.0 + 92925.0) / -92925.0 +- 1e-12)
  }

  test("T1 both sides negative and growing gives a positive change") {
    pctChange(Some(-110.0), Some(-100.0))._1.get shouldBe (0.1 +- 1e-12)
  }

  // ---- T2: degenerate cases (design section 5) ------------------------------

  test("T2 zero against zero is blank and is NOT reported as a problem") {
    // Real case: CONSO carries RA FI = RE = 0 on both sides. There is no change to report, so the
    // cell is blank but the row is not listed as not-computable.
    val (pct, status) = pctChange(Some(0.0), Some(0.0))
    pct shouldBe None
    status shouldBe CellStatus.Ok
  }

  test("T2 a non-zero new value against a zero previous one is NOT_COMPUTABLE") {
    val (pct, status) = pctChange(Some(5.0), Some(0.0))
    pct shouldBe None
    status shouldBe CellStatus.NotComputable
  }

  test("T2 a value missing on one side is reported as such, not as a change") {
    pctChange(Some(5.0), None) shouldBe ((None, CellStatus.OnlyNew))
    pctChange(None, Some(5.0)) shouldBe ((None, CellStatus.OnlyOld))
    pctChange(None, None) shouldBe ((None, CellStatus.Ok))
  }

  test("T2 an overflowing ratio is not computable rather than Infinity in the report") {
    val (pct, status) = pctChange(Some(Double.MaxValue), Some(Double.MinPositiveValue))
    pct shouldBe None
    status shouldBe CellStatus.NotComputable
  }

  test("T2 a month past the end of a short series is missing, not zero") {
    val s = series(key("CONSO", METRIC_CRD) -> Seq(1.0, 2.0))
    valueAt(s, key("CONSO", METRIC_CRD), 1) shouldBe Some(2.0)
    valueAt(s, key("CONSO", METRIC_CRD), 2) shouldBe None
    valueAt(s, key("MORTGAGE", METRIC_CRD), 0) shouldBe None
  }

  // ---- T4: scope — only common keys, everything else named ------------------

  test("T4 only keys present in both files are compared") {
    val n = series(
      key("MORTGAGE", METRIC_CRD) -> Seq(1.0, 2.0),
      key("CONSO", METRIC_CRD) -> Seq(3.0, 4.0))
    val o = series(
      key("MORTGAGE", METRIC_CRD) -> Seq(1.0, 2.0),
      key("INVEST_PRO", METRIC_CRD) -> Seq(9.0, 9.0))

    val r = compare(n, o, 2, 2)
    r.compared shouldBe Seq(key("MORTGAGE", METRIC_CRD))
    r.onlyNew shouldBe Seq(key("CONSO", METRIC_CRD))
    r.onlyOld shouldBe Seq(key("INVEST_PRO", METRIC_CRD))
    r.droppedCount shouldBe 2
  }

  test("T4 the whole five-part key has to match, not just the segment") {
    // Same segment and metric, different stress leg: NOT the same series.
    val n = series(key("MORTGAGE", METRIC_CRD, fwl = FWL_BASELINE) -> Seq(1.0))
    val o = series(key("MORTGAGE", METRIC_CRD, fwl = FWL_STRESS_MINUS) -> Seq(1.0))

    val r = compare(n, o, 1, 1)
    r.compared shouldBe empty
    r.onlyNew.size shouldBe 1
    r.onlyOld.size shouldBe 1
  }

  test("T4 the horizon is the shortest of both files and the truncation is stated") {
    val n = series(key("MORTGAGE", METRIC_CRD) -> Seq(1.0, 2.0, 3.0, 4.0))
    val o = series(key("MORTGAGE", METRIC_CRD) -> Seq(1.0, 2.0))

    val r = compare(n, o, 4, 2)
    r.months shouldBe 2
    r.newTruncated shouldBe true
    r.oldTruncated shouldBe false
  }

  test("T4 a short series shortens the horizon even when the column count agrees") {
    val n = series(
      key("MORTGAGE", METRIC_CRD) -> Seq(1.0, 2.0, 3.0),
      key("CONSO", METRIC_CRD) -> Seq(1.0))          // one metric stops early
    val o = series(
      key("MORTGAGE", METRIC_CRD) -> Seq(1.0, 2.0, 3.0),
      key("CONSO", METRIC_CRD) -> Seq(1.0, 2.0, 3.0))

    compare(n, o, 3, 3).months shouldBe 1
  }

  test("T4 no shared key at all is an empty comparison, not a crash") {
    val r = compare(series(key("A", METRIC_CRD) -> Seq(1.0)),
                    series(key("B", METRIC_CRD) -> Seq(1.0)), 1, 1)
    r.compared shouldBe empty
    r.perimeters shouldBe empty
  }

  // ---- cells ----------------------------------------------------------------

  test("cells covers every compared key and month, and only compared keys") {
    val n = series(
      key("MORTGAGE", METRIC_CRD) -> Seq(110.0, 0.0),
      key("CONSO", METRIC_CRD) -> Seq(1.0, 1.0))     // only in the new file
    val o = series(key("MORTGAGE", METRIC_CRD) -> Seq(100.0, 0.0))

    val r = compare(n, o, 2, 2)
    val cs = cells(r, n, o)

    cs.size shouldBe 2                                // 1 compared key x 2 months
    cs.map(_.key).distinct shouldBe Seq(key("MORTGAGE", METRIC_CRD))
    cs.head.month shouldBe 1
    cs.head.pctChange.get shouldBe (0.1 +- 1e-12)
    cs(1).pctChange shouldBe None                     // 0 against 0
    cs(1).status shouldBe CellStatus.Ok
  }

  // ---- layout ---------------------------------------------------------------

  test("segment tables follow the configured order, unlisted ones after it alphabetically") {
    val compared = Seq(
      key("CONSO", METRIC_CRD), key("MORTGAGE", METRIC_CRD),
      key("ZZZ_NEW", METRIC_CRD), key("AAA_NEW", METRIC_CRD))

    RaCompareView.segmentTables(compared, "BCEF", Seq("MORTGAGE", "CONSO")) shouldBe
      Seq(("MORTGAGE", "TF"), ("CONSO", "TF"), ("AAA_NEW", "TF"), ("ZZZ_NEW", "TF"))
  }

  test("a segment the configured order does not name is still reported") {
    // The point of the rule: adding a segment to INPUTS_RA must not require editing a list for it
    // to appear in the report.
    val compared = Seq(key("BRAND_NEW", METRIC_CRD))
    RaCompareView.segmentTables(compared, "BCEF", Seq("MORTGAGE")) shouldBe Seq(("BRAND_NEW", "TF"))
  }

  test("one table per (segment, rate type), not per segment") {
    val compared = Seq(
      key("MORTGAGE", METRIC_CRD, rate = "TF"),
      key("MORTGAGE", METRIC_CRD, rate = "TV"))
    RaCompareView.segmentTables(compared, "BCEF", Seq("MORTGAGE")) shouldBe
      Seq(("MORTGAGE", "TF"), ("MORTGAGE", "TV"))
  }

  test("sheet names follow the manual workbook") {
    RaCompareView.sheetName("BCEF", FWL_BASELINE) shouldBe "BCEF BASELINE"
    RaCompareView.sheetName("BCEF", FWL_STRESS_MINUS) shouldBe "BCEF -100"
    RaCompareView.sheetName("BCEF", FWL_STRESS_PLUS) shouldBe "BCEF +100"
  }

  test("metric rows are the four input metrics, with nothing derived") {
    RaCompareView.MetricRows shouldBe Seq(METRIC_CRD, METRIC_RA_STAT, METRIC_RA_FI, METRIC_RE)
    RaCompareView.MetricRows should not contain "RA"
    RaCompareView.MetricRows should not contain "%RA"
  }

  test("months are labelled M1..Mn, never as dates") {
    RaCompareView.monthLabels(3) shouldBe Seq("M1", "M2", "M3")
    RaCompareView.monthLabels(0) shouldBe empty
  }
}
