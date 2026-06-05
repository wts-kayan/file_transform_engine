package com.bnp.str.tseadfwd

import com.bnp.str.tseadfwd.mapping.PrimaryView
import com.bnp.str.tseadfwd.mapping.PrimaryView._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for the pure computation core `PrimaryView` (no Spark). Covers the documented
 * arithmetic (TECHNICAL_SPECIFICATION §4): period aggregation windows, the Central and FWL=YES
 * RA formulas, the macro-shock leg selection, the survival product, and the run-off guards
 * (CRD==0, and the RA>=1 deep-tail cliff freeze + [0,1] clamp added 2026-06-04).
 *
 * Run (offline, via the ScalaTest runner on the test classpath):
 *   mvn -o dependency:build-classpath -Dmdep.outputFile=cp.txt -DincludeScope=test
 *   java -cp "target/classes;target/test-classes;$(cat cp.txt)" \
 *        org.scalatest.tools.Runner -o -s com.bnp.str.tseadfwd.PrimaryViewSpec
 */
class PrimaryViewSpec extends AnyFunSuite with Matchers {

  private val tol = 1e-9
  /** 18-month ramp 1.0 .. 18.0 (m(0)=M1). */
  private val ramp: Array[Double] = (1 to 18).map(_.toDouble).toArray

  // ---- §4.2 period aggregation: quarterly --------------------------------------------------

  test("quarterly RA-metric Q1 = M1 + M2/2 (half-weight)") {
    aggregate(ramp, period = 1, Quarterly, isCrd = false) shouldBe Some(1.0 + 2.0 / 2) // 2.0
  }

  test("quarterly RA-metric Qn = M[3n-4]/2 + M[3n-3] + M[3n-2] + M[3n-1]/2") {
    // period 2 (0-based) -> m1/2 + m2 + m3 + m4/2 = 2/2 + 3 + 4 + 5/2 = 10.5
    aggregate(ramp, period = 2, Quarterly, isCrd = false) shouldBe Some(10.5)
  }

  test("quarterly CRD is the block mean of 3 months") {
    aggregate(ramp, period = 1, Quarterly, isCrd = true) shouldBe Some((1.0 + 2 + 3) / 3) // 2.0
    aggregate(ramp, period = 2, Quarterly, isCrd = true) shouldBe Some((4.0 + 5 + 6) / 3) // 5.0
  }

  // ---- §4.2 period aggregation: yearly -----------------------------------------------------

  test("yearly RA-metric Y1 = sum(M1..M6), CRD Y1 = mean(M1..M6)") {
    aggregate(ramp, period = 1, Yearly, isCrd = false) shouldBe Some(21.0) // 1+..+6
    aggregate(ramp, period = 1, Yearly, isCrd = true) shouldBe Some(3.5)   // 21/6
  }

  test("yearly Yn (n>=2) covers 12 months: Y2 = sum(M7..M18), CRD = mean") {
    aggregate(ramp, period = 2, Yearly, isCrd = false) shouldBe Some(150.0) // 7+..+18
    aggregate(ramp, period = 2, Yearly, isCrd = true) shouldBe Some(12.5)   // 150/12
  }

  test("aggregate returns None when the window exceeds the available months") {
    aggregate(Array(1.0), period = 1, Quarterly, isCrd = false) shouldBe None // needs M1,M2
    aggregate(ramp, period = 3, Yearly, isCrd = false) shouldBe None          // needs M19..M30
  }

  // ---- schema preamble r1: forward-fill short series to M361 -------------------------------

  test("padForward fills a short series flat with the last value; no-op when long enough") {
    padForward(Array(1.0, 2.0, 3.0), 6) shouldBe Array(1.0, 2.0, 3.0, 3.0, 3.0, 3.0)
    padForward(Array(1.0, 2.0, 3.0), 3) shouldBe Array(1.0, 2.0, 3.0) // already >= target
    padForward(Array(1.0, 2.0, 3.0), 2) shouldBe Array(1.0, 2.0, 3.0) // never truncates
    padForward(Array.empty[Double], 5) shouldBe Array.empty[Double]
  }

  // ---- §4.3 central RA formula -------------------------------------------------------------

  test("centralRa = -(RA_STAT + RA_FI + RE) / CRD at period 1 (FWL=YES Central)") {
    val crd = Array.fill(12)(-100.0)
    val ra  = centralRa(crd, Array.fill(12)(10.0), Array.fill(12)(0.0), Array.fill(12)(0.0), Quarterly)
    // STAT_Q1 = 10 + 5 = 15 ; FI = RE = 0 ; RA = -(15)/-100 = 0.15
    ra.head shouldBe (0.15 +- tol)
  }

  test("statOnlyRa (FWL=NO) uses RA_STAT only — FI and RE are excluded") {
    val crd  = Array.fill(12)(-100.0)
    val stat = Array.fill(12)(10.0)
    val fi   = Array.fill(12)(20.0) // present, but the FWL=NO path must ignore it
    statOnlyRa(crd, stat, Quarterly).head shouldBe (0.15 +- tol) // -STAT/CRD = -15/-100
    // centralRa WOULD include FI (FI_Q1 = 30) -> -(15+30)/-100 = 0.45, proving the paths differ
    centralRa(crd, stat, fi, Array.fill(12)(0.0), Quarterly).head shouldBe (0.45 +- tol)
  }

  test("centralRa run-off guard: CRD == 0 -> RA = 0") {
    val crd = Array.fill(12)(0.0)
    val ra  = centralRa(crd, Array.fill(12)(10.0), Array.fill(12)(5.0), Array.fill(12)(5.0), Quarterly)
    every(ra) shouldBe 0.0
  }

  // ---- §4.5 run-off cliff guard (RA >= 1 -> freeze) ----------------------------------------

  test("RUNOFF_RA_CAP is 1.0") {
    PrimaryView.RUNOFF_RA_CAP shouldBe 1.0
  }

  test("centralRa freezes (series stops) when a period's RA >= 1 (deep-tail cliff)") {
    // CRD collapses at period 2 while RA_STAT stays non-zero -> RA_2 ~ 3000 (>=1) -> stop after period 1.
    val crd = Array(-100.0, -100, -100, -0.01, -0.01, -0.01, -0.01, -0.01, -0.01)
    val ra  = centralRa(crd, Array.fill(9)(10.0), Array.fill(9)(0.0), Array.fill(9)(0.0), Quarterly)
    ra.length shouldBe 1            // period 2 was filtered out -> prefix frozen at period 1
    ra.head shouldBe (0.15 +- tol)
  }

  // ---- §4.3 scenario RA (FWL=YES shock) ----------------------------------------------------

  test("scenarioRa with delta == 0 equals centralRa (no shock)") {
    val crd  = Array.fill(12)(-100.0)
    val stat = Array.fill(12)(8.0); val fib = Array.fill(12)(2.0); val reb = Array.fill(12)(1.0)
    val central  = centralRa(crd, stat, fib, reb, Quarterly)
    // leg series arbitrary; delta=0 => shock contributes nothing => RA = STAT+FI+RE detail = central
    val scenario = scenarioRa(crd, stat, fib, reb,
      Array.fill(12)(-100.0), Array.fill(12)(99.0), Array.fill(12)(99.0), Quarterly, _ => 0.0)
    scenario.length shouldBe central.length
    central.zip(scenario).foreach { case (c, s) => s shouldBe (c +- tol) }
  }

  test("scenarioRa applies the schema shock: RA_FI_RE_base - (shockFI + shockRE) * delta") {
    val crd  = Array.fill(9)(-100.0)
    val zero = Array.fill(9)(0.0)
    // base STAT/FI/RE = 0 -> statDet = fireBaseDet = 0; leg FI = 30 with its own CRD -100:
    // FI_leg_Q1 = 30 + 15 = 45 ; det(fl,cl) = -45/-100 = 0.45 ; shockFI = 0.45 - 0 = 0.45
    val fiLeg = Array.fill(9)(30.0)
    def ra(delta: Double) =
      scenarioRa(crd, zero, zero, zero, crd, fiLeg, zero, Quarterly, _ => delta).head
    ra(2.0) shouldBe (-0.90 +- tol)  // 0 - (0.45 + 0) * 2
    ra(-2.0) shouldBe (0.90 +- tol)  // 0 - (0.45 + 0) * -2
  }

  // ---- §4.7 survival factor + clamp --------------------------------------------------------

  test("vectorFactored is the cumulative product of (1 - RA)") {
    val vf = vectorFactored(Vector(0.1, 0.2))
    vf(0) shouldBe (0.9 +- tol)
    vf(1) shouldBe (0.72 +- tol) // 0.9 * 0.8
  }

  test("vectorFactored clamps the emitted value into [0,1]") {
    vectorFactored(Vector(-1.0)).head shouldBe (1.0 +- tol) // 1*(1-(-1))=2 -> clamp to 1
    vectorFactored(Vector(2.0)).head shouldBe (0.0 +- tol)  // 1*(1-2)=-1  -> clamp to 0
  }

  // ---- §4.8 term grid + flat tail ----------------------------------------------------------

  test("termGrid sizes and endpoints (203 quarterly, 52 yearly; tail term 100)") {
    val q = termGrid(Quarterly)
    q.length shouldBe 203
    q.head shouldBe 0.0
    q(201) shouldBe 50.25
    q.last shouldBe 100.0
    val y = termGrid(Yearly)
    y.length shouldBe 52
    y(50) shouldBe 50.0
    y.last shouldBe 100.0
  }

  test("termSeries holds the last computed value flat for terms beyond the series") {
    val ts = termSeries(Vector(0.9, 0.8), Quarterly)
    ts.length shouldBe 203
    ts.head shouldBe (0.0, 0.9)
    ts(1) shouldBe (0.25, 0.8)
    ts(5)._2 shouldBe 0.8       // flat past the 2-element series
    ts.last shouldBe (100.0, 0.8)
  }
}
