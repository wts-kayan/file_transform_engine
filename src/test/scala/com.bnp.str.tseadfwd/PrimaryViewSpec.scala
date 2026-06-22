package com.bnp.str.tseadfwd

import com.bnp.str.tseadfwd.mapping.PrimaryView
import com.bnp.str.tseadfwd.mapping.PrimaryView._
import com.bnp.str.tseadfwd.mapping.PrimaryViewYearly
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
  /** A constant 6-month series (one yearly Y1 window: M1..M6 -> a single computed period). */
  private def six(x: Double): Array[Double] = Array.fill(6)(x)

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

  test("yearly Y1: RA metrics = SUM(M1..M6), CRD = MEAN(M1..M6)") {
    aggregate(ramp, period = 1, Yearly, isCrd = false) shouldBe Some(21.0) // 1+..+6 (raw sum)
    aggregate(ramp, period = 1, Yearly, isCrd = true) shouldBe Some(3.5)   // 21/6 (mean)
  }

  test("yearly Yn (n>=2) covers 12 months: Y2 RA = SUM(M7..M18), CRD = MEAN") {
    aggregate(ramp, period = 2, Yearly, isCrd = false) shouldBe Some(150.0) // 7+..+18 (raw sum)
    aggregate(ramp, period = 2, Yearly, isCrd = true) shouldBe Some(12.5)   // 150/12 (mean)
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
      Array.fill(12)(-100.0), Array.fill(12)(99.0), Array.fill(12)(99.0), Quarterly, _ => 0.0, -1.0)
    scenario.length shouldBe central.length
    central.zip(scenario).foreach { case (c, s) => s shouldBe (c +- tol) }
  }

  test("scenarioRa Adverse/Extreme (shockSign -1): RA_FI_RE_base - (shockFI + shockRE) * delta") {
    val crd  = Array.fill(9)(-100.0)
    val zero = Array.fill(9)(0.0)
    // base STAT/FI/RE = 0 -> statDet = fireBaseDet = 0; leg FI = 30 with its own CRD -100:
    // FI_leg_Q1 = 30 + 15 = 45 ; det(fl,cl) = -45/-100 = 0.45 ; shockFI = 0.45 - 0 = 0.45
    val fiLeg = Array.fill(9)(30.0)
    def ra(delta: Double) =
      scenarioRa(crd, zero, zero, zero, crd, fiLeg, zero, Quarterly, _ => delta, -1.0).head
    ra(2.0) shouldBe (-0.90 +- tol)  // 0 - (0.45 + 0) * 2
    ra(-2.0) shouldBe (0.90 +- tol)  // 0 - (0.45 + 0) * -2
  }

  test("scenarioRa Optimistic (shockSign +1): RA_FI_RE_base + (shockFI + shockRE) * delta") {
    val crd  = Array.fill(9)(-100.0)
    val zero = Array.fill(9)(0.0)
    // same setup as above: shockFI = 0.45, shockRE = 0; only the sign flips (STRESS(+) leg added).
    val fiLeg = Array.fill(9)(30.0)
    def ra(delta: Double) =
      scenarioRa(crd, zero, zero, zero, crd, fiLeg, zero, Quarterly, _ => delta, 1.0).head
    ra(2.0) shouldBe (0.90 +- tol)   // 0 + (0.45 + 0) * 2
    ra(-2.0) shouldBe (-0.90 +- tol) // 0 + (0.45 + 0) * -2
  }

  // ---- yearly OAT-10Y sensitivity scenario RA (Excel O155-O182, ×O173) ----------------------
  //
  // PrimaryViewYearly.scenarioRa keeps RA STAT and the CRD denominator at BASELINE and adjusts FI/RE
  // by the per-leg sensitivity scaled by the OAT spread, WITH the ×fire_base_det factor:
  //   stat_det      = -RA_STAT_base/CRD_base                          (O172)
  //   fire_base_det = -(RA_FI_base + RE_base)/CRD_base                (O173)
  //   sens_fi       = (-RA_FI_leg/CRD_leg) - (RA_FI_base/CRD_base)    (O155/O157, literal Excel)
  //   sens_re       = (-RE_leg /CRD_leg)  - (RE_base /CRD_base)       (O156/O158)
  //   fire_scen_det = fire_base_det*(1 - (sens_fi+sens_re)*dOAT)      (O174-176)
  //   RA            = stat_det + fire_scen_det                        (O179-182)
  // dOAT = (IR_10Y_FR scen - Central)*10000 (signed; its sign gives the scenario direction — no
  // shockSign). Y1 aggregation: CRD = MEAN(M1..M6); RA metrics = SUM(M1..M6).

  test("yearly scenarioRa with dOAT == 0 equals yearly centralRa (no OAT spread)") {
    val crd = six(-100.0); val stat = six(8.0); val fib = six(2.0); val reb = six(1.0)
    val central = PrimaryViewYearly.centralRa(crd, stat, fib, reb) // -(48+12+6)/-100 = 0.66
    // arbitrary leg; dOAT=0 => fire_scen_det = fire_base_det*(1-0) = fire_base_det => RA = central
    val scen = PrimaryViewYearly.scenarioRa(crd, stat, fib, reb, six(-100.0), six(99.0), six(99.0), _ => 0.0)
    central.head shouldBe (0.66 +- tol)
    scen.length shouldBe central.length
    central.zip(scen).foreach { case (c, s) => s shouldBe (c +- tol) }
  }

  test("yearly scenarioRa worked case: RA = stat_det + fire_base_det*(1 - (sens_fi+sens_re)*dOAT)") {
    // baseline: CRD mean -100; STAT sum 30 -> stat_det 0.30; FI sum 30 -> fire_base_det 0.30; RE 0.
    // leg: CRD mean -100; FI sum 90 -> det 0.90; sens_fi = 0.90 - (30/-100) = 1.20; sens_re = 0.
    // => RA = 0.30 + 0.30*(1 - 1.20*dOAT) = 0.60 - 0.36*dOAT
    val crd = six(-100.0); val stat = six(5.0); val fib = six(5.0); val zero = six(0.0)
    val legCrd = six(-100.0); val legFi = six(15.0)
    def ra(dOat: Double) =
      PrimaryViewYearly.scenarioRa(crd, stat, fib, zero, legCrd, legFi, zero, _ => dOat).head
    ra(0.0) shouldBe (0.60 +- tol)   // no spread
    ra(0.5) shouldBe (0.42 +- tol)   // 0.60 - 0.36*0.5  (positive dOAT -> lower RA, e.g. Optimistic)
    ra(-0.5) shouldBe (0.78 +- tol)  // 0.60 + 0.18      (negative dOAT -> higher RA, e.g. Adverse)
  }

  test("yearly Adverse != Extreme: same STRESS(-) leg, different OAT spread -> different RA") {
    // RA = 0.60 - 0.36*dOAT (setup as above); Adverse/Extreme both have dOAT<0 but distinct.
    val crd = six(-100.0); val stat = six(5.0); val fib = six(5.0); val zero = six(0.0)
    val legCrd = six(-100.0); val legFi = six(15.0)
    val adverse = PrimaryViewYearly.scenarioRa(crd, stat, fib, zero, legCrd, legFi, zero, _ => -0.325).head
    val extreme = PrimaryViewYearly.scenarioRa(crd, stat, fib, zero, legCrd, legFi, zero, _ => -0.125).head
    adverse shouldBe (0.60 + 0.36 * 0.325 +- tol) // 0.71700  (worse: higher RA)
    extreme shouldBe (0.60 + 0.36 * 0.125 +- tol) // 0.64500
    adverse should be > extreme                    // the old ×100-scaled run made these near-identical
  }

  test("yearly scenarioRa: when fire_base_det == 0 (FI=RE=0) every scenario equals Central") {
    // The ×fire_base_det factor zeroes the OAT adjustment, so a CONSO-like book (no FI/RE) is
    // scenario-invariant: RA = stat_det for any leg / OAT spread.
    val crd = six(-100.0); val stat = six(5.0); val zero = six(0.0)
    val central = PrimaryViewYearly.centralRa(crd, stat, zero, zero).head // -30/-100 = 0.30
    central shouldBe (0.30 +- tol)
    PrimaryViewYearly.scenarioRa(crd, stat, zero, zero, six(-100.0), six(99.0), six(7.0), _ => 5.0).head shouldBe (0.30 +- tol)
    PrimaryViewYearly.scenarioRa(crd, stat, zero, zero, six(-100.0), six(99.0), six(7.0), _ => -30.0).head shouldBe (0.30 +- tol)
  }

  test("yearly scenarioRa: leg CRD == 0 (run-off) zeroes the sensitivity (Excel SIERREUR) -> RA = Central") {
    // A 0 leg CRD makes the sensitivity cell error to 0, so the scenario FI/RE collapses to baseline.
    val crd = six(-100.0); val stat = six(8.0); val fib = six(2.0); val reb = six(1.0)
    val central = PrimaryViewYearly.centralRa(crd, stat, fib, reb).head // 0.66
    central shouldBe (0.66 +- tol)
    PrimaryViewYearly.scenarioRa(crd, stat, fib, reb, six(0.0), six(20.0), six(5.0), _ => 0.5).head shouldBe (0.66 +- tol)
  }

  test("yearly scenarioRa run-off guard: baseline CRD == 0 -> RA = 0") {
    val ra = PrimaryViewYearly.scenarioRa(six(0.0), six(5.0), six(10.0), six(1.0), six(-100.0), six(20.0), six(2.0), _ => 0.5)
    every(ra) shouldBe 0.0
  }

  // ---- §4.7 survival factor + clamp --------------------------------------------------------

  test("vectorFactored is the cumulative product of (1 - RA)") {
    val vf = vectorFactored(Vector(0.1, 0.2))
    vf(0) shouldBe (0.9 +- tol)
    vf(1) shouldBe (0.72 +- tol) // 0.9 * 0.8
  }

  test("vectorFactored caps at 1, and reports a negative running product as 1 (full exposure)") {
    vectorFactored(Vector(-1.0)).head shouldBe (1.0 +- tol) // 1*(1-(-1))=2  -> capped at 1
    vectorFactored(Vector(2.0)).head shouldBe (1.0 +- tol)  // 1*(1-2)=-1 (<0) -> reported as 1, per PrimaryView
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
