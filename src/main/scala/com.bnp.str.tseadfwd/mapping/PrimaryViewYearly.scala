package com.bnp.str.tseadfwd.mapping

/**
 * Standalone **yearly** computation core for the EAD FWD Term Structure, kept deliberately separate
 * from the quarterly path in [[PrimaryView]]. The two frequencies use different conventions and must
 * be able to evolve independently, so this object carries its OWN aggregation, period loop, and copy
 * of each RA-detail formula — no computation code is shared with the quarterly path.
 *
 * Annual aggregation (STEP 1): CRD is the MEAN over the year window (average exposure), while the RA
 * flow metrics (RA STAT/FI/RE) are the raw SUM over the window (annual loss). Window:
 *   - Y1 (term 0) = M1..M6   (half-year, mid-year start; CRD /6)
 *   - Yn (n>=2)   = M(12n-17)..M(12n-6)   (CRD /12)
 * So RA detail = annual-loss-SUM / average-exposure (the divisor applies to CRD only).
 *
 * RA detail (per year; RA metrics summed, CRD averaged, as above):
 *   - FWL=NO           : RA = -RA_STAT / CRD                          (FI/RE excluded)
 *   - FWL=YES Central  : RA = -(RA_STAT + RA_FI + RE) / CRD           (all BASELINE)
 *   - FWL=YES non-Central: OAT-10Y ADDITIVE-shock model. RA STAT and the CRD denominator stay
 *     BASELINE; the FI/RE component takes a macro-weighted parallel shock
 *     `fire_base_det + shockSign*(shock_fi + shock_re)*dOat` (NO ×O173 factor — dropped 2026-06-22 as
 *     it suppressed the spread to ~0). `dOat = (IR_10Y_FR_scen - IR_10Y_FR_central)·100`. The CALLER
 *     selects the leg (Adverse/Extreme -> STRESS(-), Optimistic -> STRESS(+)) and the matching
 *     `shockSign`, so Adverse and Extreme DIFFER (distinct OAT spreads) with a material spread. See
 *     [[scenarioRa]].
 *
 * See `docs/YEAR_CALCULATION_SPECIFICATION.md`. Only frequency-agnostic primitives are reused from
 * [[PrimaryView]] (run-off cap, computed horizon, yearly step, and the [[PrimaryView.vectorFactored]]
 * cumulative product) — none of those are quarterly formulas.
 */
object PrimaryViewYearly {

  import PrimaryView.{RUNOFF_RA_CAP, COMPUTED_HORIZON_Y, YEARLY_STEP}

  // ----- monthly -> year aggregation (RA metrics SUMMED, CRD averaged) -------

  /**
   * Annual aggregation of `m` over year `period` (1-based). The window is Y1 = M1..M6 (6 months),
   * Yn (n>=2) = M(12n-17)..M(12n-6) (12 months). The two metric kinds are combined differently:
   *   - CRD (`isCrd = true`)  -> MEAN, `SUM(window) / divisor`  (average exposure over the year)
   *   - RA STAT / RA FI / RE (`isCrd = false`) -> raw SUM over the window  (annual loss)
   * so the RA detail `-RA_metric / CRD` is `annual-loss-SUM / average-exposure`. Returns None if the
   * window runs past the available months.
   */
  def aggregate(m: Array[Double], period: Int, isCrd: Boolean): Option[Double] = {
    val (start0, len) = if (period == 1) (0, 6) else (6 + 12 * (period - 2), 12)
    val idx = (0 until len).map(start0 + _)
    val xs  = idx.flatMap(i => if (i >= 0 && i < m.length) Some(m(i)) else None)
    if (xs.length == idx.length) Some(if (isCrd) xs.sum / len.toDouble else xs.sum) else None
  }

  // ----- RA detail (own copy of each formula) --------------------------------

  /** FWL=YES Central (and the base of every scenario): `RA = -(RA_STAT + RA_FI + RE)/CRD`. */
  def centralRa(crd: Array[Double], raStat: Array[Double], raFi: Array[Double], re: Array[Double]): Vector[Double] =
    computeRa { period =>
      (for {
        c <- aggregate(crd, period, isCrd = true)
        s <- aggregate(raStat, period, isCrd = false)
        f <- aggregate(raFi, period, isCrd = false)
        r <- aggregate(re, period, isCrd = false)
      } yield if (c == 0.0) 0.0 else -(s + f + r) / c // CRD==0 -> run off, no further loss
      ).filter(_ < RUNOFF_RA_CAP) // RA >= 1 (run-off cliff) -> None -> freeze at last good value
    }

  /** FWL=NO: `RA = -RA_STAT/CRD` — FI and RE are excluded. */
  def statOnlyRa(crd: Array[Double], raStat: Array[Double]): Vector[Double] =
    computeRa { period =>
      (for {
        c <- aggregate(crd, period, isCrd = true)
        s <- aggregate(raStat, period, isCrd = false)
      } yield if (c == 0.0) 0.0 else -s / c
      ).filter(_ < RUNOFF_RA_CAP)
    }

  /**
   * FWL=YES non-Central — yearly **OAT-10Y additive-shock** model. Per the business decision
   * (2026-06-22) the literal Excel `×O173` factor (cells O174-O176) is DROPPED: it made the scenario
   * adjustment proportional to the tiny baseline FI/RE rate, which suppressed the scenario spread to
   * ~0. Instead the FI/RE component takes an ADDITIVE macro-weighted parallel shock — the same shape
   * as the quarterly engine (STEP 2-5), but on the yearly aggregation and OAT spread:
   * {{{
   *   stat_det      = -RA_STAT_base / CRD_base                              // O172 (baseline; all scenarios)
   *   fire_base_det = -(RA_FI_base + RE_base) / CRD_base                    // O173 (baseline)
   *   shock_fi      = (-RA_FI_leg/CRD_leg) - (-RA_FI_base/CRD_base)         // CHANGE in FI rate (each leg's own CRD)
   *   shock_re      = (-RE_leg /CRD_leg)  - (-RE_base /CRD_base)
   *   fire_scen_det = fire_base_det + shockSign*(shock_fi + shock_re)*dOat  // additive — NO ×fire_base_det
   *   RA            = stat_det + fire_scen_det                              // O179-182
   * }}}
   * `oatDeltaAt(period)` = `(IR_10Y_FR_scen - IR_10Y_FR_central)·100` (the signed OAT spread,
   * point-sampled at the year's anniversary). `shockSign` = +1 Optimistic (STRESS(+) leg) / -1
   * Adverse & Extreme (STRESS(-) leg); the CALLER sets it with the matching leg. Together the leg's
   * shock direction and `shockSign` give the correct sign (Adverse worse, Optimistic better) and a
   * material spread. A leg whose CRD has run off (`cl == 0`) contributes 0 (mirrors Excel SIERREUR).
   * RA STAT and the CRD denominator stay BASELINE. See `docs/YEAR_CALCULATION_SPECIFICATION.md` (§2).
   * The quarterly path ([[PrimaryView.scenarioRa]]) is a separate computation and is unchanged.
   */
  def scenarioRa(
                  crdBase: Array[Double], raStatBase: Array[Double], raFiBase: Array[Double], reBase: Array[Double],
                  crdLeg: Array[Double], raFiLeg: Array[Double], reLeg: Array[Double],
                  oatDeltaAt: Int => Double,
                  shockSign: Double
                ): Vector[Double] =
    computeRa { period =>
      (for {
        cb <- aggregate(crdBase, period, isCrd = true)
        s  <- aggregate(raStatBase, period, isCrd = false)
        fb <- aggregate(raFiBase, period, isCrd = false)
        rb <- aggregate(reBase, period, isCrd = false)
        cl <- aggregate(crdLeg, period, isCrd = true)
        fl <- aggregate(raFiLeg, period, isCrd = false)
        rl <- aggregate(reLeg, period, isCrd = false)
      } yield
        if (cb == 0.0) 0.0 // baseline exposure run off -> no further loss
        else {
          def det(x: Double, c: Double): Double = if (c == 0.0) 0.0 else -x / c // -x/c  (loss-rate detail)
          val statDet     = det(s, cb)                  // O172
          val fireBaseDet = det(fb, cb) + det(rb, cb)   // O173 = -(RA_FI+RE)/CRD baseline
          // Parallel shock = leg detail - baseline detail (each leg uses its OWN CRD). A leg whose CRD
          // has run off (cl==0) contributes nothing (Excel SIERREUR: the sensitivity cell -> 0).
          val shockFi     = if (cl == 0.0) 0.0 else det(fl, cl) - det(fb, cb)
          val shockRe     = if (cl == 0.0) 0.0 else det(rl, cl) - det(rb, cb)
          val dOat        = oatDeltaAt(period)          // signed OAT spread (O167/O168/O169)
          val fireScenDet = fireBaseDet + shockSign * (shockFi + shockRe) * dOat // additive (NO ×O173)
          statDet + fireScenDet                         // O179-182
        }
      ).filter(_ < RUNOFF_RA_CAP) // RA >= 1 (run-off cliff) -> None -> freeze at last good value
    }

  /** Yearly period loop: keep years while the window is valid and term <= the computed horizon. */
  private def computeRa(period: Int => Option[Double]): Vector[Double] = {
    val buf = Vector.newBuilder[Double]
    var p = 1
    var continue = true
    while (continue) {
      val term = (p - 1) * YEARLY_STEP
      if (term > COMPUTED_HORIZON_Y) continue = false
      else period(p) match {
        case Some(v) => buf += v; p += 1
        case None    => continue = false
      }
    }
    buf.result()
  }
}
