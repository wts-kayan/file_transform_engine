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
 *   - FWL=YES non-Central: OAT-10Y SENSITIVITY model (Excel O155-O182). RA STAT and the CRD
 *     denominator stay BASELINE; only FI/RE is adjusted, by each leg's sensitivity scaled by the OAT
 *     spread WITH the ×fire_base_det factor: `fire_scen_det = fire_base_det*(1 - (sens_fi+sens_re)*dOat)`.
 *     `dOat = (IR_10Y_FR_scen - IR_10Y_FR_central)·10000` (the workbook's percent·100 on the decimal
 *     scenario file). The CALLER selects the leg (Adverse/Extreme -> STRESS(-), Optimistic -> STRESS(+));
 *     the spread's sign gives the direction, so Adverse and Extreme DIFFER. Reproduces the workbook to
 *     8 dp (e.g. MORTGAGE Adverse term 1 EAD = 0.90785228). See [[scenarioRa]].
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
   * FWL=YES non-Central — yearly **OAT-10Y sensitivity model** (Excel `Calc and Interpol #Mortgage`,
   * cells O155–O182), VALIDATED to reproduce the workbook's term-0/term-1 cells to 8 dp. RA STAT and
   * the CRD denominator stay BASELINE; only the FI/RE component is scenario-adjusted, by each leg's
   * sensitivity scaled by the OAT-10Y spread, WITH the `×fire_base_det` factor (cells O174–O176):
   * {{{
   *   stat_det      = -RA_STAT_base / CRD_base                              // O172
   *   fire_base_det = -(RA_FI_base + RE_base) / CRD_base                    // O173
   *   sens_fi       = (-RA_FI_leg/CRD_leg) - (RA_FI_base/CRD_base)          // O155 (+100) / O157 (-100), literal Excel
   *   sens_re       = (-RE_leg  /CRD_leg) - (RE_base  /CRD_base)            // O156 (+100) / O158 (-100)
   *   fire_scen_det = fire_base_det * (1 - (sens_fi + sens_re) * dOat)      // O174-176 (= O173 - ((O173·sens)·dOat))
   *   RA            = stat_det + fire_scen_det                              // O179-182
   * }}}
   * `oatDeltaAt(period)` is the signed OAT spread `(IR_10Y_FR_scen - IR_10Y_FR_central)·10000`,
   * point-sampled at the year's anniversary — the workbook's `(percent_scen - percent_base)·100`
   * expressed on the decimal scenario file (percent = decimal·100, hence the ·10000). The CALLER
   * selects the leg by scenario (Optimistic -> STRESS(+), Adverse/Extreme -> STRESS(-)); the SIGN of
   * the spread alone gives the correct direction (Adverse worse, Optimistic better), so no separate
   * shockSign is needed. A leg whose CRD has run off (`cl == 0`) contributes 0 (Excel SIERREUR).
   *
   * Worked check vs the workbook (`BCEF_MORTGAGE_TF_Y`, Adverse, year 2): `fire_base_det = 0.00701853`,
   * `sens_fi+sens_re = 0.06310285`, `dOat = (0.03075-0.034)·10000 = -32.5` →
   * `fire_scen_det = 0.00701853·(1 - 0.06310285·(-32.5)) = 0.02141246`, `RA = 0.06827077`,
   * `EAD term 1 = 0.97437351·(1-0.06827077) = 0.90785228` — matches the Excel exactly.
   * See `docs/YEAR_CALCULATION_SPECIFICATION.md` (§2). Quarterly ([[PrimaryView.scenarioRa]]) unchanged.
   */
  def scenarioRa(
                  crdBase: Array[Double], raStatBase: Array[Double], raFiBase: Array[Double], reBase: Array[Double],
                  crdLeg: Array[Double], raFiLeg: Array[Double], reLeg: Array[Double],
                  oatDeltaAt: Int => Double
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
          def det(x: Double, c: Double): Double   = if (c == 0.0) 0.0 else -x / c // -x/c  (loss-rate detail)
          def ratio(x: Double, c: Double): Double = if (c == 0.0) 0.0 else x / c  //  x/c  (literal Excel base term)
          val statDet     = det(s, cb)                  // O172
          val fireBaseDet = det(fb, cb) + det(rb, cb)   // O173 = -(RA_FI+RE)/CRD baseline
          // O155-O158 each wrapped in Excel SIERREUR(...,0): a 0 leg CRD (cl==0) -> whole sensitivity 0.
          val sensFi      = if (cl == 0.0) 0.0 else det(fl, cl) - ratio(fb, cb) // O155/O157
          val sensRe      = if (cl == 0.0) 0.0 else det(rl, cl) - ratio(rb, cb) // O156/O158
          val dOat        = oatDeltaAt(period)          // (scen-central)·10000 (O167/O168/O169)
          val fireScenDet = fireBaseDet - ((fireBaseDet * sensFi + fireBaseDet * sensRe) * dOat) // O174-176
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
