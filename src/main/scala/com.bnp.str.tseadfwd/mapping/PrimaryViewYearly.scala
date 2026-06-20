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
 *   - FWL=YES non-Central: PURE STRESS — RA = -(RA_STAT + RA_FI + RE) / CRD computed ENTIRELY on the
 *     scenario's stress leg (Adverse/Extreme -> STRESS(-), Optimistic -> STRESS(+)). No baseline term
 *     and no macro shock/Rate, so Adverse == Extreme.
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
   * FWL=YES non-Central — **pure-stress model** (yearly only). The scenario's loss rate is computed
   * ENTIRELY on its stress leg: `RA = -(RA_STAT + RA_FI + RE) / CRD`, with every metric taken from
   * the leg (Adverse/Extreme -> STRESS(-), Optimistic -> STRESS(+)). This is identical to
   * [[centralRa]] applied to the leg series — there is NO baseline term and NO macro shock/Rate, so
   * Adverse and Extreme (both STRESS(-)) are identical. The CALLER selects the leg by scenario.
   * See `docs/YEAR_CALCULATION_SPECIFICATION.md`.
   */
  def scenarioRa(
                  crdLeg: Array[Double], raStatLeg: Array[Double],
                  raFiLeg: Array[Double], reLeg: Array[Double]
                ): Vector[Double] =
    centralRa(crdLeg, raStatLeg, raFiLeg, reLeg)

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
