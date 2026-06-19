package com.bnp.str.tseadfwd.mapping

/**
 * Standalone **yearly** computation core for the EAD FWD Term Structure, kept deliberately separate
 * from the quarterly path in [[PrimaryView]]. The two frequencies use different conventions and must
 * be able to evolve independently, so this object carries its OWN aggregation, period loop, and copy
 * of each RA-detail formula — no computation code is shared with the quarterly path.
 *
 * Annual averaging (`Annual Freq - COMPUTATION` schema, STEP 1): every metric — CRD **and** RA
 * STAT/FI/RE alike — is the MEAN over the year window, `SUM(months)/divisor`:
 *   - Y1 (term 0) = M1..M6 / 6   (half-year, mid-year start)
 *   - Yn (n>=2)   = M(12n-17)..M(12n-6) / 12
 *
 * RA detail (per year, all aggregates being the annual means above):
 *   - FWL=NO          : RA = -RA_STAT / CRD                                  (FI/RE excluded)
 *   - FWL=YES Central : RA = -(RA_STAT + RA_FI + RE) / CRD
 *   - FWL=YES scenario: RA = stat_det + fire_base_det
 *                          + shockSign * (shock_fi + shock_re) * delta
 *     with stat/fire_base from BASELINE, the shock from the scenario's stress leg (STRESS(-) for
 *     Adverse/Extreme, STRESS(+) for Optimistic) detrended by the leg's OWN CRD, and
 *     shockSign = +1 Optimistic / -1 Adverse/Extreme.
 *
 * See `docs/YEAR_CALCULATION_SPECIFICATION.md`. Only frequency-agnostic primitives are reused from
 * [[PrimaryView]] (run-off cap, computed horizon, yearly step, and the [[PrimaryView.vectorFactored]]
 * cumulative product) — none of those are quarterly formulas.
 */
object PrimaryViewYearly {

  import PrimaryView.{RUNOFF_RA_CAP, COMPUTED_HORIZON_Y, YEARLY_STEP}

  // ----- monthly -> year aggregation (MEAN of every metric) ------------------

  /**
   * Annual mean of `m` over year `period` (1-based): `SUM(window months) / divisor`. `isCrd` is
   * accepted only for signature parity with the quarterly aggregator and is ignored here — the
   * yearly convention averages every metric the same way (unlike quarterly, which half-weights the
   * RA metrics). Returns None if the window runs past the available months.
   */
  def aggregate(m: Array[Double], period: Int, isCrd: Boolean): Option[Double] = {
    val (start0, len) = if (period == 1) (0, 6) else (6 + 12 * (period - 2), 12)
    val idx = (0 until len).map(start0 + _)
    val xs  = idx.flatMap(i => if (i >= 0 && i < m.length) Some(m(i)) else None)
    if (xs.length == idx.length) Some(xs.sum / len.toDouble) else None
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
   * FWL=YES non-Central. The CALLER selects the stress leg and `shockSign` by scenario
   * (Adverse/Extreme -> STRESS(-), shockSign -1; Optimistic -> STRESS(+), shockSign +1). Each shock
   * leg is detrended by its OWN CRD; the base stat/fire terms stay BASELINE.
   */
  def scenarioRa(
                  crdBase: Array[Double], raStatBase: Array[Double],
                  raFiBase: Array[Double], reBase: Array[Double],
                  crdLeg: Array[Double], raFiLeg: Array[Double], reLeg: Array[Double],
                  deltaAt: Int => Double,
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
      } yield {
        if (cb == 0.0) 0.0
        else {
          def det(x: Double, c: Double): Double = if (c == 0.0) 0.0 else -x / c
          val statDet     = det(s, cb)
          val fireBaseDet = det(fb, cb) + det(rb, cb)
          val shockFi     = det(fl, cl) - det(fb, cb)
          val shockRe     = det(rl, cl) - det(rb, cb)
          statDet + fireBaseDet + shockSign * (shockFi + shockRe) * deltaAt(period)
        }
      }).filter(_ < RUNOFF_RA_CAP)
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
