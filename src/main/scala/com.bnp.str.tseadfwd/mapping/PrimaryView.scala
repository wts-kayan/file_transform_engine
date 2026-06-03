package com.bnp.str.tseadfwd.mapping

/**
 * Pure computation core for the EAD FWD Term Structure (no Spark dependency).
 *
 * All inputs are tiny reference series (a few dozen rows of 361 monthly points), so the
 * numeric work is done on the driver with plain Scala collections and validated against
 * `target_output/TS_EAD_FWD_25Q4_v1_small.csv`.
 *
 * Validated formula (see project memory `ead-fwd-formula`):
 *   - Quarterly aggregation (RA STAT / RA FI / RE), half-weight overlapping window:
 *       Q1 = M1 + M2/2
 *       Qn = M[3n-4]/2 + M[3n-3] + M[3n-2] + M[3n-1]/2   (1-based, step 3)
 *   - Quarterly CRD, block average: Qi = mean(M[3i-2], M[3i-1], M[3i])
 *   - Yearly aggregation: RA metrics = SUM over window (Y1 = 6 months, Yn = 12 months);
 *     CRD = MEAN over the same window.
 *   - Core RA (Central / FWL=NO):  RA_i = -(RA_STAT_i + RA_FI_i + RE_i) / CRD_i   (BASELINE)
 *   - VECTOR_i = 1 - RA_i ; EAD_RA_RATE = cumulative product of VECTOR.
 *   - Computation runs to term 30y; from term 30 on the value is held flat (... 50, 100).
 */
object PrimaryView {

  // ----- term grid -----------------------------------------------------------
  val QUARTERLY_STEP = 0.25
  val YEARLY_STEP    = 1.0
  /** Last computed term (years); beyond this the value is held flat. */
  val COMPUTED_HORIZON_Y = 30.0
  /** Last flat term before the long tail. */
  val FLAT_MAX_Y = 50.0
  /** Long tail term. */
  val TAIL_TERM = 100.0

  /**
   * Run-off cliff guard. `RA` is a per-period loss rate, so `RA >= 1` is non-physical: it only
   * happens in the deep tail when the exposure amortizes to ~0 and `|CRD|` collapses faster than
   * the numerator's (offset) aggregation window — `RA = -(STAT+FI+RE)/CRD` then explodes (e.g.
   * CRD -1.9M -> -2.87 in one quarter while RA_STAT lags at 2547 -> RA ~ 895), driving the
   * cumulative product negative. The documented `CRD == 0` guard misses it because CRD lands on a
   * tiny non-zero value. When RA crosses this cap we treat the book as run off and FREEZE the curve
   * (computeRa stops -> termSeries holds the last good value flat). See OPEN_QUESTIONS Q26.
   */
  val RUNOFF_RA_CAP = 1.0

  sealed trait Frequency { def suffix: String; def step: Double; def coreMax: Double }
  // Quarterly carries one extra step (..50.25) vs Yearly (..50), matching the target grid.
  case object Quarterly extends Frequency { val suffix = "Q"; val step = QUARTERLY_STEP; val coreMax = FLAT_MAX_Y + QUARTERLY_STEP }
  case object Yearly    extends Frequency { val suffix = "Y"; val step = YEARLY_STEP;    val coreMax = FLAT_MAX_Y }

  /** Output term grid for a frequency: 0 .. coreMax by step, then the tail term. */
  def termGrid(freq: Frequency): Vector[Double] = {
    val n = Math.round(freq.coreMax / freq.step).toInt
    val core = (0 to n).map(i => round2(i * freq.step)).toVector
    core :+ TAIL_TERM
  }

  /** Round to a clean grid value (kills binary FP noise like 0.30000000004). */
  private def round2(x: Double): Double = Math.round(x * 1e6) / 1e6

  // ----- monthly -> period aggregation --------------------------------------

  /**
   * Aggregate a monthly series into one period value.
   *
   * @param m       monthly series, 0-based (m(0) == M1)
   * @param period  1-based period index (quarter or year)
   * @param freq    Quarterly or Yearly
   * @param isCrd   CRD uses an average; RA metrics use a (half-weighted) sum
   * @return Some(value) if the window fits within the available months, else None
   */
  def aggregate(m: Array[Double], period: Int, freq: Frequency, isCrd: Boolean): Option[Double] =
    freq match {
      case Quarterly => aggregateQuarter(m, period, isCrd)
      case Yearly    => aggregateYear(m, period, isCrd)
    }

  private def at(m: Array[Double], idx0: Int): Option[Double] =
    if (idx0 >= 0 && idx0 < m.length) Some(m(idx0)) else None

  private def aggregateQuarter(m: Array[Double], q: Int, isCrd: Boolean): Option[Double] = {
    if (isCrd) {
      // block average of months [3q-2, 3q-1, 3q] (1-based) -> 0-based [3q-3 .. 3q-1]
      val idx = (0 until 3).map(j => 3 * (q - 1) + j)
      seqAt(m, idx).map(xs => xs.sum / 3.0)
    } else if (q == 1) {
      for { a <- at(m, 0); b <- at(m, 1) } yield a + b / 2.0
    } else {
      // half-weight window M[3q-4]/2 + M[3q-3] + M[3q-2] + M[3q-1]/2 -> 0-based 3q-5 .. 3q-2
      val s = 3 * q - 5
      for { a <- at(m, s); b <- at(m, s + 1); c <- at(m, s + 2); d <- at(m, s + 3) }
        yield a / 2.0 + b + c + d / 2.0
    }
  }

  private def aggregateYear(m: Array[Double], y: Int, isCrd: Boolean): Option[Double] = {
    // Y1 covers 6 months (M1..M6); Yn (n>=2) covers 12 months.
    val (start0, len) =
      if (y == 1) (0, 6)
      else (6 + 12 * (y - 2), 12)
    val idx = (0 until len).map(start0 + _)
    seqAt(m, idx).map(xs => if (isCrd) xs.sum / len.toDouble else xs.sum)
  }

  private def seqAt(m: Array[Double], idx: Seq[Int]): Option[Seq[Double]] = {
    val xs = idx.flatMap(i => at(m, i))
    if (xs.length == idx.length) Some(xs) else None
  }

  // ----- RA detail & vector factored ----------------------------------------

  /**
   * Per-period "RA" detail for the Central scenario (and for every scenario when FWL=NO):
   *   RA_i = -(RA_STAT_i + RA_FI_i + RE_i) / CRD_i
   * Returns the largest contiguous prefix of periods whose aggregation window is valid
   * and whose term does not exceed COMPUTED_HORIZON_Y.
   */
  def centralRa(
                 crd: Array[Double],
                 raStat: Array[Double],
                 raFi: Array[Double],
                 re: Array[Double],
                 freq: Frequency
               ): Vector[Double] = computeRa(freq) { period =>
    (for {
      c <- aggregate(crd, period, freq, isCrd = true)
      s <- aggregate(raStat, period, freq, isCrd = false)
      f <- aggregate(raFi, period, freq, isCrd = false)
      r <- aggregate(re, period, freq, isCrd = false)
    } yield if (c == 0.0) 0.0 else -(s + f + r) / c // CRD==0 -> exposure run off, no further loss
    ).filter(_ < RUNOFF_RA_CAP) // RA >= 1 (run-off cliff) -> None -> freeze at last good value
  }

  /**
   * Per-period RA detail for a non-central scenario under FWL=YES.
   *
   * RA_i(scen) = RA_STAT_detail_i(BASELINE) + RA_FIRE_detail_i(scen)
   * where the FI+RE detail is linearly interpolated from BASELINE towards a STRESS leg by
   * the macro rate delta, which is read **per period** from the scenario path (`deltaAt`):
   *   delta_i = deltaAt(period_i)                         (signed)
   *   leg     = STRESS(-) if delta_i < 0 else STRESS(+)   (direction of the rate move)
   *   weight  = |delta_i| / refShock
   *   FIRE_detail_i(scen) = FIRE_base + weight * (FIRE_leg - FIRE_base)
   *
   * NOTE: the exact shock scaling is the calibration point for FWL=YES (`refShock`).
   * Both stress legs are passed in; `deltaAt` selects the leg and weight per term, so a
   * ramping macro path naturally produces a term-varying shock.
   */
  def scenarioRa(
                  crd: Array[Double],
                  raStatBase: Array[Double],
                  raFiBase: Array[Double], reBase: Array[Double],
                  raFiPlus: Array[Double], rePlus: Array[Double],
                  raFiMinus: Array[Double], reMinus: Array[Double],
                  freq: Frequency,
                  deltaAt: Int => Double,
                  refShock: Double
                ): Vector[Double] = computeRa(freq) { period =>
    (for {
      c  <- aggregate(crd, period, freq, isCrd = true)
      s  <- aggregate(raStatBase, period, freq, isCrd = false)
      fb <- aggregate(raFiBase, period, freq, isCrd = false)
      rb <- aggregate(reBase, period, freq, isCrd = false)
      fp <- aggregate(raFiPlus, period, freq, isCrd = false)
      rp <- aggregate(rePlus, period, freq, isCrd = false)
      fm <- aggregate(raFiMinus, period, freq, isCrd = false)
      rm <- aggregate(reMinus, period, freq, isCrd = false)
    } yield {
      if (c == 0.0) 0.0 // CRD==0 -> exposure run off, no further loss
      else {
        val delta = deltaAt(period)
        val (fs, rs) = if (delta < 0) (fm, rm) else (fp, rp) // leg by sign of the rate move
        val statDetail     = -s / c
        val fireBaseDetail = -(fb + rb) / c
        val fireStressDet  = -(fs + rs) / c
        val w = if (refShock == 0.0) 0.0 else math.abs(delta) / refShock
        statDetail + fireBaseDetail + w * (fireStressDet - fireBaseDetail)
      }
    }
    ).filter(_ < RUNOFF_RA_CAP) // RA >= 1 (run-off cliff) -> None -> freeze at last good value
  }

  /** Build the RA prefix: keep periods while the window is valid and term <= horizon. */
  private def computeRa(freq: Frequency)(period: Int => Option[Double]): Vector[Double] = {
    val buf = Vector.newBuilder[Double]
    var p = 1
    var continue = true
    while (continue) {
      val term = (p - 1) * freq.step
      if (term > COMPUTED_HORIZON_Y) continue = false
      else period(p) match {
        case Some(v) => buf += v; p += 1
        case None    => continue = false
      }
    }
    buf.result()
  }

  /** Cumulative product of (1 - RA), clamped to [0,1] (an exposure factor can't be <0 or >1). */
  def vectorFactored(ra: Vector[Double]): Vector[Double] = {
    var acc = 1.0
    // Emit the clamped value but keep `acc` as the true running product (the RUNOFF_RA_CAP guard
    // already keeps each (1-RA) factor in (0,1]; this clamp is a cheap backstop for odd data).
    ra.map { x => acc *= (1.0 - x); math.max(0.0, math.min(1.0, acc)) }
  }

  /**
   * Map the output term grid onto the computed vector-factored series, holding the last
   * computed value flat for every term beyond the computed horizon (incl. the tail term).
   */
  def termSeries(vf: Vector[Double], freq: Frequency): Vector[(Double, Double)] = {
    if (vf.isEmpty) Vector.empty
    else termGrid(freq).map { t =>
      val n = Math.round(t / freq.step).toInt + 1 // 1-based period for this term
      val idx = Math.min(n, vf.length) - 1
      t -> vf(idx)
    }
  }
}
