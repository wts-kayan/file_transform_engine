package com.bnp.str.tseadfwd.mapping

import com.bnp.str.tseadfwd.common.MapperProvider
import com.bnp.str.tseadfwd.mapping.PrimaryView._
import com.bnp.str.tseadfwd.utility.PrimaryConstants._
import com.bnp.str.tseadfwd.validation.{ControlCheck, DataControlView, Severity}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.slf4j.LoggerFactory

/**
 * Builds the EAD FWD Term Structure output DataFrame.
 *
 * Inputs are small reference series, so the heavy numeric work runs on the driver via
 * [[PrimaryView]] (validated against the target file); Spark is used only for IO.
 *
 * Pipeline per matrix (PARAMETRAGE group) x frequency {Q, Y} x scenario {C, A, O, E}:
 *   1. resolve the matrix definition (aggregation, combined FWL flag, macroData variable)
 *   2. aggregate the constituent RA monthly series (element-wise sum when aggregated)
 *   3. compute the RA detail (Central / FWL=NO, or scenario shock for FWL=YES)
 *   4. cumulative-product to the EAD_RA_RATE term structure (flat past term 30)
 *   5. emit (EAD_MATRIX_ID, SCENARIO_ID, TERM, EAD_RA_RATE) rows, decimal-comma formatted
 */
class PrimaryMapper(
                     raInput: DataFrame, // all RA perimeters, unioned (BCEF + any of BGL/BNL/FORTIS/LS present)
                     scenario: DataFrame,
                     parametrage: DataFrame,
                     outputTableName: String
                   )(implicit sparkSession: SparkSession, config: Config)
  extends MapperProvider {

  private val log = LoggerFactory.getLogger(this.getClass)

  private val appConf = config.getConfig(APP_CONF)
  /**
   * Engine run parameters, grouped under `tseadfwd_app.parameters`. Falls back to the app root when
   * the `parameters` block is absent (backward compatibility / minimal test configs).
   */
  private val paramsConf: Config =
    if (appConf.hasPath("parameters")) appConf.getConfig("parameters") else appConf
  /**
   * Projection start quarter = term 0 (the as-of date). The FWL=YES shock reads the scenario macro
   * path from here, one step per quarter. (Renamed from `shock_window_start`.)
   */
  private val asOfDateQuarter: String =
    if (paramsConf.hasPath("as_of_date_quarter")) paramsConf.getString("as_of_date_quarter") else "2021Q1"
  /**
   * Fallback last quarter of the projection horizon, used ONLY when a matrix's PARAMETRAGE
   * `PROJECTION_HORIZON` cell is blank/unparseable. Normally each matrix's shock-window end is
   * derived as `as_of_date_quarter + PROJECTION_HORIZON` (e.g. "3Y"). (Renamed from `shock_window_end`.)
   */
  private val lastQuarterProjectionHorizon: String =
    if (paramsConf.hasPath("last_quarter_projection_horizon")) paramsConf.getString("last_quarter_projection_horizon") else "2025Q4"

  /**
   * FWL=YES scenario shock scaling (schema interpretation toggle, default true):
   *  - true  → multiply the stress-vs-baseline shock by the macro `Rate/100` (= per-term delta),
   *            so Adverse ≠ Extreme and the magnitude follows the macro path (schema STEP 3 + 4).
   *  - false → apply the shock at full size with NO `Rate` factor (the literal STEP-4 cells); this
   *            makes Adverse = Extreme (both STRESS(-)) — kept so we can compare both readings.
   */
  private val applyRateToShock: Boolean =
    if (paramsConf.hasPath("apply_rate_to_shock")) paramsConf.getBoolean("apply_rate_to_shock") else true

  /** Quarter "yyyyQq" -> ordinal (quarters since year 0); inverse is [[qLabel]]. */
  private def qOrd(q: String): Int = { val p = q.split("Q"); p(0).trim.toInt * 4 + (p(1).trim.toInt - 1) }
  private def qLabel(o: Int): String = s"${o / 4}Q${o % 4 + 1}"

  /** Ordered list of "yyyyQq" labels from start to end inclusive. */
  private def quartersBetween(startQ: String, endQ: String): Vector[String] =
    (qOrd(startQ) to qOrd(endQ)).map(qLabel).toVector

  /**
   * Parse a PROJECTION_HORIZON cell into a number of QUARTERS to offset from the as-of date:
   * "3Y" -> 12, "12Q" -> 12, a bare number -> years. None when blank or non-numeric.
   */
  private def parseHorizonQuarters(h: String): Option[Int] = {
    val t = h.trim.toUpperCase
    if (t.isEmpty) None
    else {
      val (numPart, isYears) =
        if (t.endsWith("Y")) (t.dropRight(1), true)
        else if (t.endsWith("Q")) (t.dropRight(1), false)
        else (t, true) // bare number -> years
      try { val n = numPart.trim.toInt; Some(if (isYears) n * 4 else n) }
      catch { case _: NumberFormatException => None }
    }
  }

  /**
   * The shock-window quarters for a matrix: from `as_of_date_quarter` to the projection-horizon end
   * INCLUSIVE. The end is `as_of + PROJECTION_HORIZON` (e.g. as-of 2025Q4 + "3Y" -> 2028Q4) when the
   * PARAMETRAGE column carries a value; otherwise it falls back to the
   * `last_quarter_projection_horizon` config quarter.
   */
  private def shockWindowFor(projectionHorizon: String): Vector[String] = {
    val endQ = parseHorizonQuarters(projectionHorizon) match {
      case Some(qs) => qLabel(qOrd(asOfDateQuarter) + qs)
      case None     => lastQuarterProjectionHorizon
    }
    quartersBetween(asOfDateQuarter, endQ)
  }

  /** Union of every FWL=YES matrix's shock window (for the data-control coverage check / debug), ascending. */
  private def allShockQuarters(matrices: Seq[MatrixDef]): Vector[String] =
    matrices.filter(_.fwlApplied).flatMap(m => shockWindowFor(m.projectionHorizon)).distinct.sortBy(qOrd).toVector
  /** When true, log a titled `show()` of the inputs and a full per-term trace per matrix. */
  private val debug: Boolean =
    if (paramsConf.hasPath("debug")) paramsConf.getBoolean("debug") else false

  /** When true (default), a failed pre-calculation data control aborts the run; otherwise it only warns. */
  private val validationStrict: Boolean =
    if (paramsConf.hasPath("validation.strict")) paramsConf.getBoolean("validation.strict") else true

  /** Log a title, then show the DataFrame (used to label every debug dump). */
  private def logShow(title: String, df: DataFrame, numRows: Int = 250): Unit = {
    log.info(s"\n========== $title ==========")
    df.show(numRows, truncate = false)
  }

  // --- a single matrix to compute (one PARAMETRAGE group, before the Q/Y split) ---
  private case class MatrixDef(
                                perimeter: String,
                                outSegment: String,
                                rateType: String,
                                segments: Seq[String], // constituent RA segments (>1 when aggregated)
                                fwlApplied: Boolean,
                                macroVar: String,
                                projectionHorizon: String // e.g. "3Y"; drives the shock-window end (blank -> config fallback)
                              ) {
    def matrixId(freq: Frequency): String =
      // Join only the non-empty parts so a blank RATE_TYPE (e.g. BPLS numeric segments) yields
      // `BPLS_10276_Q`, not `BPLS_10276__Q`. With a rate type it stays `BCEF_CONSO_TF_Q`.
      (Seq(perimeter, outSegment, rateType).filter(_.nonEmpty) :+ freq.suffix).mkString("_")
  }

  def getDataFrame: DataFrame = {
    log.info(s"Building $OUTPUT_EAD_FWD (as_of=$asOfDateQuarter; shock-window end per PARAMETRAGE PROJECTION_HORIZON, fallback $lastQuarterProjectionHorizon)")

    val perimeters = raInput.select(COL_PERIMETER).distinct().collect().map(_.getString(0)).toSet
    val ra = collectRa(raInput)
    val macroData = collectScenario(scenario)
    val matrices = parseParametrage(parametrage, perimeters)

    // Technical control: validate the inputs the calculation is about to consume. Logs a
    // consolidated report, writes an auditable CSV, and (when validation.strict) aborts on a FAIL.
    runDataControl(ra, macroData, matrices)

    if (debug) {
      // Exact parsed RA keys with series length. Each key is quoted with [|] delimiters so a
      // stray space or odd character in SEGMENT / RATE_TYPE / FWL_TYPE / METRIC is visible
      // (e.g. "STRESS (+)" vs "STRESS (+) " vs "STRESS(+)"). An empty/missing key here is the
      // direct cause of an EMPTY ra_detail downstream.
      val keyLines = ra.toSeq
        .map { case ((perim, seg, rt, fwl, metric), arr) => s"[$perim|$seg|$rt|$fwl|$metric] len=${arr.length}" }
        .sorted
      log.info(s"PARSED - RA keys (${ra.size}):\n  " + keyLines.mkString("\n  "))

      logShow("INPUT - PARAMETRAGE", parametrage)
      logShow("INPUT - MACRO_VARIABLE (scenario, union of shock windows)",
        scenario.where(s"$COL_SCEN_DATE IN (${allShockQuarters(matrices).map(q => s"'$q'").mkString(",")})"))
      // M1..Mn are MONTHLY columns (METRIC is the separate key column). The full series is
      // used by collectRa; here we only preview the first/last 3 months to keep the table readable.
      val allMonths = monthColumns(raInput)
      val sampleMonths = allMonths.take(3) ++ allMonths.takeRight(3)
      val raCols = Seq(COL_PERIMETER, COL_SEGMENT, COL_RATE_TYPE, COL_FWL_TYPE, COL_METRIC) ++ sampleMonths
      logShow(s"INPUT - RA all perimeters (keys + first/last months; ${allMonths.length} monthly cols used in full)",
        raInput.select(raCols.head, raCols.tail: _*))
      val matRows = matrices.map(m => Row(m.matrixId(Quarterly).dropRight(2), m.segments.mkString("+"), m.fwlApplied.toString, m.macroVar))
      val matSchema = StructType(Seq("MATRIX", "SEGMENTS", "FWL_APPLIED", "MACRO_VAR").map(StructField(_, StringType)))
      logShow("PARSED - matrix definitions", sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(matRows, 1), matSchema))
    }

    val rows: Seq[Row] = for {
      m    <- matrices
      freq <- Seq(Quarterly, Yearly)
      (scenName, scenCode) <- SCENARIO_CODES
      row  <- matrixRows(m, freq, scenName, scenCode, ra, macroData)
    } yield row

    val schema = StructType(Seq(
      StructField(OUT_MATRIX_ID, StringType, nullable = false),
      StructField(OUT_SCENARIO_ID, StringType, nullable = false),
      StructField(OUT_TERM, StringType, nullable = false),
      StructField(OUT_EAD_RA_RATE, StringType, nullable = false)
    ))
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rows, 1), schema)
  }

  /**
   * Per-(matrix, scenario, term) computation breakdown for the analysis generator job
   * ([[com.bnp.str.tseadfwd.job.Term0AnalysisDriver]]). Reuses the SAME validated input parsing
   * (`collectRa` / `collectScenario` / `parseParametrage`) and [[PrimaryView]] formula core as
   * [[getDataFrame]], so every value matches the production output at the requested term by
   * construction.
   *
   * @param terms the output terms to break down (e.g. `Seq(0.0, 0.25, 1.0, 5.0)`); empty -> just
   *              term 0. Each term is snapped to the frequency grid; a term past the computed
   *              horizon takes the held-flat `ead` (its `ra`/`vector` are then `NaN`, since no new
   *              period is computed there — exactly the run-off freeze of [[PrimaryView.termSeries]]).
   * Default frequency is Quarterly (term 0 = Q1). A matrix/scenario whose first-period aggregation
   * window is empty (no RA data, or a missing FWL=YES stress leg) is dropped — as `matrixRows` does.
   */
  def term0AnalysisRows(terms: Seq[Double], freq: Frequency = Quarterly): Seq[Term0RowView] = {
    val perimeters = raInput.select(COL_PERIMETER).distinct().collect().map(_.getString(0)).toSet
    val ra = collectRa(raInput)
    val macroData = collectScenario(scenario)
    val matrices = parseParametrage(parametrage, perimeters)

    val wanted = (if (terms.isEmpty) Seq(0.0) else terms).map(snap).distinct.sorted

    for {
      m                    <- matrices
      (scenName, scenCode) <- SCENARIO_CODES
      row                  <- termRowsFor(m, freq, scenName, scenCode, ra, macroData, wanted)
    } yield row
  }

  /** Round a term to a clean grid value (kills binary FP noise like 0.30000000004). */
  private def snap(t: Double): Double = Math.round(t * 1e6) / 1e6

  /** Build the per-term breakdown rows for one matrix / scenario (empty if there is nothing to compute). */
  private def termRowsFor(
                           m: MatrixDef, freq: Frequency, scenName: String, scenCode: String,
                           ra: Map[(String, String, String, String, String), Array[Double]],
                           macroData: Map[(String, String), Map[String, Double]],
                           terms: Seq[Double]
                         ): Seq[Term0RowView] = {
    def series(fwl: String, metric: String): Array[Double] =
      aggregateSegments(ra, m.perimeter, m.segments, m.rateType, fwl, metric)

    val crd    = series(FWL_BASELINE, METRIC_CRD)
    val raStat = series(FWL_BASELINE, METRIC_RA_STAT)
    val raFiB  = series(FWL_BASELINE, METRIC_RA_FI)
    val reB    = series(FWL_BASELINE, METRIC_RE)
    if (crd.isEmpty) return Seq.empty

    val usesShock = m.fwlApplied && scenName != SCENARIO_CENTRAL
    // Stress leg fixed by scenario (Optimistic -> STRESS(+); Adverse/Extreme -> STRESS(-)), as in matrixRows.
    val (crdLeg, fiLeg, reLeg) =
      if (scenName == SCENARIO_OPTIMISTIC)
        (series(FWL_STRESS_PLUS, METRIC_CRD), series(FWL_STRESS_PLUS, METRIC_RA_FI), series(FWL_STRESS_PLUS, METRIC_RE))
      else
        (series(FWL_STRESS_MINUS, METRIC_CRD), series(FWL_STRESS_MINUS, METRIC_RA_FI), series(FWL_STRESS_MINUS, METRIC_RE))

    val raDetail: Vector[Double] =
      if (!usesShock) {
        if (m.fwlApplied) centralRa(crd, raStat, raFiB, reB, freq) else statOnlyRa(crd, raStat, freq)
      } else {
        val mult: Int => Double =
          if (applyRateToShock) deltaPath(macroData, scenName, m.macroVar, freq, shockWindowFor(m.projectionHorizon)) else (_ => 1.0)
        scenarioRa(crd, raStat, raFiB, reB, crdLeg, fiLeg, reLeg, freq, mult)
      }
    if (raDetail.isEmpty) return Seq.empty

    val vf = vectorFactored(raDetail)
    // Macro delta path (raw Rate/100 per term); only meaningful for an FWL=YES non-Central scenario.
    val deltaFn = deltaPath(macroData, scenName, m.macroVar, freq, shockWindowFor(m.projectionHorizon))
    def agg(a: Array[Double], p: Int, isCrd: Boolean): Double =
      PrimaryView.aggregate(a, p, freq, isCrd).getOrElse(Double.NaN)

    terms.map { term =>
      // 1-based projection period for this term; ead holds flat past the computed horizon (termSeries).
      val period = Math.round(term / freq.step).toInt + 1
      val eadIdx = Math.min(period, vf.length) - 1
      val ead    = if (vf.isEmpty) Double.NaN else vf(Math.max(0, eadIdx))
      // RA / VECTOR exist only where a period was actually computed (<= raDetail length); else NaN.
      val ra0    = if (period >= 1 && period <= raDetail.length) raDetail(period - 1) else Double.NaN
      val vec0   = if (ra0.isNaN) Double.NaN else 1.0 - ra0
      val delta  = if (usesShock) deltaFn(period) else 0.0

      Term0RowView(
        matrixId = m.matrixId(freq), scenarioName = scenName, scenarioCode = scenCode,
        fwlApplied = m.fwlApplied, macroVar = m.macroVar, usesShock = usesShock,
        segments = m.segments, rateType = m.rateType,
        term = snap(term), period = period,
        crdMonths = crd.take(3).toSeq, statMonths = raStat.take(3).toSeq,
        fiMonths = raFiB.take(3).toSeq, reMonths = reB.take(3).toSeq,
        crdAgg = agg(crd, period, isCrd = true), statAgg = agg(raStat, period, isCrd = false),
        fiAgg = agg(raFiB, period, isCrd = false), reAgg = agg(reB, period, isCrd = false),
        legCrdAgg = if (usesShock) agg(crdLeg, period, isCrd = true) else Double.NaN,
        legFiAgg  = if (usesShock) agg(fiLeg, period, isCrd = false) else Double.NaN,
        legReAgg  = if (usesShock) agg(reLeg, period, isCrd = false) else Double.NaN,
        delta = delta, ra = ra0, vector = vec0, ead = ead
      )
    }
  }

  /** Compute the term-structure rows for one matrix / frequency / scenario. */
  private def matrixRows(
                          m: MatrixDef,
                          freq: Frequency,
                          scenName: String,
                          scenCode: String,
                          ra: Map[(String, String, String, String, String), Array[Double]],
                          macroData: Map[(String, String), Map[String, Double]]
                        ): Seq[Row] = {
    def series(fwl: String, metric: String): Array[Double] =
      aggregateSegments(ra, m.perimeter, m.segments, m.rateType, fwl, metric)

    val crd    = series(FWL_BASELINE, METRIC_CRD)
    val raStat = series(FWL_BASELINE, METRIC_RA_STAT)
    val raFiB  = series(FWL_BASELINE, METRIC_RA_FI)
    val reB    = series(FWL_BASELINE, METRIC_RE)

    if (crd.isEmpty) {
      log.warn(s"No RA data for matrix ${m.matrixId(freq)} (segments=${m.segments.mkString(",")}); skipped")
      return Seq.empty
    }

    val usesShock = m.fwlApplied && scenName != SCENARIO_CENTRAL

    // Stress legs (only consumed for FWL=YES non-central scenarios). Named here so a debug
    // run can report each leg's length — an empty leg makes scenarioRa's period-1 aggregate
    // return None, which yields an EMPTY ra_detail (-> empty trace / no output rows).
    val raFiPlus  = series(FWL_STRESS_PLUS, METRIC_RA_FI)
    val rePlus    = series(FWL_STRESS_PLUS, METRIC_RE)
    val crdPlus   = series(FWL_STRESS_PLUS, METRIC_CRD)
    val raFiMinus = series(FWL_STRESS_MINUS, METRIC_RA_FI)
    val reMinus   = series(FWL_STRESS_MINUS, METRIC_RE)
    val crdMinus  = series(FWL_STRESS_MINUS, METRIC_CRD)

    if (debug) {
      def stat(name: String, a: Array[Double]): String =
        s"$name=${a.length}" + (if (a.nonEmpty) s"(head=${a.head})" else "<EMPTY>")
      log.info(s"SERIES ${m.matrixId(freq)} / $scenCode  fwl=${m.fwlApplied} usesShock=$usesShock  " +
        Seq(stat("CRD", crd), stat("RA_STAT", raStat), stat("RA_FI_base", raFiB), stat("RE_base", reB),
          stat("RA_FI+", raFiPlus), stat("RE+", rePlus), stat("RA_FI-", raFiMinus), stat("RE-", reMinus)
        ).mkString("  "))
    }

    val ra_detail: Vector[Double] =
      if (!usesShock) {
        // FWL=YES Central -> STAT+FI+RE; FWL=NO -> RA_STAT only (per the business schema).
        if (m.fwlApplied) centralRa(crd, raStat, raFiB, reB, freq)
        else statOnlyRa(crd, raStat, freq)
      } else {
        // FWL=YES non-Central: the stress leg is fixed by scenario (Optimistic -> STRESS(+);
        // Adverse/Extreme -> STRESS(-)); the per-term macro delta (Rate/100) scales the
        // stress-vs-baseline shock on FI+RE.
        val (crdLeg, fiLeg, reLeg) =
          if (scenName == SCENARIO_OPTIMISTIC) (crdPlus, raFiPlus, rePlus)
          else (crdMinus, raFiMinus, reMinus)
        // Shock multiplier: Rate/100 (per-term macro delta) when applyRateToShock, else 1.0 (full
        // shock, literal STEP-4 reading). See OPEN_QUESTIONS Q33.
        val mult: Int => Double =
          if (applyRateToShock) deltaPath(macroData, scenName, m.macroVar, freq, shockWindowFor(m.projectionHorizon)) else (_ => 1.0)
        scenarioRa(crd, raStat, raFiB, reB, crdLeg, fiLeg, reLeg, freq, mult)
      }

    if (ra_detail.isEmpty) {
      // period-1 aggregation failed: some input series is empty or too short to fill the
      // first window. The SERIES log above (debug=true) shows which one. Most often a missing
      // stress leg for an FWL=YES matrix, or a key mismatch (segment/rate-type/FWL spelling).
      log.warn(s"EMPTY ra_detail for ${m.matrixId(freq)} / $scenCode " +
        s"(usesShock=$usesShock); no rows emitted. Lengths: " +
        s"crd=${crd.length} raStat=${raStat.length} raFiB=${raFiB.length} reB=${reB.length} " +
        s"raFi+=${raFiPlus.length} re+=${rePlus.length} raFi-=${raFiMinus.length} re-=${reMinus.length}")
      return Seq.empty
    }

    val vf = vectorFactored(ra_detail)

    // Debug: show every intermediate calc per term. Skip the redundant A/O/E dumps for
    // FWL=NO matrices (all scenarios equal Central there).
    if (debug && (scenName == SCENARIO_CENTRAL || m.fwlApplied)) {
      val deltaInfo =
        if (m.fwlApplied && scenName != SCENARIO_CENTRAL) {
          val arr = macroDeltaArray(macroData, scenName, m.macroVar, shockWindowFor(m.projectionHorizon))
          if (arr.isEmpty) "0" else f"path ${arr.head}%.4f..${arr.last}%.4f"
        } else "0"
      logShow(s"TRACE - ${m.matrixId(freq)} / $scenCode  (delta=$deltaInfo)",
        buildTrace(freq, crd, raStat, raFiB, reB, ra_detail, vf))
    }

    termSeries(vf, freq).map { case (term, value) =>
      Row(m.matrixId(freq), scenCode, fmtNumber(term, 2), fmtNumber(value, 9))
    }
  }

  /** Per-period intermediate-calc table: aggregated metrics, RA detail, vector, factored value. */
  private def buildTrace(
                          freq: Frequency,
                          crd: Array[Double], raStat: Array[Double], raFi: Array[Double], re: Array[Double],
                          ra: Vector[Double], vf: Vector[Double]
                        ): DataFrame = {
    def agg(a: Array[Double], p: Int, isCrd: Boolean): Double =
      PrimaryView.aggregate(a, p, freq, isCrd).getOrElse(Double.NaN)
    val rows = ra.indices.map { i =>
      val p = i + 1
      Row(p, (p - 1) * freq.step,
        agg(crd, p, isCrd = true), agg(raStat, p, isCrd = false),
        agg(raFi, p, isCrd = false), agg(re, p, isCrd = false),
        ra(i), 1.0 - ra(i), vf(i))
    }
    val schema = StructType(Seq(
      StructField("period", IntegerType), StructField("term", DoubleType),
      StructField("CRD", DoubleType), StructField("RA_STAT", DoubleType),
      StructField("RA_FI", DoubleType), StructField("RE", DoubleType),
      StructField("RA", DoubleType), StructField("VECTOR", DoubleType),
      StructField("EAD_RA_RATE", DoubleType)
    ))
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rows, 1), schema)
  }

  /** Signed macro delta (scenario - Central) for `macroVar` at each quarter of the given shock window. */
  private def macroDeltaArray(
                               macroData: Map[(String, String), Map[String, Double]],
                               scenName: String, macroVar: String, window: Vector[String]
                             ): Array[Double] =
    window.map { q =>
      def v(scen: String): Double = macroData.get((scen, q)).flatMap(_.get(macroVar)).getOrElse(0.0)
      v(scenName) - v(SCENARIO_CENTRAL)
    }.toArray

  /**
   * Maps a 1-based projection period to its macro delta on the shock-window path:
   * term 0 = window start, step 1 quarter (quarterly) or 1 year = 4 quarters (yearly);
   * past the window end (the projection horizon) the last delta is held — i.e. the shock STOPS
   * advancing at the horizon. `window` is the matrix's [[shockWindowFor]] result.
   */
  private def deltaPath(
                         macroData: Map[(String, String), Map[String, Double]],
                         scenName: String, macroVar: String, freq: Frequency, window: Vector[String]
                       ): Int => Double = {
    val arr = macroDeltaArray(macroData, scenName, macroVar, window)
    val step = if (freq == Quarterly) 1 else 4
    (period: Int) => if (arr.isEmpty) 0.0 else arr(math.min((period - 1) * step, arr.length - 1))
  }

  // ---- input collection -----------------------------------------------------

  private def monthColumns(df: DataFrame): Seq[String] =
    df.columns.filter(c => c.length > 1 && c.charAt(0) == 'M' && c.drop(1).forall(_.isDigit))
      .sortBy(_.drop(1).toInt)

  /**
   * key = (PERIMETER, SEGMENT, RATE_TYPE, FWL_TYPE, METRIC) -> monthly series (M1..Mn).
   *
   * PERIMETER is part of the key: the RA input unions several entities (BCEF/BGL/BNL/…) and the
   * same SEGMENT name (e.g. MORTGAGE) recurs across them, so omitting PERIMETER would collide those
   * rows and `.toMap` would keep only the LAST one — making every perimeter's MORTGAGE matrix use a
   * single (wrong) entity's data. `aggregateSegments` looks up with the matrix's perimeter to match.
   */
  private def collectRa(df: DataFrame): Map[(String, String, String, String, String), Array[Double]] = {
    val months = monthColumns(df)
    val cols = Seq(COL_PERIMETER, COL_SEGMENT, COL_RATE_TYPE, COL_FWL_TYPE, COL_METRIC) ++ months
    df.select(cols.head, cols.tail: _*).collect().map { r =>
      // Canonicalize key fields (null-safe): Excel cells frequently carry leading/trailing or
      // embedded odd whitespace (incl. the NBSP/narrow no-break space POI emits on French-locale
      // hosts), which would make the lookup key (e.g. "STRESS (+) ") miss the constant
      // ("STRESS (+)") and yield an empty series -> empty ra_detail -> empty non-Central scenarios.
      // canon() normalizes both this map's keys and the aggregateSegments lookup tuple identically.
      def key0(i: Int): String = canon(Option(r.get(i)).map(_.toString).getOrElse(""))
      val key = (key0(0), key0(1), key0(2), key0(3), key0(4))
      // Schema preamble (r1): forward-fill a short series flat to M361 with its last month's value.
      val series = padForward(months.indices.map(i => toDouble(r.get(5 + i))).toArray)
      key -> series
    }.toMap
  }

  /**
   * Technical control run on the parsed inputs BEFORE the calculation. Builds a list of
   * PASS/WARN/FAIL checks (label vocabulary, French-number integrity, stress legs, scenario
   * coverage, cross-consistency), logs a consolidated report, writes an auditable CSV next to
   * the output, and — when `validation.strict` (default true) — aborts the run on any FAIL.
   * Always on (independent of the debug flag).
   */
  private def runDataControl(
                              ra: Map[(String, String, String, String, String), Array[Double]],
                              macroData: Map[(String, String), Map[String, Double]],
                              matrices: Seq[MatrixDef]
                            ): Unit = {
    val checks = buildControlChecks(ra, macroData, matrices)

    val report = DataControlView.renderReport(checks)
    if (checks.exists(_.failed)) log.error("\n" + report)
    else if (checks.exists(_.warned)) log.warn("\n" + report)
    else log.info("\n" + report)

    try {
      val outCfg = appConf.getConfig(OUTPUT_EAD_FWD)
      val dir = if (outCfg.hasPath("tmpPath")) outCfg.getString("tmpPath") else "."
      val tbl = if (outCfg.hasPath("tableName")) outCfg.getString("tableName") else OUTPUT_EAD_FWD
      val path = DataControlView.writeCsv(dir, s"DATA_CONTROL_$tbl.csv", checks)
      log.info(s"DATA CONTROL report written to $path")
    } catch {
      case e: Throwable => log.warn(s"Could not write DATA CONTROL report file: ${e.getMessage}")
    }

    val failures = checks.filter(_.failed)
    if (failures.nonEmpty && validationStrict)
      throw new IllegalStateException(
        s"Pre-calculation data control FAILED (${failures.size} check(s)); aborting before calculation. " +
          s"Set $APP_CONF.parameters.validation.strict=false to override.\n" +
          failures.map(c => s"  - ${c.name}: ${c.detail}").mkString("\n"))
  }

  /** All technical-control checks on the parsed inputs (keys already canon()ed). */
  private def buildControlChecks(
                                  ra: Map[(String, String, String, String, String), Array[Double]],
                                  macroData: Map[(String, String), Map[String, Double]],
                                  matrices: Seq[MatrixDef]
                                ): Seq[ControlCheck] = {
    val checks = scala.collection.mutable.ListBuffer.empty[ControlCheck]
    def add(name: String, sev: String, detail: String): Unit = checks += ControlCheck(name, sev, detail)

    // --- structural: required columns + monthly grid ---
    val raCols = raInput.columns.toSet
    val requiredRaCols = Seq(COL_PERIMETER, COL_SEGMENT, COL_RATE_TYPE, COL_FWL_TYPE, COL_METRIC)
    val missingCols = requiredRaCols.filterNot(raCols.contains)
    if (missingCols.isEmpty) add("RA.columns", Severity.Pass, s"key columns present: ${requiredRaCols.mkString(", ")}")
    else add("RA.columns", Severity.Fail, s"missing RA key column(s): ${missingCols.mkString(", ")}")

    val months = monthColumns(raInput)
    if (months.isEmpty) add("RA.months", Severity.Fail, "no monthly M<n> columns detected in RA input")
    else {
      val nums = months.map(_.drop(1).toInt)
      val gaps = (nums.min to nums.max).filterNot(nums.toSet)
      if (gaps.isEmpty) add("RA.months", Severity.Pass, s"${months.length} contiguous monthly columns M${nums.min}..M${nums.max}")
      else add("RA.months", Severity.Warn, s"${months.length} monthly columns with gaps at: ${gaps.take(10).map("M" + _).mkString(", ")}")
    }

    if (ra.isEmpty) add("RA.rows", Severity.Fail, "RA input parsed to zero series")
    else add("RA.rows", Severity.Pass, s"${ra.size} RA series parsed")

    // --- label vocabulary (canonicalized) ---
    val fwlPresent = ra.keySet.map(_._4)
    val metricPresent = ra.keySet.map(_._5)
    val missingFwl = Seq(FWL_BASELINE, FWL_STRESS_PLUS, FWL_STRESS_MINUS).filterNot(f => fwlPresent.contains(canon(f)))
    val missingMetric = Seq(METRIC_CRD, METRIC_RA_STAT, METRIC_RA_FI, METRIC_RE).filterNot(m => metricPresent.contains(canon(m)))
    if (missingFwl.isEmpty && missingMetric.isEmpty)
      add("RA.labels", Severity.Pass, s"FWL_TYPE=${fwlPresent.toSeq.sorted.mkString("{", ", ", "}")} METRIC=${metricPresent.toSeq.sorted.mkString("{", ", ", "}")}")
    else
      add("RA.labels", Severity.Fail, s"missing FWL_TYPE=${missingFwl.mkString("[", ", ", "]")} METRIC=${missingMetric.mkString("[", ", ", "]")} (check spelling / non-breaking spaces vs the constants)")

    // --- numeric integrity: every non-empty raw cell must parse to a number ---
    // tryDouble is locale-tolerant (strips space/NBSP/narrow-NBSP grouping, comma -> dot), so
    // French-formatted cells are fine; this flags only genuinely non-numeric content.
    val (scanned, badCells, sample) = scanRawNumericCells(months)
    if (badCells == 0) add("RA.numeric", Severity.Pass, s"$scanned numeric cells scanned; all parse to a number")
    else add("RA.numeric", Severity.Fail, s"$badCells/$scanned numeric cell(s) do not parse to a number, e.g. ${sample.mkString(", ")}")

    val nonFinite = ra.count { case (_, arr) => arr.exists(d => d.isNaN || d.isInfinite) }
    if (nonFinite == 0) add("RA.finite", Severity.Pass, "all parsed RA values are finite")
    else add("RA.finite", Severity.Warn, s"$nonFinite RA series contain NaN/Infinite values")

    // --- per-matrix stress legs for FWL=YES ---
    val legIssues = for {
      m <- matrices if m.fwlApplied
      plus = m.segments.exists(s => ra.contains((canon(m.perimeter), canon(s), canon(m.rateType), canon(FWL_STRESS_PLUS), canon(METRIC_RA_FI))))
      minus = m.segments.exists(s => ra.contains((canon(m.perimeter), canon(s), canon(m.rateType), canon(FWL_STRESS_MINUS), canon(METRIC_RA_FI))))
      if !plus || !minus
    } yield s"${m.outSegment}/${m.rateType}(+=$plus,-=$minus)"
    if (matrices.exists(_.fwlApplied)) {
      if (legIssues.isEmpty) add("RA.stressLegs", Severity.Pass, "every FWL=YES matrix has both stress legs")
      else add("RA.stressLegs", Severity.Fail, s"FWL=YES matrix(es) missing a stress leg: ${legIssues.mkString("; ")} -> their non-Central scenarios would be EMPTY")
    }

    // --- PARAMETRAGE -> matrices + segment cross-consistency ---
    if (matrices.isEmpty) add("PARAMETRAGE.matrices", Severity.Fail, "no matrices resolved from PARAMETRAGE (no perimeter overlap with RA?)")
    else add("PARAMETRAGE.matrices", Severity.Pass, s"${matrices.size} matrices resolved")

    val orphanSegs = matrices.flatMap(m =>
      m.segments.filterNot(s => ra.contains((canon(m.perimeter), canon(s), canon(m.rateType), canon(FWL_BASELINE), canon(METRIC_CRD))))).distinct
    if (orphanSegs.isEmpty) add("PARAMETRAGE.segments", Severity.Pass, "all referenced segments have BASELINE/CRD in RA")
    else add("PARAMETRAGE.segments", Severity.Warn, s"segment(s) referenced by PARAMETRAGE but absent from RA BASELINE/CRD: ${orphanSegs.mkString(", ")}")

    // --- scenario coverage ---
    val scenNames = macroData.keySet.map(_._1)
    val missingScen = SCENARIO_CODES.map(_._1).filterNot(scenNames.contains)
    if (missingScen.isEmpty) add("SCENARIO.names", Severity.Pass, s"scenarios present: ${scenNames.toSeq.sorted.mkString(", ")}")
    else if (missingScen.contains(SCENARIO_CENTRAL)) add("SCENARIO.names", Severity.Fail, s"Central scenario missing (required); also missing: ${missingScen.filterNot(_ == SCENARIO_CENTRAL).mkString(", ")}")
    else add("SCENARIO.names", Severity.Warn, s"scenario(s) missing (their output is skipped): ${missingScen.mkString(", ")}")

    val allMacroVars = macroData.values.flatMap(_.keys).toSet
    val missingVars = matrices.filter(_.fwlApplied).map(_.macroVar).filter(_.nonEmpty).distinct.filterNot(allMacroVars.contains)
    if (missingVars.isEmpty) add("SCENARIO.macroVars", Severity.Pass, "all referenced MACRO_VARIABLEs present in scenario data")
    else add("SCENARIO.macroVars", Severity.Fail, s"MACRO_VARIABLE(s) referenced by PARAMETRAGE but absent from scenario data: ${missingVars.mkString(", ")} (FWL shock would be 0)")

    val centralDates = macroData.keySet.collect { case (s, d) if s == SCENARIO_CENTRAL => d }
    // Coverage is checked over the UNION of every FWL=YES matrix's shock window (as_of -> per-matrix
    // PROJECTION_HORIZON end, or the config fallback).
    val shockQuarters = allShockQuarters(matrices)
    val missingQ = shockQuarters.filterNot(centralDates.contains)
    if (shockQuarters.isEmpty) add("SCENARIO.window", Severity.Pass, "no FWL=YES matrices -> no shock window to check")
    else if (centralDates.isEmpty) add("SCENARIO.window", Severity.Warn, "no Central scenario dates found to check the shock window against")
    else if (missingQ.isEmpty) add("SCENARIO.window", Severity.Pass, s"all ${shockQuarters.size} shock-window quarters present (${shockQuarters.head}..${shockQuarters.last})")
    else add("SCENARIO.window", Severity.Warn, s"${missingQ.size} shock-window quarter(s) missing (delta holds last available): ${missingQ.take(8).mkString(", ")}")

    checks.toList
  }

  /**
   * Scan the raw (string) monthly cells and check each parses via `tryDouble` (locale-tolerant).
   * Returns (non-empty cells scanned, cells that fail to parse, up to 8 located samples of the
   * form `row <n> [SEGMENT|RATE_TYPE|FWL_TYPE|METRIC] <Mcol>='<raw>'`).
   */
  private def scanRawNumericCells(months: Seq[String]): (Int, Int, Seq[String]) = {
    if (months.isEmpty) (0, 0, Nil)
    else {
      val keyCols = Seq(COL_SEGMENT, COL_RATE_TYPE, COL_FWL_TYPE, COL_METRIC)
      val sel = keyCols ++ months
      val rows = raInput.select(sel.head, sel.tail: _*).collect()
      val k = keyCols.length
      var scanned = 0
      var bad = 0
      val examples = scala.collection.mutable.ListBuffer.empty[String]
      var rowIdx = 0
      rows.foreach { r =>
        rowIdx += 1 // 1-based RA data row (Excel row = rowIdx + 1 for the header)
        var i = 0
        while (i < months.length) {
          val v = r.get(k + i)
          if (v != null) {
            val s = v.toString.trim
            if (s.nonEmpty) {
              scanned += 1
              if (tryDouble(s).isEmpty) {
                bad += 1
                if (examples.size < 8) {
                  val key = (0 until k).map(j => Option(r.get(j)).map(_.toString).getOrElse("")).mkString("|")
                  examples += s"row $rowIdx [$key] ${months(i)}='$s'"
                }
              }
            }
          }
          i += 1
        }
      }
      (scanned, bad, examples.toList)
    }
  }

  /** Element-wise sum of the monthly series across constituent segments (same perimeter + rate type). */
  private def aggregateSegments(
                                 ra: Map[(String, String, String, String, String), Array[Double]],
                                 perimeter: String, segments: Seq[String], rateType: String, fwl: String, metric: String
                               ): Array[Double] = {
    // canon() the lookup tuple to match collectRa's canonicalized keys (whitespace/NBSP/case).
    val present = segments.flatMap(s => ra.get((canon(perimeter), canon(s), canon(rateType), canon(fwl), canon(metric))))
    if (present.isEmpty) Array.empty
    else {
      val len = present.map(_.length).max
      val acc = new Array[Double](len)
      present.foreach(a => a.indices.foreach(i => acc(i) += a(i)))
      acc
    }
  }

  /** key = (scenario, Date) -> (macroVarName -> value). */
  private def collectScenario(df: DataFrame): Map[(String, String), Map[String, Double]] = {
    val macroCols = df.columns.filterNot(c => c == COL_SCEN_DATE || c == COL_SCEN_NAME)
    df.collect().map { r =>
      val date = r.getAs[String](COL_SCEN_DATE)
      val scen = r.getAs[String](COL_SCEN_NAME)
      val vals = macroCols.map(c => c -> toDouble(r.getAs[Any](c))).toMap
      (scen, date) -> vals
    }.toMap
  }

  // ---- PARAMETRAGE parsing --------------------------------------------------

  private def parseParametrage(df: DataFrame, perimeters: Set[String]): Seq[MatrixDef] = {
    val cols = Seq(COL_PERIMETER, COL_SEGMENT, COL_RATE_TYPE, COL_AGGREGATION,
      COL_AGG_SEGMENT_NAME, COL_FWL_TO_BE_APPLIED, COL_MACRO_VARIABLE, COL_PROJECTION_HORIZON)
    val recs = df.select(cols.head, cols.tail: _*).collect()
      .map { r =>
        (str(r, 0), str(r, 1), str(r, 2), str(r, 3), str(r, 4), str(r, 5), str(r, 6), str(r, 7))
      }
      .filter { case (perim, seg, _, _, _, _, _, _) => perimeters.contains(perim) && seg.nonEmpty }

    // group by (perimeter, output segment, rate type) so INVEST_PRO + INVEST_CORP collapse
    // into INVEST while distinct rate types (TF/TV) stay separate matrices.
    recs.groupBy { case (perim, seg, rateType, agg, aggName, _, _, _) =>
      val out = if (agg.equalsIgnoreCase(YES) && aggName.nonEmpty) aggName else seg
      (perim, out, rateType)
    }.map { case ((perim, out, rateType), group) =>
      val segments = group.map(_._2).distinct.toSeq
      // combined FWL flag for an aggregated matrix: YES if ANY constituent applies FWL.
      val fwlApplied = group.exists(_._6.equalsIgnoreCase(YES))
      val macroVar = group.map(_._7).find(v => v.nonEmpty && !v.equalsIgnoreCase("NONE")).getOrElse("")
      // per-matrix projection horizon (first non-empty in the group); blank -> config fallback later.
      val projectionHorizon = group.map(_._8).find(_.nonEmpty).getOrElse("")
      MatrixDef(perim, out, rateType, segments, fwlApplied, macroVar, projectionHorizon)
    }.toSeq.sortBy(m => (m.perimeter, m.outSegment, m.rateType))
  }

  // ---- helpers --------------------------------------------------------------

  private def str(r: Row, i: Int): String = Option(r.get(i)).map(_.toString.trim).getOrElse("")

  /**
   * Canonicalize a key field used to match RA rows so that cosmetic label differences between
   * environments never break the (SEGMENT, RATE_TYPE, FWL_TYPE, METRIC) lookup. Null-safe;
   * uppercases (Locale.ROOT) then strips EVERY character that is not a letter, digit, or sign,
   * which removes: regular spaces, the NBSP (U+00A0) / narrow no-break space (U+202F) POI emits
   * on French-locale hosts, underscores, and parentheses — while keeping the `+`/`-` that
   * distinguish the stress legs. So all of these collapse to one token and match:
   *   "RA STAT" == "RA_STAT" == "ra stat"            -> "RASTAT"
   *   "RA FI"   == "RA_FI"                            -> "RAFI"
   *   "STRESS (+)" == "STRESS(+)" == "STRESS (+)" -> "STRESS+"
   *   "STRESS (-)"                                    -> "STRESS-"
   * This is why a French-locale BNP host (NBSP inside "STRESS (+)") or an underscore METRIC
   * spelling ("RA_FI") made the stress legs miss -> empty ra_detail -> EMPTY non-Central
   * scenarios, while single-token BASELINE/Central still matched. Applied to BOTH the collectRa
   * map keys and the aggregateSegments lookup tuple so they normalize identically.
   */
  private def canon(s: String): String =
    Option(s).getOrElse("")
      .toUpperCase(java.util.Locale.ROOT)
      .replaceAll("[^A-Z0-9+-]", "")

  /**
   * Locale-tolerant, format-agnostic numeric parse. Returns None for empty / non-numeric input.
   * spark-excel returns cell values as strings whose thousands/decimal punctuation varies by the
   * host locale and the source file:
   *   - French:  "-92 924,788279"  (space/NBSP grouping, COMMA decimal)
   *   - US:      "-92,924.788279"  (COMMA grouping, DOT decimal)
   *   - canonical (usePlainNumberFormat): "-92924.788279" (no grouping, DOT decimal)
   * We strip every horizontal-whitespace grouping (`\h` = space / U+00A0 / U+202F), then resolve the
   * decimal mark: when BOTH ',' and '.' are present the LAST one is the decimal and the other is
   * grouping; a lone ',' is a decimal comma; a lone '.' is already canonical.
   */
  private def tryDouble(v: Any): Option[Double] = v match {
    case null                => None
    case d: Double           => Some(d)
    case n: java.lang.Number => Some(n.doubleValue())
    case other               =>
      val t = other.toString.trim.replaceAll("\\h", "")
      if (t.isEmpty) None
      else if (t == "-" || t == "–" || t == "—") Some(0.0) // accounting / French nil -> zero
      else {
        val lc = t.lastIndexOf(','); val ld = t.lastIndexOf('.')
        val norm =
          if (lc >= 0 && ld >= 0) { if (lc > ld) t.replace(".", "").replace(',', '.') else t.replace(",", "") }
          else if (lc >= 0) t.replace(',', '.')
          else t
        try Some(norm.toDouble) catch { case _: NumberFormatException => None }
      }
  }

  private def toDouble(v: Any): Double = tryDouble(v).getOrElse(0.0)

  /** Decimal-comma formatting, half-up at `maxScale`, trailing zeros stripped (e.g. 0.5 -> "0,5"). */
  private def fmtNumber(value: Double, maxScale: Int): String = {
    if (value.isNaN || value.isInfinite) return "0"
    val bd = BigDecimal(value)
      .setScale(maxScale, BigDecimal.RoundingMode.HALF_UP)
      .bigDecimal.stripTrailingZeros()
    val plain = if (bd.scale() < 0) bd.setScale(0).toPlainString else bd.toPlainString
    plain.replace(".", ",")
  }
}
