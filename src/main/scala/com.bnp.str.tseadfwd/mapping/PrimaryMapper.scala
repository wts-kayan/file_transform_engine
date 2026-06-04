package com.bnp.str.tseadfwd.mapping

import com.bnp.str.tseadfwd.common.MapperProvider
import com.bnp.str.tseadfwd.mapping.PrimaryView._
import com.bnp.str.tseadfwd.utility.PrimaryConstants._
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
  /** FWL=YES shock reads the scenario macro path over this window (term 0 = start, step 1Q). */
  private val shockWindowStart: String =
    if (appConf.hasPath("shock_window_start")) appConf.getString("shock_window_start") else "2021Q1"
  private val shockWindowEnd: String =
    if (appConf.hasPath("shock_window_end")) appConf.getString("shock_window_end") else "2025Q4"
  /** Stress reference magnitude used to scale the macroData rate delta (FWL=YES calibration). */
  private val refShock: Double =
    if (appConf.hasPath("ref_shock")) appConf.getDouble("ref_shock") else 1.0

  /** Ordered list of "yyyyQq" labels from start to end inclusive. */
  private lazy val shockWindow: Vector[String] = {
    def ord(q: String): Int = { val p = q.split("Q"); p(0).toInt * 4 + (p(1).toInt - 1) }
    (ord(shockWindowStart) to ord(shockWindowEnd)).map { o => s"${o / 4}Q${o % 4 + 1}" }.toVector
  }
  /** When true, log a titled `show()` of the inputs and a full per-term trace per matrix. */
  private val debug: Boolean =
    if (appConf.hasPath("debug")) appConf.getBoolean("debug") else false

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
                                macroVar: String
                              ) {
    def matrixId(freq: Frequency): String =
      // Join only the non-empty parts so a blank RATE_TYPE (e.g. BPLS numeric segments) yields
      // `BPLS_10276_Q`, not `BPLS_10276__Q`. With a rate type it stays `BCEF_CONSO_TF_Q`.
      (Seq(perimeter, outSegment, rateType).filter(_.nonEmpty) :+ freq.suffix).mkString("_")
  }

  def getDataFrame: DataFrame = {
    log.info(s"Building $OUTPUT_EAD_FWD (shockWindow=$shockWindowStart..$shockWindowEnd, refShock=$refShock)")

    val perimeters = raInput.select(COL_PERIMETER).distinct().collect().map(_.getString(0)).toSet
    val ra = collectRa(raInput)
    val macroData = collectScenario(scenario)
    val matrices = parseParametrage(parametrage, perimeters)

    logRaKeyDiagnostics(ra, matrices)

    if (debug) {
      // Exact parsed RA keys with series length. Each key is quoted with [|] delimiters so a
      // stray space or odd character in SEGMENT / RATE_TYPE / FWL_TYPE / METRIC is visible
      // (e.g. "STRESS (+)" vs "STRESS (+) " vs "STRESS(+)"). An empty/missing key here is the
      // direct cause of an EMPTY ra_detail downstream.
      val keyLines = ra.toSeq
        .map { case ((seg, rt, fwl, metric), arr) => s"[$seg|$rt|$fwl|$metric] len=${arr.length}" }
        .sorted
      log.info(s"PARSED - RA keys (${ra.size}):\n  " + keyLines.mkString("\n  "))

      logShow("INPUT - PARAMETRAGE", parametrage)
      logShow("INPUT - MACRO_VARIABLE (scenario, shock window)",
        scenario.where(s"$COL_SCEN_DATE IN (${shockWindow.map(q => s"'$q'").mkString(",")})"))
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

  /** Compute the term-structure rows for one matrix / frequency / scenario. */
  private def matrixRows(
                          m: MatrixDef,
                          freq: Frequency,
                          scenName: String,
                          scenCode: String,
                          ra: Map[(String, String, String, String), Array[Double]],
                          macroData: Map[(String, String), Map[String, Double]]
                        ): Seq[Row] = {
    def series(fwl: String, metric: String): Array[Double] =
      aggregateSegments(ra, m.segments, m.rateType, fwl, metric)

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
    val raFiMinus = series(FWL_STRESS_MINUS, METRIC_RA_FI)
    val reMinus   = series(FWL_STRESS_MINUS, METRIC_RE)

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
        centralRa(crd, raStat, raFiB, reB, freq)
      } else {
        // term-varying shock: the macro delta path (vs Central) over the window selects the
        // stress leg and weight per term; both legs are supplied to scenarioRa.
        scenarioRa(crd, raStat, raFiB, reB,
          raFiPlus, rePlus, raFiMinus, reMinus,
          freq, deltaPath(macroData, scenName, m.macroVar, freq), refShock)
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
          val arr = macroDeltaArray(macroData, scenName, m.macroVar)
          if (arr.isEmpty) "0" else f"path ${arr.head}%.4f..${arr.last}%.4f"
        } else "0"
      logShow(s"TRACE - ${m.matrixId(freq)} / $scenCode  (delta=$deltaInfo, refShock=$refShock)",
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

  /** Signed macro delta (scenario - Central) for `macroVar` at each quarter of the shock window. */
  private def macroDeltaArray(
                               macroData: Map[(String, String), Map[String, Double]],
                               scenName: String, macroVar: String
                             ): Array[Double] =
    shockWindow.map { q =>
      def v(scen: String): Double = macroData.get((scen, q)).flatMap(_.get(macroVar)).getOrElse(0.0)
      v(scenName) - v(SCENARIO_CENTRAL)
    }.toArray

  /**
   * Maps a 1-based projection period to its macro delta on the window path:
   * term 0 = window start, step 1 quarter (quarterly) or 1 year = 4 quarters (yearly);
   * past the window end the last delta is held.
   */
  private def deltaPath(
                         macroData: Map[(String, String), Map[String, Double]],
                         scenName: String, macroVar: String, freq: Frequency
                       ): Int => Double = {
    val arr = macroDeltaArray(macroData, scenName, macroVar)
    val step = if (freq == Quarterly) 1 else 4
    (period: Int) => if (arr.isEmpty) 0.0 else arr(math.min((period - 1) * step, arr.length - 1))
  }

  // ---- input collection -----------------------------------------------------

  private def monthColumns(df: DataFrame): Seq[String] =
    df.columns.filter(c => c.length > 1 && c.charAt(0) == 'M' && c.drop(1).forall(_.isDigit))
      .sortBy(_.drop(1).toInt)

  /** key = (SEGMENT, RATE_TYPE, FWL_TYPE, METRIC) -> monthly series (M1..Mn). */
  private def collectRa(df: DataFrame): Map[(String, String, String, String), Array[Double]] = {
    val months = monthColumns(df)
    val cols = Seq(COL_SEGMENT, COL_RATE_TYPE, COL_FWL_TYPE, COL_METRIC) ++ months
    df.select(cols.head, cols.tail: _*).collect().map { r =>
      // Canonicalize key fields (null-safe): Excel cells frequently carry leading/trailing or
      // embedded odd whitespace (incl. the NBSP/narrow no-break space POI emits on French-locale
      // hosts), which would make the lookup key (e.g. "STRESS (+) ") miss the constant
      // ("STRESS (+)") and yield an empty series -> empty ra_detail -> empty non-Central scenarios.
      // canon() normalizes both this map's keys and the aggregateSegments lookup tuple identically.
      def key0(i: Int): String = canon(Option(r.get(i)).map(_.toString).getOrElse(""))
      val key = (key0(0), key0(1), key0(2), key0(3))
      val series = months.indices.map(i => toDouble(r.get(4 + i))).toArray
      key -> series
    }.toMap
  }

  /**
   * Surface the FWL_TYPE / METRIC label vocabulary actually present in the parsed RA input vs.
   * what the FWL=YES shock path needs, and flag any FWL=YES matrix whose STRESS(+)/STRESS(-)
   * legs are absent. A mismatch here (a stray NBSP, an alternate spelling, or simply missing
   * stress rows in the source) is the usual reason non-Central scenarios come back EMPTY. Keys
   * are already canon()ed, so what's printed is the normalized form actually used for matching.
   * Always on (independent of the debug flag).
   */
  private def logRaKeyDiagnostics(
                                   ra: Map[(String, String, String, String), Array[Double]],
                                   matrices: Seq[MatrixDef]
                                 ): Unit = {
    val fwlPresent    = ra.keySet.map(_._3)
    val metricPresent = ra.keySet.map(_._4)
    log.info(s"RA key vocabulary (canonicalized): " +
      s"FWL_TYPE=${fwlPresent.toSeq.sorted.mkString("{", ", ", "}")}  " +
      s"METRIC=${metricPresent.toSeq.sorted.mkString("{", ", ", "}")}")

    val missingFwl    = Seq(FWL_BASELINE, FWL_STRESS_PLUS, FWL_STRESS_MINUS).map(canon).filterNot(fwlPresent)
    val missingMetric = Seq(METRIC_CRD, METRIC_RA_STAT, METRIC_RA_FI, METRIC_RE).map(canon).filterNot(metricPresent)
    if (missingFwl.nonEmpty || missingMetric.nonEmpty)
      log.warn(s"RA input is missing expected shock-path labels — " +
        s"FWL_TYPE missing=${missingFwl.mkString("[", ", ", "]")} " +
        s"METRIC missing=${missingMetric.mkString("[", ", ", "]")}. " +
        s"Non-Central FWL=YES scenarios for affected matrices will be EMPTY. " +
        s"Check the source cells for non-breaking spaces / spelling vs the constants.")

    // Per-matrix: an FWL=YES matrix needs both stress legs (checked on RA FI) for >=1 segment.
    for (m <- matrices if m.fwlApplied) {
      def hasLeg(fwl: String): Boolean =
        m.segments.exists(s => ra.contains((canon(s), canon(m.rateType), canon(fwl), canon(METRIC_RA_FI))))
      val plus  = hasLeg(FWL_STRESS_PLUS)
      val minus = hasLeg(FWL_STRESS_MINUS)
      if (!plus || !minus)
        log.warn(s"FWL=YES matrix ${m.outSegment}/${m.rateType} (segments=${m.segments.mkString(",")}) " +
          s"is missing a stress leg (STRESS(+) present=$plus, STRESS(-) present=$minus); " +
          s"its non-Central scenarios will be EMPTY.")
    }
  }

  /** Element-wise sum of the monthly series across constituent segments (same rate type). */
  private def aggregateSegments(
                                 ra: Map[(String, String, String, String), Array[Double]],
                                 segments: Seq[String], rateType: String, fwl: String, metric: String
                               ): Array[Double] = {
    // canon() the lookup tuple to match collectRa's canonicalized keys (whitespace/NBSP/case).
    val present = segments.flatMap(s => ra.get((canon(s), canon(rateType), canon(fwl), canon(metric))))
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
      COL_AGG_SEGMENT_NAME, COL_FWL_TO_BE_APPLIED, COL_MACRO_VARIABLE)
    val recs = df.select(cols.head, cols.tail: _*).collect()
      .map { r =>
        (str(r, 0), str(r, 1), str(r, 2), str(r, 3), str(r, 4), str(r, 5), str(r, 6))
      }
      .filter { case (perim, seg, _, _, _, _, _) => perimeters.contains(perim) && seg.nonEmpty }

    // group by (perimeter, output segment, rate type) so INVEST_PRO + INVEST_CORP collapse
    // into INVEST while distinct rate types (TF/TV) stay separate matrices.
    recs.groupBy { case (perim, seg, rateType, agg, aggName, _, _) =>
      val out = if (agg.equalsIgnoreCase(YES) && aggName.nonEmpty) aggName else seg
      (perim, out, rateType)
    }.map { case ((perim, out, rateType), group) =>
      val segments = group.map(_._2).distinct.toSeq
      // combined FWL flag for an aggregated matrix: YES if ANY constituent applies FWL.
      val fwlApplied = group.exists(_._6.equalsIgnoreCase(YES))
      val macroVar = group.map(_._7).find(v => v.nonEmpty && !v.equalsIgnoreCase("NONE")).getOrElse("")
      MatrixDef(perim, out, rateType, segments, fwlApplied, macroVar)
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

  private def toDouble(v: Any): Double = v match {
    case null              => 0.0
    case d: Double         => d
    case n: java.lang.Number => n.doubleValue()
    case s: String         =>
      // spark-excel returns locale-formatted strings (e.g. "-8,128" with a thousands
      // separator); inputs use '.' as the decimal mark, so commas are group separators.
      // Strip ALL grouping whitespace too — including the non-breaking space (U+00A0) and
      // narrow no-break space (U+202F) that POI emits on French-locale hosts. Java's \s does
      // NOT match those, so a plain .replace(" ", "") left them in and large values (e.g.
      // "-8 128") threw NumberFormatException -> 0.0, firing the CRD==0 run-off guard.
      val t = s.trim.replace(",", "").replaceAll("[\\s\\u00A0\\u202F]", "")
      if (t.isEmpty) 0.0 else try t.toDouble catch { case _: NumberFormatException => 0.0 }
    case other             => try other.toString.toDouble catch { case _: Throwable => 0.0 }
  }

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
