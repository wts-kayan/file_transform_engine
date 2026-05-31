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
                     ra_bcef: DataFrame,
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
      s"${perimeter}_${outSegment}_${rateType}_${freq.suffix}"
  }

  def getDataFrame: DataFrame = {
    log.info(s"Building $OUTPUT_EAD_FWD (shockWindow=$shockWindowStart..$shockWindowEnd, refShock=$refShock)")

    val perimeters = ra_bcef.select(COL_PERIMETER).distinct().collect().map(_.getString(0)).toSet
    val ra = collectRa(ra_bcef)
    val macroData = collectScenario(scenario)
    val matrices = parseParametrage(parametrage, perimeters)

    if (debug) {
      logShow("INPUT - PARAMETRAGE", parametrage)
      logShow("INPUT - MACRO_VARIABLE (scenario, shock window)",
        scenario.where(s"$COL_SCEN_DATE IN (${shockWindow.map(q => s"'$q'").mkString(",")})"))
      // M1..Mn are MONTHLY columns (METRIC is the separate key column). The full series is
      // used by collectRa; here we only preview the first/last 3 months to keep the table readable.
      val allMonths = monthColumns(ra_bcef)
      val sampleMonths = allMonths.take(3) ++ allMonths.takeRight(3)
      val raCols = Seq(COL_PERIMETER, COL_SEGMENT, COL_RATE_TYPE, COL_FWL_TYPE, COL_METRIC) ++ sampleMonths
      logShow(s"INPUT - RA_BCEF (keys + first/last months; ${allMonths.length} monthly cols used in full)",
        ra_bcef.select(raCols.head, raCols.tail: _*))
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

    val ra_detail: Vector[Double] =
      if (!m.fwlApplied || scenName == SCENARIO_CENTRAL) {
        centralRa(crd, raStat, raFiB, reB, freq)
      } else {
        // term-varying shock: the macro delta path (vs Central) over the window selects the
        // stress leg and weight per term; both legs are supplied to scenarioRa.
        scenarioRa(crd, raStat, raFiB, reB,
          series(FWL_STRESS_PLUS, METRIC_RA_FI), series(FWL_STRESS_PLUS, METRIC_RE),
          series(FWL_STRESS_MINUS, METRIC_RA_FI), series(FWL_STRESS_MINUS, METRIC_RE),
          freq, deltaPath(macroData, scenName, m.macroVar, freq), refShock)
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
      val key = (r.getString(0), r.getString(1), r.getString(2), r.getString(3))
      val series = months.indices.map(i => toDouble(r.get(4 + i))).toArray
      key -> series
    }.toMap
  }

  /** Element-wise sum of the monthly series across constituent segments (same rate type). */
  private def aggregateSegments(
                                 ra: Map[(String, String, String, String), Array[Double]],
                                 segments: Seq[String], rateType: String, fwl: String, metric: String
                               ): Array[Double] = {
    val present = segments.flatMap(s => ra.get((s, rateType, fwl, metric)))
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

  private def toDouble(v: Any): Double = v match {
    case null              => 0.0
    case d: Double         => d
    case n: java.lang.Number => n.doubleValue()
    case s: String         =>
      // spark-excel returns locale-formatted strings (e.g. "-8,128" with a thousands
      // separator); inputs use '.' as the decimal mark, so commas are group separators.
      val t = s.trim.replace(",", "").replace(" ", "")
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
