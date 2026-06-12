package com.bnp.str.tseadfwd.job

import com.bnp.str.tseadfwd.mapping.{PrimaryMapper, PrimaryView, Term0RowView}
import com.bnp.str.tseadfwd.reader.PrimaryReader
import com.bnp.str.tseadfwd.sessionmanager.SparkSessionManager
import com.bnp.str.tseadfwd.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Spark job that auto-generates the EAD_RA_RATE computation breakdown for every (matrix, scenario)
 * at a CONFIGURABLE set of terms, in the spirit of the hand-written
 * `docs/ANALYSIS_TERM0_COMPUTATION.md` — but driven entirely by the configured inputs.
 *
 * It reads the same inputs as the production pipeline (RA perimeters, MACRO_VARIABLE, PARAMETRAGE)
 * via [[PrimaryReader]], then asks [[PrimaryMapper.term0AnalysisRows]] for the breakdown — so every
 * value is computed through the SAME validated parsing + [[PrimaryView]] formulas as the production
 * output and equals the production `EAD_RA_RATE` at each term by construction.
 *
 * Single argument: the path to `application.conf`. Everything is read from the
 * `tseadfwd_app.TERM0_ANALYSIS` block:
 *   enabled    — when false, the job does nothing (generation gate). Default true.
 *   terms      — list of output terms to break down, e.g. `[0, 0.25, 1, 5, 30]`. Default `[0]`.
 *   enginePath — (optional) the REAL engine output CSV (TS_EAD_FWD). When set, each analysis `ead`
 *                is reconciled against the engine's `EAD_RA_RATE` at the same term -> STATUS column
 *                (MATCH / DIFF / MISSING). A bad input parse then shows as DIFF, not a silent error.
 *   tol        — abs-error threshold for a MATCH (default 1e-6).
 *   mdPath / csvPath — outputs (Markdown narrative + decimal-comma CSV). Default next to the output.
 */
object Term0AnalysisDriver {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val confPath = args.lift(0).getOrElse("localRun/tseadfwd/application.conf")

    implicit val spark: SparkSession = SparkSessionManager.fetchSparkSession("term0-analysis")
    spark.sparkContext.setLogLevel("ERROR")

    // Read the conf through Hadoop's FileSystem (like MainDriver), so the same call resolves a
    // local path, a `--files`-shipped conf (basename in the working dir), or an HDFS path.
    implicit val config: Config = {
      val reader = PrimaryUtilities.getHdfsReader(confPath)(spark.sparkContext)
      try ConfigFactory.parseReader(reader).resolve() finally reader.close()
    }
    val appConf = config.getConfig(PrimaryConstants.APP_CONF)
    val anaConf =
      if (appConf.hasPath("TERM0_ANALYSIS")) appConf.getConfig("TERM0_ANALYSIS") else ConfigFactory.empty()

    // ---- generation gate ----
    val enabled = !anaConf.hasPath("enabled") || anaConf.getBoolean("enabled")
    if (!enabled) {
      logger.info("TERM0_ANALYSIS.enabled = false -> analysis generation skipped.")
      println(">>> Term analysis disabled (TERM0_ANALYSIS.enabled = false); nothing generated.")
      spark.stop()
      return
    }

    def strOr(key: String, default: String): String =
      if (anaConf.hasPath(key)) anaConf.getString(key) else default

    // ---- configurable terms ----
    val terms: Seq[Double] = {
      import scala.collection.JavaConverters._
      if (anaConf.hasPath("terms"))
        anaConf.getDoubleList("terms").asScala.map(_.doubleValue()).toVector
      else Seq(0.0)
    }
    val tol         = if (anaConf.hasPath("tol")) anaConf.getDouble("tol") else 1e-6
    val enginePath  = if (anaConf.hasPath("enginePath")) Some(anaConf.getString("enginePath")) else None
    val mdPath      = strOr("mdPath", "localRun/tseadfwd/output/ANALYSIS_TERM0_GENERATED.md")
    val csvPath     = strOr("csvPath", "localRun/tseadfwd/output/ANALYSIS_TERM0_GENERATED.csv")

    val reader = new PrimaryReader()
    val mapper = new PrimaryMapper(
      reader.raInput,
      reader.getMappingReader(PrimaryConstants.MACRO_VARIABLE),
      reader.getMappingReader(PrimaryConstants.PARAMETRAGE),
      PrimaryConstants.OUTPUT_EAD_FWD
    )

    val rawRows = mapper.term0AnalysisRows(terms, PrimaryView.Quarterly)
    logger.info(s"Term analysis: ${rawRows.size} (matrix, scenario, term) breakdown(s) computed " +
      s"over terms ${terms.map(snap).distinct.sorted.mkString(", ")}")

    // ---- reconcile against the real engine output, when configured ----
    val rows = enginePath match {
      case Some(ep) => reconcile(rawRows, ep, tol)
      case None     => rawRows
    }

    writeMarkdown(mdPath, rows)
    writeCsv(csvPath, rows)

    val nMatrices = rows.map(_.matrixId).distinct.size
    val nTerms    = rows.map(_.term).distinct.size
    val recoSummary = {
      val statuses = rows.map(_.status).filter(_.nonEmpty)
      if (statuses.isEmpty) "no engine reconciliation"
      else {
        val diff = statuses.count(_ == "DIFF"); val miss = statuses.count(_ == "MISSING")
        s"reconciled vs engine: ${statuses.count(_ == "MATCH")} MATCH, $diff DIFF, $miss MISSING"
      }
    }
    println(s"\n>>> Term analysis written:\n    markdown: $mdPath\n    csv     : $csvPath" +
      s"\n    ($nMatrices matrices x scenarios x $nTerms terms = ${rows.size} rows; $recoSummary)")

    spark.stop()
  }

  // ---- engine reconciliation ------------------------------------------------

  /**
   * Attach the REAL engine output's `EAD_RA_RATE` (at the same matrix/scenario/term) and a STATUS
   * to each row. The engine output (`TS_EAD_FWD`) keeps the RATE_TYPE token, so its id equals the
   * analysis `matrixId` — no normalization needed. Returns the rows unchanged (status="MISSING") if
   * the engine file cannot be read.
   */
  private def reconcile(rows: Seq[Term0RowView], enginePath: String, tol: Double)
                       (implicit spark: SparkSession): Seq[Term0RowView] = {
    val engine: Map[(String, String, Double), Double] =
      try {
        val df = spark.read.option("header", "true").option("delimiter", ";").csv(enginePath)
        df.select(PrimaryConstants.OUT_MATRIX_ID, PrimaryConstants.OUT_SCENARIO_ID,
            PrimaryConstants.OUT_TERM, PrimaryConstants.OUT_EAD_RA_RATE)
          .collect().flatMap { r =>
            val mid = r.getString(0); val scen = r.getString(1)
            val t = parseComma(r.getString(2)); val e = parseComma(r.getString(3))
            if (t.isNaN) None else Some((mid, scen, snap(t)) -> e)
          }.toMap
      } catch {
        case ex: Throwable =>
          logger.warn(s"Could not read engine output for reconciliation ($enginePath): ${ex.getMessage}")
          Map.empty
      }

    if (engine.isEmpty) rows.map(_.copy(status = "MISSING"))
    else rows.map { r =>
      engine.get((r.matrixId, r.scenarioCode, r.term)) match {
        case Some(e) =>
          val status = if (!e.isNaN && !r.ead.isNaN && math.abs(e - r.ead) <= tol) "MATCH" else "DIFF"
          r.copy(engineEad = e, status = status)
        case None => r.copy(status = "MISSING")
      }
    }
  }

  // ---- formatting -----------------------------------------------------------

  /** Round a term to a clean grid value (must match PrimaryMapper.snap). */
  private def snap(t: Double): Double = Math.round(t * 1e6) / 1e6

  /** Decimal-comma string -> Double (NaN on empty / non-numeric). */
  private def parseComma(s: String): Double =
    if (s == null || s.trim.isEmpty) Double.NaN
    else try s.trim.replace(",", ".").toDouble catch { case _: Throwable => Double.NaN }

  /** Full-precision dot-decimal for the narrative (NaN -> "n/a"). */
  private def dot(v: Double, dp: Int = 6): String =
    if (v.isNaN || v.isInfinite) "n/a" else String.format(java.util.Locale.ROOT, s"%.${dp}f", Double.box(v))

  /** Compact term label: "0", "0.25", "30" (no trailing zeros). */
  private def termStr(t: Double): String =
    BigDecimal(t).setScale(6, BigDecimal.RoundingMode.HALF_UP).bigDecimal.stripTrailingZeros.toPlainString

  /** Decimal-comma for the CSV, half-up at 12 dp, trailing zeros stripped (NaN/Inf -> empty). */
  private def comma(v: Double): String =
    if (v.isNaN || v.isInfinite) ""
    else BigDecimal(v).setScale(12, BigDecimal.RoundingMode.HALF_UP)
      .bigDecimal.stripTrailingZeros.toPlainString.replace(".", ",")

  private def month(xs: Seq[Double], i: Int, dp: Int = 6): String =
    if (i < xs.length) dot(xs(i), dp) else "n/a"

  /** Loss-rate determinant `-x / c` (0 when CRD has run off) — mirrors PrimaryView.scenarioRa. */
  private def det(x: Double, c: Double): Double = if (c == 0.0) 0.0 else -x / c

  /**
   * The exact arithmetic the engine used to reach RA / VECTOR / EAD_RA_RATE at one term, rendered as
   * a worked block. A non-shock term (Central, or any scenario when FWL=NO) uses
   * `RA = -(STAT[+FI+RE]) / CRD`; a shocked term decomposes the stat / fire-base / shock pieces of
   * [[PrimaryView.scenarioRa]]. The cumulative-product line shows the explicit `prevEad * VECTOR`
   * only when this term's period immediately follows the previous listed one — otherwise intermediate
   * periods are hidden, so it states the running product instead of inventing a one-step multiply.
   */
  private def termSteps(r: Term0RowView, prevPeriod: Int, prevEad: Double): String = {
    val sb = new StringBuilder
    val t  = termStr(r.term)
    val consecutive = !prevEad.isNaN && r.period == prevPeriod + 1
    val eadLine =
      if (consecutive) s"EAD_RA_RATE($t) = ${dot(prevEad, 8)} * ${dot(r.vector, 8)} = ${dot(r.ead, 8)}"
      else s"EAD_RA_RATE($t) = running product of VECTOR through period ${r.period} = ${dot(r.ead, 8)}"

    if (r.crdAgg == 0.0) {
      sb.append(s"term $t (period ${r.period}) -- CRD run off (=0): RA = 0, VECTOR = 1\n")
    } else if (r.usesShock) {
      val statDet     = det(r.statAgg, r.crdAgg)
      val fireBaseDet = det(r.fiAgg, r.crdAgg) + det(r.reAgg, r.crdAgg)
      val shockFi     = det(r.legFiAgg, r.legCrdAgg) - det(r.fiAgg, r.crdAgg)
      val shockRe     = det(r.legReAgg, r.legCrdAgg) - det(r.reAgg, r.crdAgg)
      sb.append(s"term $t (period ${r.period}, macro delta = ${dot(r.delta, 6)})\n")
      sb.append(s"  stat_det      = -(STAT)/CRD             = -(${dot(r.statAgg)}) / ${dot(r.crdAgg)} = ${dot(statDet, 8)}\n")
      sb.append(s"  fire_base_det = -(FI+RE)/CRD            = -(${dot(r.fiAgg)} + ${dot(r.reAgg)}) / ${dot(r.crdAgg)} = ${dot(fireBaseDet, 8)}\n")
      sb.append(s"  shock_fi      = -FI_leg/CRD_leg + FI/CRD = ${dot(shockFi, 8)}\n")
      sb.append(s"  shock_re      = -RE_leg/CRD_leg + RE/CRD = ${dot(shockRe, 8)}\n")
      sb.append(s"  RA($t)        = stat_det + fire_base_det - (shock_fi + shock_re) * delta\n")
      sb.append(s"                = ${dot(statDet, 8)} + ${dot(fireBaseDet, 8)} - (${dot(shockFi, 8)} + ${dot(shockRe, 8)}) * ${dot(r.delta, 6)} = ${dot(r.ra, 8)}\n")
      sb.append(s"  VECTOR($t)    = 1 - ${dot(r.ra, 8)} = ${dot(r.vector, 8)}\n")
    } else {
      val numerator = if (r.fwlApplied) r.statAgg + r.fiAgg + r.reAgg else r.statAgg
      val numTxt =
        if (r.fwlApplied) s"${dot(r.statAgg)} + ${dot(r.fiAgg)} + ${dot(r.reAgg)} = ${dot(numerator)}"
        else dot(r.statAgg)
      sb.append(s"term $t (period ${r.period})\n")
      sb.append(s"  numerator     = ${if (r.fwlApplied) "STAT + FI + RE" else "STAT"} = $numTxt\n")
      sb.append(s"  RA($t)        = -(${dot(numerator)}) / ${dot(r.crdAgg)} = ${dot(r.ra, 8)}\n")
      sb.append(s"  VECTOR($t)    = 1 - ${dot(r.ra, 8)} = ${dot(r.vector, 8)}\n")
    }
    sb.append(s"  $eadLine\n\n")
    sb.toString()
  }

  // ---- CSV ------------------------------------------------------------------

  private def writeCsv(path: String, rows: Seq[Term0RowView])(implicit spark: SparkSession): Unit =
    PrimaryUtilities.writeStringToHdfs(path, renderCsv(rows))(spark.sparkContext)

  /** Render the decimal-comma, `;`-delimited CSV. Public for unit testing. */
  def renderCsv(rows: Seq[Term0RowView]): String = {
    val sb = new StringBuilder
    sb.append(Seq(
      "EAD_MATRIX_ID", "SCENARIO_ID", "TERM", "PERIOD", "FWL_APPLIED", "MACRO_VAR", "USES_SHOCK",
      "CRD_M1", "CRD_M2", "CRD_M3", "STAT_M1", "STAT_M2", "STAT_M3",
      "FI_M1", "FI_M2", "FI_M3", "RE_M1", "RE_M2", "RE_M3",
      "CRD_AGG", "STAT_AGG", "FI_AGG", "RE_AGG",
      "LEG_CRD_AGG", "LEG_FI_AGG", "LEG_RE_AGG",
      "DELTA", "RA", "VECTOR", "EAD_RA_RATE", "ENGINE_EAD", "STATUS").mkString(";")).append("\n")
    sortRows(rows).foreach { r =>
      def m(xs: Seq[Double], i: Int): String = if (i < xs.length) comma(xs(i)) else ""
      sb.append(Seq(
        r.matrixId, r.scenarioCode, comma(r.term), r.period.toString,
        r.fwlApplied.toString, r.macroVar, r.usesShock.toString,
        m(r.crdMonths, 0), m(r.crdMonths, 1), m(r.crdMonths, 2),
        m(r.statMonths, 0), m(r.statMonths, 1), m(r.statMonths, 2),
        m(r.fiMonths, 0), m(r.fiMonths, 1), m(r.fiMonths, 2),
        m(r.reMonths, 0), m(r.reMonths, 1), m(r.reMonths, 2),
        comma(r.crdAgg), comma(r.statAgg), comma(r.fiAgg), comma(r.reAgg),
        comma(r.legCrdAgg), comma(r.legFiAgg), comma(r.legReAgg),
        comma(r.delta), comma(r.ra), comma(r.vector), comma(r.ead),
        comma(r.engineEad), r.status).mkString(";")).append("\n")
    }
    sb.toString()
  }

  /** Stable order: matrix, scenario (canonical order), then term. */
  private def sortRows(rows: Seq[Term0RowView]): Seq[Term0RowView] = {
    val scenOrder = PrimaryConstants.SCENARIO_CODES.map(_._1).zipWithIndex.toMap
    rows.sortBy(r => (r.matrixId, scenOrder.getOrElse(r.scenarioName, Int.MaxValue), r.term))
  }

  // ---- Markdown -------------------------------------------------------------

  private def writeMarkdown(path: String, rows: Seq[Term0RowView])(implicit spark: SparkSession): Unit =
    PrimaryUtilities.writeStringToHdfs(path, renderMarkdown(rows))(spark.sparkContext)

  /** Render the full Markdown narrative. Public for unit testing. */
  def renderMarkdown(rows: Seq[Term0RowView]): String = {
    val sb = new StringBuilder
    val reconciled = rows.exists(_.status.nonEmpty)
    val allTerms = rows.map(_.term).distinct.sorted

    sb.append("# Analysis — EAD_RA_RATE computation breakdown (generated)\n\n")
    sb.append("Auto-generated by the `Term0AnalysisDriver` job from the configured inputs — the machine\n")
    sb.append("equivalent of `docs/ANALYSIS_TERM0_COMPUTATION.md`. Every value is computed through the\n")
    sb.append("same validated parsing and `PrimaryView` formulas as the production output, so each\n")
    sb.append("`EAD_RA_RATE` matches the engine at that term.\n\n")
    sb.append(s"Terms analysed (quarterly grid; term 0 = Q1): ${allTerms.map(termStr).mkString(", ")}.\n")
    sb.append("Aggregation: `CRD = mean(window months)`; `RA metric = M1 + M2/2` (Q1) / half-weight\n")
    sb.append("window thereafter. Loss rate: `RA = -(STAT+FI+RE)/CRD` (FWL=YES) or `-(STAT)/CRD` (FWL=NO);\n")
    sb.append("`VECTOR = 1 - RA`; `EAD_RA_RATE` = cumulative product of VECTOR (held flat past horizon).\n")
    if (reconciled)
      sb.append("`ENGINE` / `STATUS` reconcile each `EAD_RA_RATE` against the real engine output CSV.\n")
    sb.append("\n---\n\n")

    val byMatrix = rows.groupBy(_.matrixId)
    val matrixOrder = rows.map(_.matrixId).distinct
    val scenOrder = PrimaryConstants.SCENARIO_CODES.map(_._1).zipWithIndex.toMap

    matrixOrder.zipWithIndex.foreach { case (mid, idx) =>
      val group = byMatrix(mid)
      val head = group.head // raw-input sample is identical across the matrix's rows
      sb.append(s"## ${idx + 1}. `$mid`\n\n")
      sb.append(s"- Segments: `${head.segments.mkString(" + ")}` — RATE_TYPE: " +
        (if (head.rateType.nonEmpty) s"`${head.rateType}`" else "*(blank)*") + "\n")
      sb.append(s"- FWL_TO_BE_APPLIED: **${if (head.fwlApplied) "YES" else "NO"}**" +
        (if (head.fwlApplied && head.macroVar.nonEmpty) s" — macro var `${head.macroVar}`" else "") + "\n")
      val ruleTxt =
        if (head.fwlApplied) "`RA = -(RA_STAT + RA_FI + RE) / CRD`  (STAT+FI+RE)"
        else "`RA = -(RA_STAT) / CRD`  (stat-only; FI/RE excluded)"
      sb.append(s"- Loss-rate rule: $ruleTxt\n\n")

      // Raw baseline input sample (first 3 months) — a quick parse sanity check.
      sb.append("**Raw baseline input sample (first 3 months)**\n\n")
      sb.append("| Metric | M1 | M2 | M3 |\n|---|---|---|---|\n")
      sb.append(s"| CRD | ${month(head.crdMonths, 0)} | ${month(head.crdMonths, 1)} | ${month(head.crdMonths, 2)} |\n")
      sb.append(s"| RA STAT | ${month(head.statMonths, 0)} | ${month(head.statMonths, 1)} | ${month(head.statMonths, 2)} |\n")
      sb.append(s"| RA FI | ${month(head.fiMonths, 0)} | ${month(head.fiMonths, 1)} | ${month(head.fiMonths, 2)} |\n")
      sb.append(s"| RE | ${month(head.reMonths, 0)} | ${month(head.reMonths, 1)} | ${month(head.reMonths, 2)} |\n\n")

      // Worked Term-0 Central example, when term 0 was requested.
      group.find(r => r.scenarioName == PrimaryConstants.SCENARIO_CENTRAL && r.term == 0.0).foreach { c =>
        val numerator = if (head.fwlApplied) c.statAgg + c.fiAgg + c.reAgg else c.statAgg
        val numTxt = if (head.fwlApplied)
          s"${dot(c.statAgg)} + ${dot(c.fiAgg)} + ${dot(c.reAgg)} = ${dot(numerator)}"
        else dot(c.statAgg)
        sb.append("**Worked Term 0 — Central (no shock)**\n\n```\n")
        sb.append(s"numerator           = $numTxt\n")
        sb.append(s"RA(Term 0)          = -(${dot(numerator)}) / ${dot(c.crdAgg)} = ${dot(c.ra, 8)}\n")
        sb.append(s"VECTOR(Term 0)      = 1 - ${dot(c.ra, 8)} = ${dot(c.vector, 8)}\n")
        sb.append(s"EAD_RA_RATE(Term 0) = ${dot(c.ead, 8)}\n```\n\n")
      }

      // Per-scenario term table.
      group.groupBy(_.scenarioName).toSeq
        .sortBy { case (scen, _) => scenOrder.getOrElse(scen, Int.MaxValue) }
        .foreach { case (scen, scenRows) =>
          val code = scenRows.head.scenarioCode
          sb.append(s"**$scen ($code) — per-term breakdown**\n\n")
          val engCols = if (reconciled) " ENGINE | STATUS |" else ""
          val engSep  = if (reconciled) "---|---|" else ""
          sb.append(s"| term | CRD | RA_STAT | RA_FI | RE | delta | RA | VECTOR | EAD_RA_RATE |$engCols\n")
          sb.append(s"|---|---|---|---|---|---|---|---|---|$engSep\n")
          scenRows.sortBy(_.term).foreach { r =>
            val deltaTxt = if (r.usesShock) dot(r.delta, 6) else "—"
            val eng = if (reconciled) s" ${dot(r.engineEad, 8)} | ${if (r.status.isEmpty) "—" else r.status} |" else ""
            sb.append(s"| ${termStr(r.term)} | ${dot(r.crdAgg)} | ${dot(r.statAgg)} | ${dot(r.fiAgg)} | " +
              s"${dot(r.reAgg)} | $deltaTxt | ${dot(r.ra, 8)} | ${dot(r.vector, 8)} | ${dot(r.ead, 8)} |$eng\n")
          }
          sb.append("\n")

          // Worked computation steps for every listed term — the exact arithmetic of
          // PrimaryView.scenarioRa, so each line shows how the engine reached RA / VECTOR /
          // EAD_RA_RATE at that term and the cumulative product is auditable term by term.
          sb.append(s"*$scen ($code) — worked computation steps per term*\n\n```\n")
          var prevPeriod = Int.MinValue
          var prevEad    = Double.NaN
          scenRows.sortBy(_.term).foreach { r =>
            sb.append(termSteps(r, prevPeriod, prevEad))
            prevPeriod = r.period
            prevEad    = r.ead
          }
          sb.append("```\n\n")
        }
      sb.append("---\n\n")
    }
    sb.toString()
  }
}
