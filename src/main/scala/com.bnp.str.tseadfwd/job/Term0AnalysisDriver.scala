package com.bnp.str.tseadfwd.job

import com.bnp.str.tseadfwd.mapping.{PrimaryMapper, PrimaryView, Term0RowView}
import com.bnp.str.tseadfwd.reader.PrimaryReader
import com.bnp.str.tseadfwd.sessionmanager.SparkSessionManager
import com.bnp.str.tseadfwd.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.io.{File, PrintWriter}

/**
 * Spark job that auto-generates the Term-0 (period 1) EAD_RA_RATE computation breakdown for every
 * (matrix, scenario), in the spirit of the hand-written `docs/ANALYSIS_TERM0_COMPUTATION.md` —
 * but driven entirely by the configured input workbooks rather than typed by hand.
 *
 * It reads the same inputs as the production pipeline (RA perimeters, MACRO_VARIABLE, PARAMETRAGE)
 * via [[PrimaryReader]], then asks [[PrimaryMapper.term0AnalysisRows]] for the breakdown — so every number
 * is computed through the SAME validated parsing + [[PrimaryView]] formulas and equals the
 * production `EAD_RA_RATE` at TERM 0.
 *
 * Single argument: the path to `application.conf`. Output paths are read from the
 * `tseadfwd_app.TERM0_ANALYSIS` block:
 *   mdPath  — Markdown narrative (worked example per matrix, all scenarios)
 *   csvPath — `;`-delimited, decimal-comma CSV: one row per (matrix, scenario) with every
 *             intermediate value (M1..M3, Q1 aggregates, stress-leg Q1, delta, RA, VECTOR, EAD).
 * Both default next to the other outputs if the block (or a key) is absent.
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
    def pathOr(key: String, default: String): String =
      if (anaConf.hasPath(key)) anaConf.getString(key) else default

    val mdPath  = pathOr("mdPath", "localRun/tseadfwd/output/ANALYSIS_TERM0_GENERATED.md")
    val csvPath = pathOr("csvPath", "localRun/tseadfwd/output/ANALYSIS_TERM0_GENERATED.csv")

    val reader = new PrimaryReader()
    val mapper = new PrimaryMapper(
      reader.raInput,
      reader.getMappingReader(PrimaryConstants.MACRO_VARIABLE),
      reader.getMappingReader(PrimaryConstants.PARAMETRAGE),
      PrimaryConstants.OUTPUT_EAD_FWD
    )

    val rows = mapper.term0AnalysisRows(PrimaryView.Quarterly)
    logger.info(s"Term-0 analysis: ${rows.size} (matrix, scenario) breakdown(s) computed")

    writeMarkdown(mdPath, rows)
    writeCsv(csvPath, rows)

    val matrices = rows.map(_.matrixId).distinct.size
    println(s"\n>>> Term-0 analysis written:\n    markdown: $mdPath\n    csv     : $csvPath" +
      s"\n    ($matrices matrices x scenarios = ${rows.size} rows)")

    spark.stop()
  }

  // ---- formatting -----------------------------------------------------------

  /** Full-precision dot-decimal for the narrative (NaN -> "n/a"). */
  private def dot(v: Double, dp: Int = 6): String =
    if (v.isNaN || v.isInfinite) "n/a" else String.format(java.util.Locale.ROOT, s"%.${dp}f", Double.box(v))

  /** Decimal-comma for the CSV, half-up at 12 dp, trailing zeros stripped (NaN/Inf -> empty). */
  private def comma(v: Double): String =
    if (v.isNaN || v.isInfinite) ""
    else BigDecimal(v).setScale(12, BigDecimal.RoundingMode.HALF_UP)
      .bigDecimal.stripTrailingZeros.toPlainString.replace(".", ",")

  private def month(xs: Seq[Double], i: Int, dp: Int = 6): String =
    if (i < xs.length) dot(xs(i), dp) else "n/a"

  // ---- CSV ------------------------------------------------------------------

  private def writeCsv(path: String, rows: Seq[Term0RowView]): Unit = {
    val f = new File(path)
    Option(f.getParentFile).foreach(_.mkdirs())
    val pw = new PrintWriter(f, "UTF-8")
    try {
      pw.println(Seq(
        "EAD_MATRIX_ID", "SCENARIO_ID", "FWL_APPLIED", "MACRO_VAR", "USES_SHOCK",
        "CRD_M1", "CRD_M2", "CRD_M3", "STAT_M1", "STAT_M2", "STAT_M3",
        "FI_M1", "FI_M2", "FI_M3", "RE_M1", "RE_M2", "RE_M3",
        "CRD_Q1", "STAT_Q1", "FI_Q1", "RE_Q1",
        "LEG_CRD_Q1", "LEG_FI_Q1", "LEG_RE_Q1",
        "DELTA0", "RA_TERM0", "VECTOR_TERM0", "EAD_RA_RATE_TERM0").mkString(";"))
      rows.foreach { r =>
        def m(xs: Seq[Double], i: Int): String = if (i < xs.length) comma(xs(i)) else ""
        pw.println(Seq(
          r.matrixId, r.scenarioCode, r.fwlApplied.toString, r.macroVar, r.usesShock.toString,
          m(r.crdMonths, 0), m(r.crdMonths, 1), m(r.crdMonths, 2),
          m(r.statMonths, 0), m(r.statMonths, 1), m(r.statMonths, 2),
          m(r.fiMonths, 0), m(r.fiMonths, 1), m(r.fiMonths, 2),
          m(r.reMonths, 0), m(r.reMonths, 1), m(r.reMonths, 2),
          comma(r.crdQ1), comma(r.statQ1), comma(r.fiQ1), comma(r.reQ1),
          comma(r.legCrdQ1), comma(r.legFiQ1), comma(r.legReQ1),
          comma(r.delta0), comma(r.ra0), comma(r.vector0), comma(r.ead0)).mkString(";"))
      }
    } finally pw.close()
  }

  // ---- Markdown -------------------------------------------------------------

  private def writeMarkdown(path: String, rows: Seq[Term0RowView]): Unit = {
    val f = new File(path)
    Option(f.getParentFile).foreach(_.mkdirs())
    val pw = new PrintWriter(f, "UTF-8")
    try pw.print(renderMarkdown(rows)) finally pw.close()
  }

  /** Render the full Markdown narrative. Public for unit testing. */
  def renderMarkdown(rows: Seq[Term0RowView]): String = {
    val sb = new StringBuilder
    sb.append("# Analysis — Term-0 (period 1) EAD_RA_RATE computation (generated)\n\n")
    sb.append("Auto-generated by the `Term0AnalysisDriver` job from the configured inputs — the machine\n")
    sb.append("equivalent of `docs/ANALYSIS_TERM0_COMPUTATION.md`. **Term 0 = quarter Q1** (period 1),\n")
    sb.append("quarterly. Every value is computed through the same validated parsing and `PrimaryView`\n")
    sb.append("formulas as the production output, so `EAD_RA_RATE (Term 0)` matches it exactly.\n\n")
    sb.append("Aggregation at Q1: `CRD_Q1 = mean(M1,M2,M3)`; `X_Q1 = M1 + M2/2` for RA STAT / RA FI / RE.\n")
    sb.append("Central / FWL=NO loss rate: `RA = -(STAT+FI+RE)/CRD` (FWL=YES) or `-(STAT)/CRD` (FWL=NO).\n")
    sb.append("`VECTOR = 1 - RA`; at Term 0 the factored value seeds the cumulative product, so\n")
    sb.append("`EAD_RA_RATE(Term 0) = VECTOR`.\n\n")
    sb.append("---\n\n")

    // Preserve discovery order; group scenarios under their matrix.
    val byMatrix = rows.groupBy(_.matrixId)
    val matrixOrder = rows.map(_.matrixId).distinct

    matrixOrder.zipWithIndex.foreach { case (mid, idx) =>
      val group = byMatrix(mid)
      val head = group.head // baseline Q1 is identical across scenarios of a matrix
      sb.append(s"## ${idx + 1}. `$mid`\n\n")
      sb.append(s"- Segments: `${group.head.segments.mkString(" + ")}` — RATE_TYPE: " +
        (if (head.rateType.nonEmpty) s"`${head.rateType}`" else "*(blank)*") + "\n")
      sb.append(s"- FWL_TO_BE_APPLIED: **${if (head.fwlApplied) "YES" else "NO"}**" +
        (if (head.fwlApplied && head.macroVar.nonEmpty) s" — macro var `${head.macroVar}`" else "") + "\n")
      val ruleTxt =
        if (head.fwlApplied) "`RA = -(RA_STAT + RA_FI + RE) / CRD`  (STAT+FI+RE)"
        else "`RA = -(RA_STAT) / CRD`  (stat-only; FI/RE excluded)"
      sb.append(s"- Central loss-rate rule: $ruleTxt\n\n")

      // Baseline inputs + Q1 aggregation
      sb.append("**Baseline inputs (first 3 months) and Q1 aggregation**\n\n")
      sb.append("| Metric | M1 | M2 | M3 | Q1 |\n|---|---|---|---|---|\n")
      sb.append(s"| CRD | ${month(head.crdMonths, 0)} | ${month(head.crdMonths, 1)} | ${month(head.crdMonths, 2)} | ${dot(head.crdQ1)} |\n")
      sb.append(s"| RA STAT | ${month(head.statMonths, 0)} | ${month(head.statMonths, 1)} | ${month(head.statMonths, 2)} | ${dot(head.statQ1)} |\n")
      sb.append(s"| RA FI | ${month(head.fiMonths, 0)} | ${month(head.fiMonths, 1)} | ${month(head.fiMonths, 2)} | ${dot(head.fiQ1)} |\n")
      sb.append(s"| RE | ${month(head.reMonths, 0)} | ${month(head.reMonths, 1)} | ${month(head.reMonths, 2)} | ${dot(head.reQ1)} |\n\n")

      // Central worked numbers
      val central = group.find(_.scenarioName == PrimaryConstants.SCENARIO_CENTRAL).getOrElse(head)
      val numerator = if (head.fwlApplied) head.statQ1 + head.fiQ1 + head.reQ1 else head.statQ1
      val numTxt = if (head.fwlApplied)
        s"${dot(head.statQ1)} + ${dot(head.fiQ1)} + ${dot(head.reQ1)} = ${dot(numerator)}"
      else s"${dot(head.statQ1)}"
      sb.append("**Central (no shock):**\n\n```\n")
      sb.append(s"numerator           = $numTxt\n")
      sb.append(s"RA(Term 0)          = -(${dot(numerator)}) / ${dot(head.crdQ1)} = ${dot(central.ra0, 8)}\n")
      sb.append(s"VECTOR(Term 0)      = 1 - ${dot(central.ra0, 8)} = ${dot(central.vector0, 8)}\n")
      sb.append(s"EAD_RA_RATE(Term 0) = ${dot(central.ead0, 8)}\n```\n\n")

      // Per-scenario term-0 result table
      sb.append("**Term-0 result per scenario**\n\n")
      sb.append("| Scenario | delta(Q1) | RA(Term 0) | VECTOR | EAD_RA_RATE(Term 0) |\n")
      sb.append("|---|---|---|---|---|\n")
      // order scenarios by the canonical SCENARIO_CODES sequence
      val order = PrimaryConstants.SCENARIO_CODES.map(_._1).zipWithIndex.toMap
      group.sortBy(r => order.getOrElse(r.scenarioName, Int.MaxValue)).foreach { r =>
        val deltaTxt = if (r.usesShock) dot(r.delta0, 6) else "—"
        sb.append(s"| ${r.scenarioName} (${r.scenarioCode}) | $deltaTxt | ${dot(r.ra0, 8)} | ${dot(r.vector0, 8)} | ${dot(r.ead0, 8)} |\n")
      }
      sb.append("\n")

      // Shock-leg detail for FWL=YES matrices
      val shocked = group.filter(_.usesShock)
      if (shocked.nonEmpty) {
        sb.append("**Shock detail (FWL=YES non-Central — stress leg Q1 values)**\n\n")
        sb.append("| Scenario | stress leg | CRD_Q1 | RA_FI_Q1 | RE_Q1 | delta(Q1) |\n")
        sb.append("|---|---|---|---|---|---|\n")
        shocked.foreach { r =>
          val leg = if (r.scenarioName == PrimaryConstants.SCENARIO_OPTIMISTIC) "STRESS (+)" else "STRESS (-)"
          sb.append(s"| ${r.scenarioName} (${r.scenarioCode}) | $leg | ${dot(r.legCrdQ1)} | ${dot(r.legFiQ1)} | ${dot(r.legReQ1)} | ${dot(r.delta0, 6)} |\n")
        }
        sb.append("\n")
      }
      sb.append("---\n\n")
    }
    sb.toString()
  }
}
