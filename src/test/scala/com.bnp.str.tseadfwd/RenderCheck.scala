package com.bnp.str.tseadfwd

import com.bnp.str.tseadfwd.job.Term0AnalysisDriver
import com.bnp.str.tseadfwd.mapping.Term0RowView

/**
 * Spark-free render check for [[Term0AnalysisDriver.renderMarkdown]] — prints the Markdown for a
 * no-shock (Central) and a shocked (Adverse) scenario so the new per-term "worked computation steps"
 * block can be eyeballed. renderMarkdown is pure, so this needs no SparkSession.
 *
 * Run: mvn exec:java -Dexec.mainClass=com.bnp.str.tseadfwd.RenderCheck -Dexec.classpathScope=test
 */
object RenderCheck {

  private def det(x: Double, c: Double): Double = if (c == 0.0) 0.0 else -x / c

  /** Build one self-consistent row: RA/VECTOR/EAD are derived exactly as the engine would. */
  private def row(scen: String, code: String, shock: Boolean, term: Double, period: Int,
                  crd: Double, stat: Double, fi: Double, re: Double,
                  legCrd: Double, legFi: Double, legRe: Double, delta: Double,
                  prevEad: Double): Term0RowView = {
    val ra =
      if (crd == 0.0) 0.0
      else if (shock) {
        val statDet     = det(stat, crd)
        val fireBaseDet = det(fi, crd) + det(re, crd)
        val shockFi     = det(legFi, legCrd) - det(fi, crd)
        val shockRe     = det(legRe, legCrd) - det(re, crd)
        statDet + fireBaseDet - (shockFi + shockRe) * delta
      } else det(stat + fi + re, crd)
    val vector = 1.0 - ra
    val ead    = prevEad * vector
    Term0RowView(
      matrixId = "BCEF_MORTGAGE_TF_Q", scenarioName = scen, scenarioCode = code,
      fwlApplied = true, macroVar = "IR_10Y_FR", usesShock = shock,
      segments = Seq("MORTGAGE"), rateType = "TF", term = term, period = period,
      crdMonths = Seq(-90, -90, -90), statMonths = Seq(-0.6, -0.6, -0.6),
      fiMonths = Seq(-0.05, -0.05, -0.05), reMonths = Seq(-0.006, -0.006, -0.006),
      crdAgg = crd, statAgg = stat, fiAgg = fi, reAgg = re,
      legCrdAgg = legCrd, legFiAgg = legFi, legReAgg = legRe,
      delta = delta, ra = ra, vector = vector, ead = ead)
  }

  def main(args: Array[String]): Unit = {
    // Central (no shock) — three consecutive quarters, so EAD chains as prevEad * VECTOR.
    val c1 = row("Central", "C", shock = false, 0.0,  1, 100, -0.6,  -0.05, -0.006, 0, 0, 0, 0, 1.0)
    val c2 = row("Central", "C", shock = false, 0.25, 2, 100, -1.25, -0.05, -0.006, 0, 0, 0, 0, c1.ead)
    val c3 = row("Central", "C", shock = false, 0.5,  3, 100, -1.25, -0.05, -0.006, 0, 0, 0, 0, c2.ead)

    // Adverse (FWL=YES, shocked) — exercises the stat/fire-base/shock decomposition branch.
    val a1 = row("Adverse", "A", shock = true, 0.0,  1, 100, -0.6,  -0.05, -0.006, 110, -0.09, -0.02, 0.0042, 1.0)
    val a2 = row("Adverse", "A", shock = true, 0.25, 2, 100, -1.25, -0.05, -0.006, 110, -0.09, -0.02, 0.0042, a1.ead)

    println(Term0AnalysisDriver.renderMarkdown(Seq(c1, c2, c3, a1, a2)))
  }
}
