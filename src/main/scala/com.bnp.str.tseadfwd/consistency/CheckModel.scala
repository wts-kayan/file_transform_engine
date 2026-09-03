package com.bnp.str.tseadfwd.consistency

/**
 * Value model of the TS_EAD_FWD business consistency-check report.
 *
 * Pure data — no Spark, no IO — so the rules ([[ConsistencyCheckMapper]]) and the rendering
 * ([[CheckHtmlView]]) can be unit-tested independently of each other.
 */

/**
 * A business rule requested by the business team.
 *
 * @param id      stable identifier used in the report and in logs (CR01, CR02, ...)
 * @param title   short human label
 * @param detail  what the rule checks, in the business team's own terms
 * @param removes true when a hit REMOVES rows from the output — the Main job performs the removal;
 *                the consistency-check side only names the rows. False = reporting only.
 */
final case class CheckRule(id: String, title: String, detail: String, removes: Boolean)

object CheckRule {

  /**
   * CR01 — for one (EAD_MATRIX_ID, SCENARIO_ID), when EVERY term carries `EAD_RA_RATE = 1` the whole
   * curve is full exposure: no loss ever accrues, so the line says nothing. The business asked for
   * those lines to be removed from the output and listed in the report.
   */
  val AllTermsEqualOne = CheckRule(
    id = "CR01",
    title = "All terms equal to 1, by EAD_MATRIX_ID and SCENARIO_ID",
    detail = "Lines are grouped by EAD_MATRIX_ID and SCENARIO_ID; one group is one curve. " +
      "The group is flagged when EVERY one of its terms carries EAD_RA_RATE = 1: the exposure is " +
      "full at every term, no loss ever accrues, so the line carries no information. A single term " +
      "below 1 is enough to keep the whole group. Note EAD_MATRIX_ID ends in _Q or _Y, so the " +
      "quarterly and yearly curves of the same matrix are two separate groups.",
    removes = true)

  /**
   * CR02 — a negative `EAD_RA_RATE` is non-physical (an exposure factor lies in [0, 1]); a zero one
   * is possible but means the exposure has fully run off. Reported, never removed: the business
   * wants to see both, not to have them silently disappear.
   *
   * The text has to describe the rule in BOTH scopes, because `includeZero` decides which one runs
   * and this value is a stable constant — results are matched on it by identity.
   */
  val NegativeEadRaRate = CheckRule(
    id = "CR02",
    title = "Negative or zero EAD_RA_RATE",
    detail = "EAD_RA_RATE is strictly negative, which is not a possible exposure factor (it must " +
      "lie between 0 and 1). With rules.negative_ead_ra_rate.includeZero = true the rule ALSO " +
      "fires on exactly 0: a term where the exposure has fully run off. With includeZero unset " +
      "(the default) a zero does not fire and only strictly negative values are listed. Either " +
      "way the line is KEPT and, by default, so is the value as computed: a negative rate can only " +
      "appear once allow_negative_ead_ra_rate has been switched on, and the point of switching it " +
      "on is to see the real number. Configure a marker (replaceWith) to have the value written as " +
      "a token instead, for a consumer that cannot take a negative. The report gives a SUMMARY " +
      "only - how many lines carry a zero and how many carry a negative - never the lines " +
      "themselves: those two counts are what the business reads, and a zero rate is ordinary " +
      "enough to run to thousands of lines. The Findings count is the number of LINES affected.",
    removes = false)

  /**
   * CR03 — the mirror of CR01. For one (EAD_MATRIX_ID, SCENARIO_ID), SOME terms carry
   * `EAD_RA_RATE = 1` and others do not: the exposure is full over part of the curve and running
   * off over the rest. That is a possible shape rather than an impossible one, so nothing is
   * removed, but the business asked for those curves to be named — a term still at full exposure
   * where the curve has otherwise started to amortise is worth a look.
   *
   * Disjoint from CR01 by construction: CR01 needs EVERY term equal to 1, CR03 needs at least one
   * and not all. A curve with no term equal to 1 fires neither rule.
   */
  val SomeTermsEqualOne = CheckRule(
    id = "CR03",
    title = "Some terms equal to 1, but not the whole curve",
    detail = "Lines are grouped by EAD_MATRIX_ID and SCENARIO_ID; one group is one curve. The " +
      "group is flagged when AT LEAST ONE of its terms carries EAD_RA_RATE = 1 and at least one " +
      "does not: full exposure over part of the curve only. A curve whose every term equals 1 " +
      "belongs to CR01 and is not repeated here; a curve with no term equal to 1 is not flagged " +
      "at all. The line is KEPT - this rule only names the curve for review. One line is reported " +
      "per curve, with how many of its terms equal 1, not one line per term.",
    removes = false)

  /** Every rule, in report order. */
  val All: Seq[CheckRule] = Seq(AllTermsEqualOne, NegativeEadRaRate, SomeTermsEqualOne)
}

/**
 * One offending item.
 *
 * Not necessarily one output row: `term` is filled only when the finding names one exact row.
 *  - group-level rules (CR01, CR03): `term` empty, the two keys name the curve, `value` describes it
 *  - summary lines (CR02): `term` empty, both keys are "-", and `detail` counts the rows behind it
 */
final case class CheckFinding(
                            matrixId: String,
                            scenarioId: String,
                            term: String,
                            value: String,
                            detail: String
                          )

/**
 * Outcome of one rule.
 *
 * @param rule      the rule evaluated
 * @param enabled   false when switched off in the conf — reported as SKIPPED, not as PASS
 * @param total     total number of findings (may exceed `findings.size`, which the report caps)
 * @param findings  the findings actually listed in the report
 * @param rowsRemoved rows the Main job removed because of this rule (0 when the rule only reports)
 * @param applied   true when the removal was actually applied (rule removes AND remove = true)
 * @param reportOnly true for a standalone reporting run, where removal is simply not this job's
 *                   business — distinct from a run where the removal was switched off
 * @param valuesReplaced values rewritten to [[marker]] in the output (the line itself is kept)
 * @param marker    the token written in place of the offending value, empty when none is configured
 * @param summarised true when `findings` SUMMARISES `total` instead of listing a prefix of it — CR02
 *                   reports one counted line per kind of hit, whatever the number of rows behind it.
 *                   Such a result is never [[truncated]]: there is no longer listing to ask for, so
 *                   a "showing the first 2 of 1247" note would point the reader at a setting that
 *                   would change nothing.
 */
final case class CheckRuleResult(
                               rule: CheckRule,
                               enabled: Boolean,
                               total: Long,
                               findings: Seq[CheckFinding],
                               rowsRemoved: Long,
                               applied: Boolean,
                               reportOnly: Boolean = false,
                               valuesReplaced: Long = 0L,
                               marker: String = "",
                               summarised: Boolean = false
                             ) {

  /** True when findings were listed but the report shows only a prefix of them. */
  def truncated: Boolean = !summarised && total > findings.size

  /** PASS (nothing found) / SKIPPED (disabled) / REMOVED (found and dropped) / REPORTED (found, kept). */
  def status: String =
    if (!enabled) "SKIPPED"
    else if (total == 0L) "PASS"
    else if (applied) "REMOVED"
    else "REPORTED"

  /**
   * What happened to the offending rows, for the report's Action column.
   *
   * The "nothing found" case comes FIRST: with no finding there is nothing to act on, and any other
   * wording would describe the configuration rather than the outcome — a rule that passes must not
   * read as though its removal had been switched off.
   */
  def action: String =
    if (!enabled) "-"
    else if (total == 0L) "-"
    else if (!rule.removes && marker.nonEmpty)
      s"line(s) kept, $valuesReplaced value(s) written as $marker in the output"
    else if (!rule.removes) "kept (reporting only)"
    else if (applied) s"$rowsRemoved row(s) removed from the output"
    else if (reportOnly) "not applicable: this run only reports, the main job removes these lines"
    else "kept (removal disabled in the configuration)"
}

/**
 * The consolidated report for one run.
 *
 * @param source      what was inspected (the output table, or the CSV a standalone run read)
 * @param runId       run_history run id, so a report can be tied back to its execution
 * @param generatedAt render timestamp
 * @param rowsIn      rows examined, before any removal
 * @param rowsOut     rows written after removal (equals `rowsIn` for a report-only run)
 * @param results     one entry per rule, in [[CheckRule.All]] order
 * @param outputFile  the term-structure FILE these rules were run on — the file the main job writes,
 *                    or the CSV a standalone run read. A report is read next to the data it judges,
 *                    so it has to name that data unambiguously (several vintages of the same table
 *                    live side by side in one output directory). Empty when unknown: the report then
 *                    simply omits the line rather than showing a blank one.
 */
final case class CheckReport(
                           source: String,
                           runId: String,
                           generatedAt: String,
                           rowsIn: Long,
                           rowsOut: Long,
                           results: Seq[CheckRuleResult],
                           outputFile: String = ""
                         ) {

  /** Last path segment of [[outputFile]] — the file NAME, for the header line. Empty when unknown. */
  def outputFileName: String = {
    val cut = outputFile.lastIndexOf('/') max outputFile.lastIndexOf('\\')
    if (cut >= 0) outputFile.substring(cut + 1) else outputFile
  }

  /** Total findings across every rule. */
  def totalFindings: Long = results.map(_.total).sum

  /** Overall verdict shown in the report header. */
  def verdict: String = if (totalFindings == 0L) "PASS" else "FINDINGS"

  /** The (matrix, scenario) keys CR01 asks the Main job to remove; empty when the rule is off. */
  def removalKeys: Seq[(String, String)] =
    results
      .filter(r => r.rule == CheckRule.AllTermsEqualOne && r.enabled)
      .flatMap(_.findings.map(f => (f.matrixId, f.scenarioId)))

  /** One-line summary for the run log — named by the file it judges, when that file is known. */
  def summaryLine: String = {
    val on = if (outputFileName.isEmpty) "" else s" on $outputFileName"
    s"CONSISTENCY CHECK$on - $verdict ($rowsIn row(s) in, $rowsOut out): " +
      results.map(r => s"${r.rule.id} ${r.status}(${r.total})").mkString(", ")
  }
}
