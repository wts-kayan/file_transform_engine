package com.bnp.str.tseadfwd.coherence

/**
 * Renders a [[CheckReport]] as ONE self-contained HTML document.
 *
 * Self-contained on purpose: the file is handed to the business team by mail or dropped on a share,
 * where an external stylesheet or a CDN font would simply not resolve. Everything (layout, colours,
 * the lot) is inline, and the page opens the same from a local disk, a network share or HDFS.
 *
 * Deliberately light: one small stylesheet, no script, no image, no web font, and plain ASCII
 * wording so the file survives any mail gateway or terminal that re-encodes it.
 *
 * Pure `String` in, `String` out: no Spark, no IO, so it is unit-tested directly.
 */
object CheckHtmlView {

  /** HTML-escape a cell. Matrix ids and details are engine-generated, but never trust them blindly. */
  private def esc(s: String): String =
    if (s == null) ""
    else s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;")

  /** CSS class carrying the status colour. */
  private def statusClass(status: String): String = status match {
    case "PASS"    => "ok"
    case "SKIPPED" => "muted"
    case _         => "bad"
  }

  private def badge(text: String): String =
    s"""<span class="badge ${statusClass(text)}">${esc(text)}</span>"""

  /** One table cell. `raw` is for the few cells that carry markup we built ourselves (badges). */
  private case class Cell(text: String, cls: String = "", raw: Boolean = false)

  private def renderTable(headers: Seq[(String, String)], rows: Seq[Seq[Cell]]): String = {
    def attr(cls: String): String = if (cls.isEmpty) "" else s""" class="$cls""""
    val head = headers.map { case (h, c) => s"<th${attr(c)}>${esc(h)}</th>" }
      .mkString("        <tr>", "", "</tr>\n")
    val body = rows.map(_.map(c => s"<td${attr(c.cls)}>${if (c.raw) c.text else esc(c.text)}</td>")
      .mkString("        <tr>", "", "</tr>\n")).mkString
    s"""    <div class="scroll">
       |      <table>
       |        <thead>
       |$head        </thead>
       |        <tbody>
       |$body        </tbody>
       |      </table>
       |    </div>
       |""".stripMargin
  }

  /** One rule section: what the rule states, what was done about it, then the findings. */
  private def ruleSection(r: CheckRuleResult): String = {
    val header =
      s"""    <section class="rule ${statusClass(r.status)}">
         |      <h2><span class="rid">${esc(r.rule.id)}</span>${esc(r.rule.title)} ${badge(r.status)}</h2>
         |      <p class="what">${esc(r.rule.detail)}</p>
         |      <p class="act"><b>Action:</b> ${esc(r.action)}</p>
         |""".stripMargin

    val body =
      if (!r.enabled) """      <p class="none">Rule disabled in the configuration, not evaluated.</p>"""
      else if (r.total == 0L) """      <p class="none">No line matched this rule.</p>"""
      else {
        val groupLevel = r.rule == CheckRule.AllTermsEqualOne
        val headers =
          if (groupLevel) Seq(("EAD_MATRIX_ID", ""), ("SCENARIO_ID", ""), ("EAD_RA_RATE", "num"), ("Detail", ""))
          else Seq(("EAD_MATRIX_ID", ""), ("SCENARIO_ID", ""), ("TERM", "num"), ("EAD_RA_RATE", "num"), ("Detail", ""))
        val rows = r.findings.map { f =>
          val key = Seq(Cell(f.matrixId, "mono"), Cell(f.scenarioId, "mono"))
          val values =
            if (groupLevel) Seq(Cell(f.value, "num mono"))
            else Seq(Cell(f.term, "num mono"), Cell(f.value, "num mono"))
          key ++ values :+ Cell(f.detail)
        }
        val note =
          if (r.truncated)
            s"""      <p class="note">Showing the first ${r.findings.size} of ${r.total} finding(s).
               |        Raise <code>rules.negative_ead_ra_rate.maxRowsInReport</code> to list more.</p>
               |""".stripMargin
          else ""
        renderTable(headers, rows) + note
      }

    header + body + "\n    </section>\n"
  }

  /** The full HTML document for one report. */
  def render(report: CheckReport): String = {
    val summaryRows = report.results.map(r => Seq(
      Cell(r.rule.id, "mono"),
      Cell(r.rule.title),
      Cell(badge(r.status), raw = true),
      Cell(r.total.toString, "num"),
      Cell(r.action)))

    val kept = report.rowsOut
    val removed = report.rowsIn - report.rowsOut

    // The file the rules were run on. Named twice on purpose: the NAME next to the title, because
    // that is what a reader checks first when several vintages of the same table sit in one
    // directory, and the FULL PATH below, because that is what identifies it without ambiguity.
    // Omitted entirely when unknown — a blank "Output file" line would read as a missing output.
    val fileTitle =
      if (report.outputFileName.isEmpty) ""
      else s"""<span class="file">${esc(report.outputFileName)}</span> - """
    val fileRow =
      if (report.outputFile.isEmpty) ""
      else s"""      <dt>Output file</dt><dd class="mono">${esc(report.outputFile)}</dd>\n"""
    // Browser tab and print header: the file name distinguishes two reports open side by side,
    // which "Business coherence check: TS_EAD_FWD" alone does not.
    val docTitle =
      if (report.outputFileName.isEmpty) "Business coherence check: TS_EAD_FWD"
      else s"Business coherence check: ${report.outputFileName}"

    s"""<!DOCTYPE html>
       |<html lang="en">
       |<head>
       |<meta charset="utf-8"/>
       |<meta name="viewport" content="width=device-width, initial-scale=1"/>
       |<title>${esc(docTitle)}</title>
       |<style>
       |  :root { --line:#dfe3e8; --ink:#1c1c1c; --dim:#5b6470; --head:#f4f6f8; }
       |  * { box-sizing: border-box; }
       |  body { font-family: Segoe UI, Arial, sans-serif; color: var(--ink); margin: 0;
       |         padding: 2rem 1.5rem; line-height: 1.45; }
       |  main { max-width: 70rem; margin: 0 auto; }
       |  h1 { font-size: 1.45rem; margin: 0 0 .2rem; }
       |  h2 { font-size: 1rem; margin: 0 0 .4rem; }
       |  .sub { color: var(--dim); margin: 0 0 1.4rem; font-size: .9rem; }
       |  .sub .file { font-family: Consolas, monospace; color: var(--ink); }
       |  .stats { display: flex; flex-wrap: wrap; gap: .6rem; margin: 0 0 1.4rem; padding: 0; list-style: none; }
       |  .stats li { border: 1px solid var(--line); border-radius: .4rem; padding: .45rem .9rem; min-width: 8rem; }
       |  .stats b { display: block; font-size: 1.15rem; font-variant-numeric: tabular-nums; }
       |  .stats span { color: var(--dim); font-size: .75rem; text-transform: uppercase; letter-spacing: .04em; }
       |  dl { display: grid; grid-template-columns: max-content 1fr; gap: .25rem 1rem;
       |       font-size: .85rem; margin: 0 0 1.8rem; }
       |  dt { color: var(--dim); } dd { margin: 0; word-break: break-all; }
       |  .scroll { overflow-x: auto; margin: .5rem 0 .3rem; }
       |  table { border-collapse: collapse; font-size: .85rem; width: 100%; }
       |  th, td { border-bottom: 1px solid var(--line); padding: .4rem .8rem; text-align: left;
       |           white-space: nowrap; }
       |  th { background: var(--head); font-weight: 600; }
       |  td:last-child, th:last-child { white-space: normal; }
       |  tbody tr:hover { background: #f8f9fb; }
       |  .num { text-align: right; font-variant-numeric: tabular-nums; }
       |  .mono, code { font-family: Consolas, monospace; }
       |  .badge { font-size: .7rem; padding: .1rem .5rem; border-radius: .8rem; vertical-align: middle;
       |           letter-spacing: .03em; }
       |  .ok    { background:#e6f4ea; color:#14622b; } .bad { background:#fdecea; color:#a4262c; }
       |  .muted { background:#eceff1; color:#4a545e; }
       |  section.rule { border-left: 3px solid var(--line); padding: .1rem 0 .1rem 1rem; margin: 1.8rem 0; }
       |  section.ok { border-left-color:#8fce9f; } section.bad { border-left-color:#e69a9f; }
       |  .rid { font-family: Consolas, monospace; color: var(--dim); margin-right: .5rem; }
       |  .what, .act, .note, .none { font-size: .85rem; color: #3d444c; margin: .25rem 0; }
       |  .none { font-style: italic; color: var(--dim); }
       |  .foot { color: var(--dim); font-size: .75rem; border-top: 1px solid var(--line);
       |          margin-top: 2.5rem; padding-top: .6rem; }
       |  code { background:#f2f3f5; padding: 0 .2rem; border-radius: .2rem; }
       |  @media print { body { padding: 0; } tbody tr:hover { background: none; } }
       |</style>
       |</head>
       |<body>
       |  <main>
       |    <h1>Business coherence check: TS_EAD_FWD ${badge(report.verdict)}</h1>
       |    <p class="sub">$fileTitle${report.totalFindings} finding(s) over ${report.rowsIn} output line(s).</p>
       |
       |    <ul class="stats">
       |      <li><b>${report.rowsIn}</b><span>Lines examined</span></li>
       |      <li><b>$kept</b><span>Lines kept</span></li>
       |      <li><b>$removed</b><span>Lines removed</span></li>
       |      <li><b>${report.totalFindings}</b><span>Findings</span></li>
       |    </ul>
       |
       |    <dl>
       |      <dt>Source</dt><dd>${esc(report.source)}</dd>
       |$fileRow      <dt>Run id</dt><dd class="mono">${esc(report.runId)}</dd>
       |      <dt>Generated</dt><dd>${esc(report.generatedAt)}</dd>
       |      <dt>Lines</dt><dd>${report.rowsIn} examined, $kept kept ($removed removed)</dd>
       |    </dl>
       |
       |    <h2>Summary</h2>
       |${renderTable(Seq(("Rule", "mono"), ("Title", ""), ("Status", ""), ("Findings", "num"), ("Action", "")), summaryRows)}
       |${report.results.map(ruleSection).mkString("\n")}
       |    <p class="foot">IRIS / tseadfwd, business coherence check on the TS_EAD_FWD term structure.</p>
       |  </main>
       |</body>
       |</html>
       |""".stripMargin
  }
}
