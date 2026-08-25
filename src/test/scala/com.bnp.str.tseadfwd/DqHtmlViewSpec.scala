package com.bnp.str.tseadfwd

import com.bnp.str.tseadfwd.dataquality._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for the HTML rendering of the data-quality report (pure String, no Spark).
 *
 * Run (offline, via the ScalaTest runner on the test classpath):
 *   java -cp "target/classes;target/test-classes;$(cat cp.txt)" \
 *        org.scalatest.tools.Runner -o -s com.bnp.str.tseadfwd.DqHtmlViewSpec
 */
class DqHtmlViewSpec extends AnyFunSuite with Matchers {

  private def report(results: Seq[DqRuleResult], rowsIn: Long = 10L, rowsOut: Long = 8L,
                     outputFile: String = "") =
    DqReport("TS_EAD_FWD", "run-123", "2026-07-31 10:00:00", rowsIn, rowsOut, results, outputFile)

  private val r01Hit = DqRuleResult(
    rule = DqRule.AllTermsEqualOne,
    enabled = true,
    total = 1L,
    findings = Seq(DqFinding("BCEF_CONSO_TF_Q", "C", "", "1", "all 3 term(s) equal 1")),
    rowsRemoved = 3L,
    applied = true)

  private val r02Clean = DqRuleResult(DqRule.NegativeEadRaRate, enabled = true, 0L, Seq.empty, 0L, applied = false)

  test("the report is one self-contained document — no external stylesheet, script or image") {
    val html = DqHtmlView.render(report(Seq(r01Hit, r02Clean)))

    html should startWith("<!DOCTYPE html>")
    html should include("</html>")
    html should include("<style>")
    html should not include "<link"
    html should not include "<script"
    html should not include "http://"
    html should not include "https://"
  }

  test("a finding is listed with its keys, and the removal is stated") {
    val html = DqHtmlView.render(report(Seq(r01Hit, r02Clean)))

    html should include("BCEF_CONSO_TF_Q")
    html should include("R01")
    html should include("REMOVED")
    html should include("3 row(s) removed from the output")
    html should include("run-123")
    html should include("10 examined, 8 kept (2 removed)")
  }

  test("a rule with no finding renders as PASS with an explicit note") {
    val html = DqHtmlView.render(report(Seq(r02Clean), rowsIn = 4L, rowsOut = 4L))

    html should include("PASS")
    html should include("No line matched this rule.")
  }

  test("a disabled rule is SKIPPED, not PASS") {
    val skipped = DqRuleResult(DqRule.NegativeEadRaRate, enabled = false, 0L, Seq.empty, 0L, applied = false)
    val html = DqHtmlView.render(report(Seq(skipped), rowsIn = 4L, rowsOut = 4L))

    html should include("SKIPPED")
    html should include("Rule disabled in the configuration")
  }

  test("the wording is plain ASCII, so no gateway or terminal can mangle it") {
    val html = DqHtmlView.render(report(Seq(r01Hit, r02Clean)))
    val offending = html.filter(_ > 127.toChar).distinct

    withClue(s"non-ASCII character(s) found: ${offending.map(c => f"U+${c.toInt}%04X").mkString(", ")} ") {
      offending shouldBe ""
    }
  }

  test("the page stays light: no script, no image, no web font, one stylesheet") {
    val html = DqHtmlView.render(report(Seq(r01Hit, r02Clean)))

    html.toLowerCase should not include "<script"
    html.toLowerCase should not include "<img"
    html.toLowerCase should not include "@font-face"
    html.toLowerCase should not include "url("
    html.sliding("<style>".length).count(_ == "<style>") shouldBe 1
    html.length should be < 8000 // a mail-friendly page, not a bundle
  }

  test("a truncated listing says so") {
    val many = (1 to 3).map(i => DqFinding("M", "C", s"$i", s"-0,$i", "negative exposure factor"))
    val r02 = DqRuleResult(DqRule.NegativeEadRaRate, enabled = true, 42L, many, 0L, applied = false)
    val html = DqHtmlView.render(report(Seq(r02)))

    html should include("Showing the first 3 of 42 finding(s)")
  }

  test("cell content is HTML-escaped") {
    val nasty = DqRuleResult(
      rule = DqRule.NegativeEadRaRate,
      enabled = true,
      total = 1L,
      findings = Seq(DqFinding("<script>alert(1)</script>", "C & A", "0", "-1", "\"quoted\"")),
      rowsRemoved = 0L,
      applied = false)
    val html = DqHtmlView.render(report(Seq(nasty)))

    html should include("&lt;script&gt;")
    html should not include "<script>alert"
    html should include("C &amp; A")
    html should include("&quot;quoted&quot;")
  }

  test("the report names the output file it was run on — full path and file name") {
    val html = DqHtmlView.render(report(Seq(r01Hit, r02Clean),
      outputFile = "hdfs:///user/tseadfwd/output/TS_EAD_FWD_25Q4_v1.csv"))

    html should include("<dt>Output file</dt>")
    html should include("hdfs:///user/tseadfwd/output/TS_EAD_FWD_25Q4_v1.csv")
    // the file NAME alone, next to the title and in the browser tab, where the path would not fit
    html should include("<title>Data quality: TS_EAD_FWD_25Q4_v1.csv</title>")
    html should include("""<span class="file">TS_EAD_FWD_25Q4_v1.csv</span>""")
  }

  test("an unknown output file leaves no blank line behind") {
    val html = DqHtmlView.render(report(Seq(r02Clean), rowsIn = 4L, rowsOut = 4L))

    html should not include "Output file"
    html should not include """<span class="file">"""
    html should include("<title>Data quality: TS_EAD_FWD</title>")
  }

  test("the output file name is HTML-escaped like every other cell") {
    val html = DqHtmlView.render(report(Seq(r02Clean), outputFile = "out/<TS>&.csv"))

    html should include("&lt;TS&gt;&amp;.csv")
    html should not include "<TS>"
  }

  test("the verdict follows the total findings") {
    DqHtmlView.render(report(Seq(r02Clean), 4L, 4L)) should include("""<span class="badge ok">PASS</span>""")
    DqHtmlView.render(report(Seq(r01Hit))) should include("""<span class="badge bad">FINDINGS</span>""")
  }
}
