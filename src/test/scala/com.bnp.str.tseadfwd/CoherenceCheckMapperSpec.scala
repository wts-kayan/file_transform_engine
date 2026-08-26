package com.bnp.str.tseadfwd

import com.bnp.str.tseadfwd.coherence.{CheckConfig, CheckRule, CoherenceCheckMapper}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for the business coherence-check rules on the TS_EAD_FWD output.
 *
 * Values are built exactly as the engine emits them — decimal-comma STRINGS, trailing zeros
 * stripped, so a full-exposure term is the literal "1" — because the rules have to parse them.
 *
 * Run (offline, via the ScalaTest runner on the test classpath):
 *   mvn -o dependency:build-classpath -Dmdep.outputFile=cp.txt -DincludeScope=test
 *   java -cp "target/classes;target/test-classes;$(cat cp.txt)" \
 *        org.scalatest.tools.Runner -o -s com.bnp.str.tseadfwd.CoherenceCheckMapperSpec
 */
class CoherenceCheckMapperSpec extends AnyFunSuite with Matchers with SparkTestSession {

  private val baseConf = CheckConfig(
    enabled = true,
    htmlPath = "target/test-checks.html",
    sourcePath = "unused",
    allTermsEqualOneEnabled = true,
    allTermsEqualOneRemoves = true,
    tolerance = 1e-9,
    negativeEnabled = true,
    negativeIncludesZero = false,
    maxRowsInReport = 500,
    negativeMarker = "NV",
    excludeEadRaRateGe1 = false)

  /** (EAD_MATRIX_ID, SCENARIO_ID, TERM, EAD_RA_RATE, EAD_CCF_RATE) as the mapper emits them. */
  private def output(rows: (String, String, String, String)*): DataFrame = {
    import spark.implicits._
    rows.map { case (m, s, t, r) => (m, s, t, r, "") }
      .toDF("EAD_MATRIX_ID", "SCENARIO_ID", "TERM", "EAD_RA_RATE", "EAD_CCF_RATE")
  }

  private def run(df: DataFrame, conf: CheckConfig = baseConf) =
    new CoherenceCheckMapper(conf)(spark).apply(df, source = "TS_EAD_FWD", runId = "test-run")

  private def result(outcome: com.bnp.str.tseadfwd.coherence.CheckOutcome, rule: CheckRule) =
    outcome.report.results.find(_.rule == rule).get

  // ---- CR01: all terms equal to 1 -------------------------------------------

  test("CR01 removes a (matrix, scenario) whose every term equals 1, and reports it") {
    val df = output(
      ("BCEF_CONSO_TF_Q", "C", "0", "1"),
      ("BCEF_CONSO_TF_Q", "C", "0,25", "1"),
      ("BCEF_CONSO_TF_Q", "C", "0,5", "1"),
      ("BCEF_CONSO_TF_Q", "A", "0", "1"),
      ("BCEF_CONSO_TF_Q", "A", "0,25", "0,98"))

    val outcome = run(df)
    val r01 = result(outcome, CheckRule.AllTermsEqualOne)

    r01.total shouldBe 1L
    r01.findings.head.matrixId shouldBe "BCEF_CONSO_TF_Q"
    r01.findings.head.scenarioId shouldBe "C"
    r01.findings.head.detail should include("all 3 term(s) equal 1")
    r01.status shouldBe "REMOVED"
    r01.rowsRemoved shouldBe 3L

    // the Adverse curve is untouched — one term below 1 is enough to keep the whole line
    outcome.cleaned.count() shouldBe 2L
    outcome.cleaned.select("SCENARIO_ID").distinct().collect().map(_.getString(0)) shouldBe Array("A")
    outcome.report.rowsIn shouldBe 5L
    outcome.report.rowsOut shouldBe 2L
  }

  test("CR01 does not fire when a single term deviates from 1") {
    val df = output(
      ("BGL_MORTGAGE_Y", "E", "0", "1"),
      ("BGL_MORTGAGE_Y", "E", "1", "1"),
      ("BGL_MORTGAGE_Y", "E", "2", "0,999999"))

    val outcome = run(df)
    result(outcome, CheckRule.AllTermsEqualOne).total shouldBe 0L
    result(outcome, CheckRule.AllTermsEqualOne).status shouldBe "PASS"
    // nothing found is not the same as removal switched off — the action must not blame the conf
    result(outcome, CheckRule.AllTermsEqualOne).action shouldBe "-"
    outcome.cleaned.count() shouldBe 3L
  }

  test("a rule that passes with remove = true never reports the removal as disabled") {
    val clean = output(("BCEF_CONSO_TF_Q", "C", "0", "0,99"))

    val passing = result(run(clean), CheckRule.AllTermsEqualOne)
    passing.status shouldBe "PASS"
    passing.action should not include "disabled"

    // the wording is reserved for the case it actually describes
    val hit = output(("BCEF_CONSO_TF_Q", "C", "0", "1"))
    result(run(hit, baseConf.copy(allTermsEqualOneRemoves = false)), CheckRule.AllTermsEqualOne)
      .action shouldBe "kept (removal disabled in the configuration)"
  }

  test("CR01 groups on the matrix id, so the quarterly and yearly curves are separate lines") {
    val df = output(
      ("BNL_CONSO_TF_Q", "C", "0", "1"),
      ("BNL_CONSO_TF_Q", "C", "0,25", "1"),
      ("BNL_CONSO_TF_Y", "C", "0", "0,95"))

    val outcome = run(df)
    val r01 = result(outcome, CheckRule.AllTermsEqualOne)

    r01.total shouldBe 1L
    r01.findings.head.matrixId shouldBe "BNL_CONSO_TF_Q"
    outcome.cleaned.count() shouldBe 1L
  }

  test("CR01 with remove = false reports the line but leaves it in the output") {
    val df = output(
      ("BCEF_CONSO_TF_Q", "C", "0", "1"),
      ("BCEF_CONSO_TF_Q", "C", "0,25", "1"))

    val outcome = run(df, baseConf.copy(allTermsEqualOneRemoves = false))
    val r01 = result(outcome, CheckRule.AllTermsEqualOne)

    r01.total shouldBe 1L
    r01.status shouldBe "REPORTED"
    r01.rowsRemoved shouldBe 0L
    outcome.cleaned.count() shouldBe 2L
  }

  test("CR01 tolerates a rounded 1 within the configured tolerance") {
    val df = output(
      ("BCEF_CONSO_TF_Q", "C", "0", "1,0000000001"),
      ("BCEF_CONSO_TF_Q", "C", "0,25", "0,9999999999"))

    result(run(df, baseConf.copy(tolerance = 1e-6)), CheckRule.AllTermsEqualOne).total shouldBe 1L
    result(run(df, baseConf.copy(tolerance = 1e-12)), CheckRule.AllTermsEqualOne).total shouldBe 0L
  }

  test("CR01 treats a blank EAD_RA_RATE as not equal to 1, so the group is not flagged") {
    val df = output(
      ("BCEF_CONSO_TF_Q", "C", "0", "1"),
      ("BCEF_CONSO_TF_Q", "C", "0,25", ""))

    result(run(df), CheckRule.AllTermsEqualOne).total shouldBe 0L
  }

  test("CR01 disabled: nothing evaluated, nothing removed") {
    val df = output(
      ("BCEF_CONSO_TF_Q", "C", "0", "1"),
      ("BCEF_CONSO_TF_Q", "C", "0,25", "1"))

    val outcome = run(df, baseConf.copy(allTermsEqualOneEnabled = false))
    result(outcome, CheckRule.AllTermsEqualOne).status shouldBe "SKIPPED"
    outcome.cleaned.count() shouldBe 2L
  }

  // ---- CR02: negative EAD_RA_RATE -------------------------------------------

  test("CR02 reports every negative EAD_RA_RATE and removes none of them") {
    val df = output(
      ("BCEF_CONSO_TF_Q", "C", "0", "0,98"),
      ("BCEF_CONSO_TF_Q", "C", "0,25", "-0,15"),
      ("BCEF_CONSO_TF_Q", "A", "0,5", "-2,4"))

    val outcome = run(df, baseConf.copy(negativeMarker = ""))
    val r02 = result(outcome, CheckRule.NegativeEadRaRate)

    r02.total shouldBe 2L
    r02.status shouldBe "REPORTED"
    r02.rowsRemoved shouldBe 0L
    r02.findings.map(_.value) should contain allOf ("-0,15", "-2,4")
    outcome.cleaned.count() shouldBe 3L // nothing removed
  }

  test("CR02 writes the marker in place of a negative value, keeping the line and the column order") {
    val df = output(
      ("BCEF_CONSO_TF_Q", "C", "0", "0,98"),
      ("BCEF_CONSO_TF_Q", "C", "0,25", "-0,15"))

    val outcome = run(df)
    val rows = outcome.cleaned.orderBy("TERM").collect()

    outcome.cleaned.columns shouldBe Array("EAD_MATRIX_ID", "SCENARIO_ID", "TERM", "EAD_RA_RATE", "EAD_CCF_RATE")
    rows.length shouldBe 2                       // the line survives
    rows.map(_.getAs[String]("EAD_RA_RATE")) shouldBe Array("0,98", "NV")

    val r02 = result(outcome, CheckRule.NegativeEadRaRate)
    r02.valuesReplaced shouldBe 1L
    r02.marker shouldBe "NV"
    r02.action shouldBe "line(s) kept, 1 value(s) written as NV in the output"
    // the report keeps the value AS COMPUTED — the marker is an output concern, not a finding
    r02.findings.map(_.value) shouldBe Seq("-0,15")
  }

  test("CR02 marker is applied after the numeric filters, never before") {
    // 1 would be dropped by the >= 1 exclusion, -0,3 kept and marked; if the marker ran first the
    // exclusion would see a non-numeric cell and the row would survive unfiltered.
    val df = output(
      ("BCEF_CONSO_TF_Q", "C", "0", "1"),
      ("BCEF_CONSO_TF_Q", "C", "0,25", "-0,3"),
      ("BCEF_CONSO_TF_Q", "C", "0,5", "0,9"))

    val outcome = run(df, baseConf.copy(excludeEadRaRateGe1 = true))
    val values = outcome.cleaned.orderBy("TERM").collect().map(_.getAs[String]("EAD_RA_RATE"))

    values shouldBe Array("NV", "0,9") // the "1" row is gone, the negative is marked
  }

  test("CR02 with replaceWith empty leaves the computed value in the output") {
    val df = output(("BCEF_CONSO_TF_Q", "C", "0", "-0,15"))

    val outcome = run(df, baseConf.copy(negativeMarker = ""))
    outcome.cleaned.head().getAs[String]("EAD_RA_RATE") shouldBe "-0,15"
    result(outcome, CheckRule.NegativeEadRaRate).action shouldBe "kept (reporting only)"
  }

  test("by DEFAULT no marker is configured, so a negative value is written as computed") {
    // A negative rate only exists once allow_negative_ead_ra_rate has been switched on, and the
    // point of switching it on is to see the real number — the default must not mask it.
    val conf = com.typesafe.config.ConfigFactory.parseString(
      """tseadfwd_app {
        |  TS_EAD_FWD { tmpPath = "target", tableName = "T" }
        |  COHERENCE_CHECK { enabled = true }
        |}""".stripMargin)

    val checks = CheckConfig.from(conf)
    checks.negativeMarker shouldBe ""
    checks.negativeEnabled shouldBe true
    checks.allTermsEqualOneRemoves shouldBe true

    val outcome = run(output(("BCEF_CONSO_TF_Q", "C", "0", "-0,15")), checks)
    outcome.cleaned.head().getAs[String]("EAD_RA_RATE") shouldBe "-0,15"
  }

  test("CR02 disabled: no marker is written even when a negative value is present") {
    val df = output(("BCEF_CONSO_TF_Q", "C", "0", "-0,15"))

    val outcome = run(df, baseConf.copy(negativeEnabled = false))
    outcome.cleaned.head().getAs[String]("EAD_RA_RATE") shouldBe "-0,15"
    result(outcome, CheckRule.NegativeEadRaRate).status shouldBe "SKIPPED"
  }

  test("CR02 caps the listed rows but keeps the full count") {
    val rows = (1 to 10).map(i => ("BCEF_CONSO_TF_Q", "C", s"$i", s"-0,$i"))
    val outcome = run(output(rows: _*), baseConf.copy(maxRowsInReport = 3))
    val r02 = result(outcome, CheckRule.NegativeEadRaRate)

    r02.total shouldBe 10L
    r02.findings.size shouldBe 3
    r02.truncated shouldBe true
  }

  test("CR02 does not fire on zero by default — only strictly negative values") {
    val outcome = run(output(("BCEF_CONSO_TF_Q", "C", "0", "0")))
    result(outcome, CheckRule.NegativeEadRaRate).total shouldBe 0L
  }

  test("CR02 fires on zero when includeZero is on") {
    val outcome = run(output(("BCEF_CONSO_TF_Q", "C", "0", "0")),
      baseConf.copy(negativeIncludesZero = true))
    val r02 = result(outcome, CheckRule.NegativeEadRaRate)

    r02.total shouldBe 1L
    r02.findings.head.detail shouldBe "zero exposure factor (exposure fully run off)"
  }

  test("CR02 with includeZero names a zero and a negative apart") {
    val outcome = run(
      output(
        ("BCEF_CONSO_TF_Q", "C", "0", "0"),
        ("BCEF_CONSO_TF_Q", "C", "1", "-0,25"),
        ("BCEF_CONSO_TF_Q", "C", "2", "0,5")),
      baseConf.copy(negativeIncludesZero = true, negativeMarker = ""))
    val r02 = result(outcome, CheckRule.NegativeEadRaRate)

    r02.total shouldBe 2L
    r02.findings.map(_.detail) should contain theSameElementsAs Seq(
      "zero exposure factor (exposure fully run off)", "negative exposure factor")
  }

  test("CR02 with includeZero still REMOVES nothing — the lines stay in the output") {
    val rows = output(
      ("BCEF_CONSO_TF_Q", "C", "0", "0"),
      ("BCEF_CONSO_TF_Q", "C", "1", "-0,25"),
      ("BCEF_CONSO_TF_Q", "C", "2", "0,5"))
    val outcome = run(rows, baseConf.copy(negativeIncludesZero = true, negativeMarker = ""))

    outcome.cleaned.count() shouldBe 3L
    outcome.report.rowsOut shouldBe 3L
    result(outcome, CheckRule.NegativeEadRaRate).rowsRemoved shouldBe 0L
  }

  test("CR02 marker masks a zero too when includeZero is on") {
    val outcome = run(
      output(
        ("BCEF_CONSO_TF_Q", "C", "0", "0"),
        ("BCEF_CONSO_TF_Q", "C", "1", "0,5")),
      baseConf.copy(negativeIncludesZero = true, negativeMarker = "NV"))

    val byTerm = outcome.cleaned.collect()
      .map(r => r.getAs[String]("TERM") -> r.getAs[String]("EAD_RA_RATE")).toMap
    byTerm("0") shouldBe "NV"
    byTerm("1") shouldBe "0,5"
    result(outcome, CheckRule.NegativeEadRaRate).valuesReplaced shouldBe 1L
  }

  // ---- the exclude_ead_ra_rate_ge_1 engine option, moved out of the mapper ---

  test("exclude_ead_ra_rate_ge_1 drops the full-exposure terms AFTER the rules have seen them") {
    val df = output(
      ("BCEF_CONSO_TF_Q", "C", "0", "1"),
      ("BCEF_CONSO_TF_Q", "C", "0,25", "1"),
      ("BCEF_CONSO_TF_Q", "A", "0", "1"),
      ("BCEF_CONSO_TF_Q", "A", "0,25", "0,97"))

    val outcome = run(df, baseConf.copy(excludeEadRaRateGe1 = true))

    // CR01 still sees — and names — the Central curve, which the old in-mapper filter had already
    // deleted before any rule could group it.
    result(outcome, CheckRule.AllTermsEqualOne).total shouldBe 1L
    // Central removed by CR01 (2 rows), Adverse term 0 dropped by the >= 1 exclusion (1 row).
    outcome.cleaned.count() shouldBe 1L
    outcome.report.rowsOut shouldBe 1L
  }

  // ---- report-only path (the standalone driver) -----------------------------

  test("reportOnly evaluates the rules without removing anything") {
    val df = output(
      ("BCEF_CONSO_TF_Q", "C", "0", "1"),
      ("BCEF_CONSO_TF_Q", "C", "0,25", "1"))

    val report = new CoherenceCheckMapper(baseConf)(spark).reportOnly(df, "some.csv", "standalone")

    report.results.find(_.rule == CheckRule.AllTermsEqualOne).get.total shouldBe 1L
    report.results.find(_.rule == CheckRule.AllTermsEqualOne).get.status shouldBe "REPORTED"
    report.results.find(_.rule == CheckRule.AllTermsEqualOne).get.action should
      include("the main job removes these lines")
    report.rowsIn shouldBe 2L
    report.rowsOut shouldBe 2L
    report.removalKeys shouldBe Seq(("BCEF_CONSO_TF_Q", "C"))
  }

  test("a clean output produces a PASS report") {
    val df = output(
      ("BCEF_CONSO_TF_Q", "C", "0", "0,99"),
      ("BCEF_CONSO_TF_Q", "C", "0,25", "0,97"))

    val outcome = run(df)
    outcome.report.verdict shouldBe "PASS"
    outcome.report.totalFindings shouldBe 0L
    outcome.cleaned.count() shouldBe 2L
  }

  // ---- the output file the report names -------------------------------------

  test("the report names the output file the rules were run on") {
    val df = output(("BCEF_CONSO_TF_Q", "C", "0", "0,99"))

    val report = new CoherenceCheckMapper(baseConf)(spark)
      .apply(df, source = "TS_EAD_FWD", runId = "test-run",
        outputFile = "hdfs:///user/tseadfwd/output/TS_EAD_FWD_25Q4_v1.csv").report

    report.outputFile shouldBe "hdfs:///user/tseadfwd/output/TS_EAD_FWD_25Q4_v1.csv"
    report.outputFileName shouldBe "TS_EAD_FWD_25Q4_v1.csv"
    report.summaryLine should include("on TS_EAD_FWD_25Q4_v1.csv")
  }

  test("a report-only run names the file it read, not the configured output") {
    val df = output(("BCEF_CONSO_TF_Q", "C", "0", "0,99"))
    val conf = baseConf.copy(sourcePath = "localRun/tseadfwd/output/A_PREVIOUS_VINTAGE.csv")

    val report = new CoherenceCheckMapper(conf)(spark).reportOnly(df, "TS_EAD_FWD", "standalone")

    report.outputFile shouldBe "localRun/tseadfwd/output/A_PREVIOUS_VINTAGE.csv"
    report.outputFileName shouldBe "A_PREVIOUS_VINTAGE.csv"
  }

  test("an unknown output file leaves the report — and the log line — without one") {
    val df = output(("BCEF_CONSO_TF_Q", "C", "0", "0,99"))

    val report = new CoherenceCheckMapper(baseConf)(spark)
      .apply(df, source = "TS_EAD_FWD", runId = "test-run", outputFile = "").report

    report.outputFileName shouldBe ""
    report.summaryLine should startWith("COHERENCE CHECK - PASS")
  }

  // ---- CheckConfig: the output file is resolved as the writer resolves it -------

  private def confWith(outputBlock: String): CheckConfig =
    CheckConfig.from(ConfigFactory.parseString(
      s"""tseadfwd_app {
         |  TS_EAD_FWD { tmpPath = "localRun/tseadfwd/output", tableName = "TS_EAD_FWD_25Q4", $outputBlock }
         |}""".stripMargin))

  test("CheckConfig resolves the collapsed single CSV file") {
    confWith("""format = "csv", singleFile = true""").outputFile shouldBe
      "localRun/tseadfwd/output/TS_EAD_FWD_25Q4.csv"
  }

  test("CheckConfig defaults to the collapsed CSV when neither key is set") {
    confWith("""mode = "overwrite"""").outputFile shouldBe
      "localRun/tseadfwd/output/TS_EAD_FWD_25Q4.csv"
  }

  test("CheckConfig says so when the output is left as a part-file directory") {
    confWith("""format = "csv", singleFile = false""").outputFile should
      include("TS_EAD_FWD_25Q4 (part-file directory)")
  }

  test("CheckConfig resolves an Excel output to the workbook") {
    confWith("""format = "com.crealytics.spark.excel"""").outputFile shouldBe
      "localRun/tseadfwd/output/TS_EAD_FWD_25Q4.xlsx"
  }
}
