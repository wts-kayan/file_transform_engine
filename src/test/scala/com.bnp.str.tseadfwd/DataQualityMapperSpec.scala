package com.bnp.str.tseadfwd

import com.bnp.str.tseadfwd.dataquality.{DataQualityMapper, DqConfig, DqRule}
import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for the business data-quality rules on the TS_EAD_FWD output.
 *
 * Values are built exactly as the engine emits them — decimal-comma STRINGS, trailing zeros
 * stripped, so a full-exposure term is the literal "1" — because the rules have to parse them.
 *
 * Run (offline, via the ScalaTest runner on the test classpath):
 *   mvn -o dependency:build-classpath -Dmdep.outputFile=cp.txt -DincludeScope=test
 *   java -cp "target/classes;target/test-classes;$(cat cp.txt)" \
 *        org.scalatest.tools.Runner -o -s com.bnp.str.tseadfwd.DataQualityMapperSpec
 */
class DataQualityMapperSpec extends AnyFunSuite with Matchers with SparkTestSession {

  private val baseConf = DqConfig(
    enabled = true,
    htmlPath = "target/test-dq.html",
    sourcePath = "unused",
    allTermsEqualOneEnabled = true,
    allTermsEqualOneRemoves = true,
    tolerance = 1e-9,
    negativeEnabled = true,
    maxRowsInReport = 500,
    negativeMarker = "NV",
    excludeEadRaRateGe1 = false)

  /** (EAD_MATRIX_ID, SCENARIO_ID, TERM, EAD_RA_RATE, EAD_CCF_RATE) as the mapper emits them. */
  private def output(rows: (String, String, String, String)*): DataFrame = {
    import spark.implicits._
    rows.map { case (m, s, t, r) => (m, s, t, r, "") }
      .toDF("EAD_MATRIX_ID", "SCENARIO_ID", "TERM", "EAD_RA_RATE", "EAD_CCF_RATE")
  }

  private def run(df: DataFrame, conf: DqConfig = baseConf) =
    new DataQualityMapper(conf)(spark).apply(df, source = "TS_EAD_FWD", runId = "test-run")

  private def result(outcome: com.bnp.str.tseadfwd.dataquality.DqOutcome, rule: DqRule) =
    outcome.report.results.find(_.rule == rule).get

  // ---- R01: all terms equal to 1 -------------------------------------------

  test("R01 removes a (matrix, scenario) whose every term equals 1, and reports it") {
    val df = output(
      ("BCEF_CONSO_TF_Q", "C", "0", "1"),
      ("BCEF_CONSO_TF_Q", "C", "0,25", "1"),
      ("BCEF_CONSO_TF_Q", "C", "0,5", "1"),
      ("BCEF_CONSO_TF_Q", "A", "0", "1"),
      ("BCEF_CONSO_TF_Q", "A", "0,25", "0,98"))

    val outcome = run(df)
    val r01 = result(outcome, DqRule.AllTermsEqualOne)

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

  test("R01 does not fire when a single term deviates from 1") {
    val df = output(
      ("BGL_MORTGAGE_Y", "E", "0", "1"),
      ("BGL_MORTGAGE_Y", "E", "1", "1"),
      ("BGL_MORTGAGE_Y", "E", "2", "0,999999"))

    val outcome = run(df)
    result(outcome, DqRule.AllTermsEqualOne).total shouldBe 0L
    result(outcome, DqRule.AllTermsEqualOne).status shouldBe "PASS"
    // nothing found is not the same as removal switched off — the action must not blame the conf
    result(outcome, DqRule.AllTermsEqualOne).action shouldBe "-"
    outcome.cleaned.count() shouldBe 3L
  }

  test("a rule that passes with remove = true never reports the removal as disabled") {
    val clean = output(("BCEF_CONSO_TF_Q", "C", "0", "0,99"))

    val passing = result(run(clean), DqRule.AllTermsEqualOne)
    passing.status shouldBe "PASS"
    passing.action should not include "disabled"

    // the wording is reserved for the case it actually describes
    val hit = output(("BCEF_CONSO_TF_Q", "C", "0", "1"))
    result(run(hit, baseConf.copy(allTermsEqualOneRemoves = false)), DqRule.AllTermsEqualOne)
      .action shouldBe "kept (removal disabled in the configuration)"
  }

  test("R01 groups on the matrix id, so the quarterly and yearly curves are separate lines") {
    val df = output(
      ("BNL_CONSO_TF_Q", "C", "0", "1"),
      ("BNL_CONSO_TF_Q", "C", "0,25", "1"),
      ("BNL_CONSO_TF_Y", "C", "0", "0,95"))

    val outcome = run(df)
    val r01 = result(outcome, DqRule.AllTermsEqualOne)

    r01.total shouldBe 1L
    r01.findings.head.matrixId shouldBe "BNL_CONSO_TF_Q"
    outcome.cleaned.count() shouldBe 1L
  }

  test("R01 with remove = false reports the line but leaves it in the output") {
    val df = output(
      ("BCEF_CONSO_TF_Q", "C", "0", "1"),
      ("BCEF_CONSO_TF_Q", "C", "0,25", "1"))

    val outcome = run(df, baseConf.copy(allTermsEqualOneRemoves = false))
    val r01 = result(outcome, DqRule.AllTermsEqualOne)

    r01.total shouldBe 1L
    r01.status shouldBe "REPORTED"
    r01.rowsRemoved shouldBe 0L
    outcome.cleaned.count() shouldBe 2L
  }

  test("R01 tolerates a rounded 1 within the configured tolerance") {
    val df = output(
      ("BCEF_CONSO_TF_Q", "C", "0", "1,0000000001"),
      ("BCEF_CONSO_TF_Q", "C", "0,25", "0,9999999999"))

    result(run(df, baseConf.copy(tolerance = 1e-6)), DqRule.AllTermsEqualOne).total shouldBe 1L
    result(run(df, baseConf.copy(tolerance = 1e-12)), DqRule.AllTermsEqualOne).total shouldBe 0L
  }

  test("R01 treats a blank EAD_RA_RATE as not equal to 1, so the group is not flagged") {
    val df = output(
      ("BCEF_CONSO_TF_Q", "C", "0", "1"),
      ("BCEF_CONSO_TF_Q", "C", "0,25", ""))

    result(run(df), DqRule.AllTermsEqualOne).total shouldBe 0L
  }

  test("R01 disabled: nothing evaluated, nothing removed") {
    val df = output(
      ("BCEF_CONSO_TF_Q", "C", "0", "1"),
      ("BCEF_CONSO_TF_Q", "C", "0,25", "1"))

    val outcome = run(df, baseConf.copy(allTermsEqualOneEnabled = false))
    result(outcome, DqRule.AllTermsEqualOne).status shouldBe "SKIPPED"
    outcome.cleaned.count() shouldBe 2L
  }

  // ---- R02: negative EAD_RA_RATE -------------------------------------------

  test("R02 reports every negative EAD_RA_RATE and removes none of them") {
    val df = output(
      ("BCEF_CONSO_TF_Q", "C", "0", "0,98"),
      ("BCEF_CONSO_TF_Q", "C", "0,25", "-0,15"),
      ("BCEF_CONSO_TF_Q", "A", "0,5", "-2,4"))

    val outcome = run(df, baseConf.copy(negativeMarker = ""))
    val r02 = result(outcome, DqRule.NegativeEadRaRate)

    r02.total shouldBe 2L
    r02.status shouldBe "REPORTED"
    r02.rowsRemoved shouldBe 0L
    r02.findings.map(_.value) should contain allOf ("-0,15", "-2,4")
    outcome.cleaned.count() shouldBe 3L // nothing removed
  }

  test("R02 writes the marker in place of a negative value, keeping the line and the column order") {
    val df = output(
      ("BCEF_CONSO_TF_Q", "C", "0", "0,98"),
      ("BCEF_CONSO_TF_Q", "C", "0,25", "-0,15"))

    val outcome = run(df)
    val rows = outcome.cleaned.orderBy("TERM").collect()

    outcome.cleaned.columns shouldBe Array("EAD_MATRIX_ID", "SCENARIO_ID", "TERM", "EAD_RA_RATE", "EAD_CCF_RATE")
    rows.length shouldBe 2                       // the line survives
    rows.map(_.getAs[String]("EAD_RA_RATE")) shouldBe Array("0,98", "NV")

    val r02 = result(outcome, DqRule.NegativeEadRaRate)
    r02.valuesReplaced shouldBe 1L
    r02.marker shouldBe "NV"
    r02.action shouldBe "line(s) kept, 1 value(s) written as NV in the output"
    // the report keeps the value AS COMPUTED — the marker is an output concern, not a finding
    r02.findings.map(_.value) shouldBe Seq("-0,15")
  }

  test("R02 marker is applied after the numeric filters, never before") {
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

  test("R02 with replaceWith empty leaves the computed value in the output") {
    val df = output(("BCEF_CONSO_TF_Q", "C", "0", "-0,15"))

    val outcome = run(df, baseConf.copy(negativeMarker = ""))
    outcome.cleaned.head().getAs[String]("EAD_RA_RATE") shouldBe "-0,15"
    result(outcome, DqRule.NegativeEadRaRate).action shouldBe "kept (reporting only)"
  }

  test("by DEFAULT no marker is configured, so a negative value is written as computed") {
    // A negative rate only exists once allow_negative_ead_ra_rate has been switched on, and the
    // point of switching it on is to see the real number — the default must not mask it.
    val conf = com.typesafe.config.ConfigFactory.parseString(
      """tseadfwd_app {
        |  TS_EAD_FWD { tmpPath = "target", tableName = "T" }
        |  DATA_QUALITY { enabled = true }
        |}""".stripMargin)

    val dq = DqConfig.from(conf)
    dq.negativeMarker shouldBe ""
    dq.negativeEnabled shouldBe true
    dq.allTermsEqualOneRemoves shouldBe true

    val outcome = run(output(("BCEF_CONSO_TF_Q", "C", "0", "-0,15")), dq)
    outcome.cleaned.head().getAs[String]("EAD_RA_RATE") shouldBe "-0,15"
  }

  test("R02 disabled: no marker is written even when a negative value is present") {
    val df = output(("BCEF_CONSO_TF_Q", "C", "0", "-0,15"))

    val outcome = run(df, baseConf.copy(negativeEnabled = false))
    outcome.cleaned.head().getAs[String]("EAD_RA_RATE") shouldBe "-0,15"
    result(outcome, DqRule.NegativeEadRaRate).status shouldBe "SKIPPED"
  }

  test("R02 caps the listed rows but keeps the full count") {
    val rows = (1 to 10).map(i => ("BCEF_CONSO_TF_Q", "C", s"$i", s"-0,$i"))
    val outcome = run(output(rows: _*), baseConf.copy(maxRowsInReport = 3))
    val r02 = result(outcome, DqRule.NegativeEadRaRate)

    r02.total shouldBe 10L
    r02.findings.size shouldBe 3
    r02.truncated shouldBe true
  }

  test("R02 does not fire on zero — only strictly negative values") {
    val outcome = run(output(("BCEF_CONSO_TF_Q", "C", "0", "0")))
    result(outcome, DqRule.NegativeEadRaRate).total shouldBe 0L
  }

  // ---- the exclude_ead_ra_rate_ge_1 engine option, moved out of the mapper ---

  test("exclude_ead_ra_rate_ge_1 drops the full-exposure terms AFTER the rules have seen them") {
    val df = output(
      ("BCEF_CONSO_TF_Q", "C", "0", "1"),
      ("BCEF_CONSO_TF_Q", "C", "0,25", "1"),
      ("BCEF_CONSO_TF_Q", "A", "0", "1"),
      ("BCEF_CONSO_TF_Q", "A", "0,25", "0,97"))

    val outcome = run(df, baseConf.copy(excludeEadRaRateGe1 = true))

    // R01 still sees — and names — the Central curve, which the old in-mapper filter had already
    // deleted before any rule could group it.
    result(outcome, DqRule.AllTermsEqualOne).total shouldBe 1L
    // Central removed by R01 (2 rows), Adverse term 0 dropped by the >= 1 exclusion (1 row).
    outcome.cleaned.count() shouldBe 1L
    outcome.report.rowsOut shouldBe 1L
  }

  // ---- report-only path (the standalone driver) -----------------------------

  test("reportOnly evaluates the rules without removing anything") {
    val df = output(
      ("BCEF_CONSO_TF_Q", "C", "0", "1"),
      ("BCEF_CONSO_TF_Q", "C", "0,25", "1"))

    val report = new DataQualityMapper(baseConf)(spark).reportOnly(df, "some.csv", "standalone")

    report.results.find(_.rule == DqRule.AllTermsEqualOne).get.total shouldBe 1L
    report.results.find(_.rule == DqRule.AllTermsEqualOne).get.status shouldBe "REPORTED"
    report.results.find(_.rule == DqRule.AllTermsEqualOne).get.action should
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
}
