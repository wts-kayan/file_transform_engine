package com.bnp.str.climatetables.mapping

import com.bnp.str.climatetables.mapping.PrimaryMapper._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Non-regression test for the `computePdDeterioration` rule resolution.
 *
 * The deterioration pd rule used to be picked with a wildcard join against the portfolio followed by
 * `row_number() over (partitionBy id_technique orderBy rule_num) == 1`. Because none of the six
 * predicates is an equality (each is `portfolio === rule || rule === ""`), that planned as a
 * BroadcastNestedLoopJoin whose output was then fully sorted - once per resolution, twice per run.
 * It is now resolved on the rule table: the rules are split by wildcard pattern, reduced to the
 * lowest rule_num per pattern and key tuple, and each pattern joined as the equality it is.
 *
 * This suite runs the old shape and the new one side by side on the same data and requires them to
 * agree, including on the edge cases the rewrite had to preserve: wildcard cells, null rule cells,
 * the six-character `pd_model` prefix match, rows matching no rule at all, and rule_num ordering.
 *
 * Run (offline, via the ScalaTest runner on the test classpath):
 *   mvn -o dependency:build-classpath -Dmdep.outputFile=cp.txt -DincludeScope=test
 *   java -cp "target/classes;target/test-classes;$(cat cp.txt)" \
 *        org.scalatest.tools.Runner -o -s com.bnp.str.climatetables.mapping.PdDeteriorationRuleSpec
 */
class PdDeteriorationRuleSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private lazy val spark: SparkSession = SparkSession.builder()
    .appName("PdDeteriorationRuleSpec").master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = if (SparkSession.getActiveSession.isDefined) spark.stop()

  /** Portfolio side: only the columns the rule predicates read, plus the partition key. */
  private def portfolio: DataFrame = {
    import spark.implicits._
    Seq(
      // id,  pd_model,         pd_model_orig,    pays, wiod, epc,  chr,  chr_ori, scale, scale_ori
      ("p1", "PDMODEL_A_LONG", "PDMODEL_A_LONG", "FR", "S1", "E1", "R1", "R1", "SC1", "SC1"),
      ("p2", "PDMODEL_B", "PDMODEL_A_OTHER", "BE", "S2", "E2", "R2", "R1", "SC2", "SC1"),
      ("p3", "ZZZZZZ", "ZZZZZZ", "XX", "S9", "E9", "R9", "R9", "SC9", "SC9"), // matches nothing
      ("p4", "PDMODEL_A_LONG", "PDMODEL_B", "FR", "S1", "E1", "R2", "R2", "SC1", "SC2"),
      ("p5", null, "PDMODEL_B", "FR", null, "E1", "R1", "R1", "SC1", "SC2") // null portfolio cells
    ).toDF("id_technique", "pd_model", "pd_model_origination", "cd_pays_residence_cal",
      "cd_sect_wiod", "nace_sector_ecb_epc_rating", "cd_niv_risq_chr", "chr_at_origination",
      "ste_target_rating_scale", "ste_target_rating_scale_origination")
  }

  /** Rule side, already filtered and aliased exactly as `inputDeteriorationPdFiltered` leaves it. */
  private def rules: DataFrame = {
    import spark.implicits._
    Seq(
      // rule_num, pd_model_short, geo,  nace, epc, chr,  scale, value, cap
      ("30", "", "", "", "", "", "", 1.30, 0.30), // catch-all, highest rule_num
      ("10", "PDMODEL_A", "FR", "", "", "R1", "", 1.10, 0.10), // narrow, wins on p1
      ("20", "PDMODEL_A", "FR", "", "", "", "", 1.20, 0.20), // broader, wins on p4
      ("15", "PDMODEL_B", "", "", "", "", "SC2", 1.15, 0.15),
      ("25", null, "BE", "", "", "", "", 1.25, 0.25) // null cell: rule can never match
    ).toDF("__det_rule_num__", "__det_pd_model_short_name__", "__det_geographical_breakdown__",
      "__det_nace_sector_ecb__", "__det_epc_rating__", "__det_cd_niv_risq_chr__",
      "__det_rating_scale__", "__det_value__", "__det_cap__")
  }

  /** The join + row_number() shape this change replaces, kept verbatim as the reference. */
  private def previousShape(df: DataFrame, ruleTable: DataFrame): DataFrame = {
    val windowSpecAsOf = Window.partitionBy(col("id_technique"))
      .orderBy(col("det_as_of.__det_rule_num__").cast("long").asc)

    val asOf = df.join(ruleTable.as("det_as_of").hint("broadcast"),
      (col("pd_model").substr(1, 6) === col("det_as_of.__det_pd_model_short_name__").substr(1, 6) ||
        col("det_as_of.__det_pd_model_short_name__") === "") &&
        (col("cd_pays_residence_cal") === col("det_as_of.__det_geographical_breakdown__") ||
          col("det_as_of.__det_geographical_breakdown__") === "") &&
        (col("cd_sect_wiod") === col("det_as_of.__det_nace_sector_ecb__") ||
          col("det_as_of.__det_nace_sector_ecb__") === "") &&
        (col("nace_sector_ecb_epc_rating") === col("det_as_of.__det_epc_rating__") ||
          col("det_as_of.__det_epc_rating__") === "") &&
        (col("cd_niv_risq_chr") === col("det_as_of.__det_cd_niv_risq_chr__") ||
          col("det_as_of.__det_cd_niv_risq_chr__") === "") &&
        (col("ste_target_rating_scale") === col("det_as_of.__det_rating_scale__") ||
          col("det_as_of.__det_rating_scale__") === ""), "left")
      .withColumn("__row_num__", row_number().over(windowSpecAsOf))
      .filter(col("__row_num__") === 1)
      .select(df("*"),
        col("det_as_of.__det_value__").as("__det_value__"),
        col("det_as_of.__det_cap__").as("__det_cap__"))

    val windowSpecAsOrig = Window.partitionBy(col("id_technique"))
      .orderBy(col("det_orig.__det_rule_num__").cast("long").asc)

    asOf.join(ruleTable.as("det_orig").hint("broadcast"),
      (col("pd_model_origination").substr(1, 6) === col("det_orig.__det_pd_model_short_name__").substr(1, 6) ||
        col("det_orig.__det_pd_model_short_name__") === "") &&
        (col("cd_pays_residence_cal") === col("det_orig.__det_geographical_breakdown__") ||
          col("det_orig.__det_geographical_breakdown__") === "") &&
        (col("cd_sect_wiod") === col("det_orig.__det_nace_sector_ecb__") ||
          col("det_orig.__det_nace_sector_ecb__") === "") &&
        (col("nace_sector_ecb_epc_rating") === col("det_orig.__det_epc_rating__") ||
          col("det_orig.__det_epc_rating__") === "") &&
        (col("chr_at_origination") === col("det_orig.__det_cd_niv_risq_chr__") ||
          col("det_orig.__det_cd_niv_risq_chr__") === "") &&
        (col("ste_target_rating_scale_origination") === col("det_orig.__det_rating_scale__") ||
          col("det_orig.__det_rating_scale__") === ""), "left")
      .withColumn("__row_num__", row_number().over(windowSpecAsOrig))
      .filter(col("__row_num__") === 1)
      .select(asOf("*"),
        col("det_orig.__det_value__").as("__det_value_origination__"),
        col("det_orig.__det_cap__").as("__det_cap_origination__"))
  }

  /** The shape `computePdDeterioration` now uses, reusing one reduced rule table as it does. */
  private def currentShape(df: DataFrame, ruleTable: DataFrame): DataFrame = {
    val reduced = reducePdDeteriorationRules(ruleTable)
    val masks = pdDeteriorationMasks(reduced)

    val asOf = resolvePdDeterioration(df, reduced, masks, PD_DET_AS_OF_KEYS, PD_DET_AS_OF)
      .select(df("*"),
        col(s"$PD_DET_AS_OF.value").as("__det_value__"),
        col(s"$PD_DET_AS_OF.cap").as("__det_cap__"))

    resolvePdDeterioration(asOf, reduced, masks, PD_DET_ORIGINATION_KEYS, PD_DET_ORIG)
      .select(asOf("*"),
        col(s"$PD_DET_ORIG.value").as("__det_value_origination__"),
        col(s"$PD_DET_ORIG.cap").as("__det_cap_origination__"))
  }

  private val resolved = Seq("id_technique", "__det_value__", "__det_cap__",
    "__det_value_origination__", "__det_cap_origination__")

  test("the pattern joins resolve the same rule as the join + row_number() they replace") {
    val before = previousShape(portfolio, rules).select(resolved.map(col): _*)
    val after = currentShape(portfolio, rules).select(resolved.map(col): _*)

    before.except(after).collect() shouldBe empty
    after.except(before).collect() shouldBe empty
    after.count() shouldBe portfolio.count()
  }

  test("the resolved rule is the lowest rule_num among those that match") {
    val byId = currentShape(portfolio, rules).select(resolved.map(col): _*)
      .collect().map(r => r.getString(0) -> r).toMap

    // p1 matches rules 10, 20 and 30 as-of, so 10 wins
    byId("p1").getDouble(1) shouldBe 1.10
    byId("p1").getDouble(2) shouldBe 0.10
    // p4 fails rule 10 on cd_niv_risq_chr, so 20 wins
    byId("p4").getDouble(1) shouldBe 1.20
    // p2 (geo BE) fails rules 10 and 20 on the country, so 15 wins on the rating scale
    byId("p2").getDouble(1) shouldBe 1.15
    // at origination p2 has scale SC1, so 15 no longer matches either and only the catch-all is left
    byId("p2").getDouble(3) shouldBe 1.30
    // p3 matches nothing but the catch-all
    byId("p3").getDouble(1) shouldBe 1.30
  }

  test("pd_model matches on the first six characters only, as the original predicate did") {
    // "PDMODEL_A" and "PDMODEL_B" both truncate to "PDMODE", so rule 15 (PDMODEL_B) is reachable
    // from a PDMODEL_A portfolio row. Preserved deliberately: it is the behaviour of
    // substr(1, 6) === substr(1, 6) in the join this replaces.
    val onlyPrefixRule = rules.filter(col("__det_rule_num__") === "15")
    val after = currentShape(portfolio, onlyPrefixRule)
    val before = previousShape(portfolio, onlyPrefixRule)

    // p4: pd_model PDMODEL_A_LONG, ste_target_rating_scale_origination SC2 -> matches at origination
    after.filter(col("id_technique") === "p4").head().getAs[Double]("__det_value_origination__") shouldBe 1.15
    before.except(after).collect() shouldBe empty
    after.except(before).collect() shouldBe empty
  }

  test("a row matching no rule keeps the null the left join produced") {
    // p3 matches nothing but the catch-all, so drop the catch-all for this case
    val narrow = rules.filter(col("__det_rule_num__") =!= "30")
    val before = previousShape(portfolio, narrow).select(resolved.map(col): _*)
    val after = currentShape(portfolio, narrow).select(resolved.map(col): _*)

    before.except(after).collect() shouldBe empty
    after.except(before).collect() shouldBe empty
    after.filter(col("id_technique") === "p3")
      .head().toSeq.tail.forall(_ == null) shouldBe true
  }

  test("a rule with a null cell never matches, as in the old three-valued join predicate") {
    // rule 25 would win for p2 (geo = BE) if its null pd_model_short_name were read as a wildcard
    val onlyNullRule = rules.filter(col("__det_rule_num__") === "25")
    Option(currentShape(portfolio, onlyNullRule)
      .filter(col("id_technique") === "p2").head().getAs[Any]("__det_value__")) shouldBe None
    Option(previousShape(portfolio, onlyNullRule)
      .filter(col("id_technique") === "p2").head().getAs[Any]("__det_value__")) shouldBe None
  }

  test("an empty rule table leaves every row null, as the left join did") {
    val empty = rules.filter(lit(false))
    val after = currentShape(portfolio, empty).select(resolved.map(col): _*)
    after.count() shouldBe portfolio.count()
    after.filter(col("__det_value__").isNotNull).count() shouldBe 0
  }

  test("the resolved columns keep the types they had when they came out of the join") {
    val before = previousShape(portfolio, rules).select(resolved.map(col): _*).schema
    val after = currentShape(portfolio, rules).select(resolved.map(col): _*).schema
    after.map(f => f.name -> f.dataType) shouldBe before.map(f => f.name -> f.dataType)
  }

  test("the new plan resolves through broadcast equality joins, with no window and no nested loop") {
    val resolvedDf = currentShape(portfolio, rules)
    resolvedDf.collect()
    val plan = resolvedDf.queryExecution.executedPlan.toString

    plan should include ("BroadcastHashJoin")
    plan should not include "Window"
    plan should not include "SortMergeJoin"
    plan should not include "BroadcastNestedLoopJoin"
  }

  test("the rule count no longer bounds the plan: 20k rules resolve as 4 wildcard patterns") {
    import spark.implicits._
    // A rule table the CASE chain this replaces could not build: one nested when per rule, 20k deep.
    val manyRules = (Seq(("99999", "PDMODEL_A", "FR", "", "", "", "", 9.99, 0.99)) ++
      (1 to 20000).map(i => (i.toString, "PDMODEL_A", s"C$i", "", "", "", "", i / 100.0, i / 1000.0)))
      .toDF("__det_rule_num__", "__det_pd_model_short_name__", "__det_geographical_breakdown__",
        "__det_nace_sector_ecb__", "__det_epc_rating__", "__det_cd_niv_risq_chr__",
        "__det_rating_scale__", "__det_value__", "__det_cap__")
      .union(rules)

    pdDeteriorationMasks(reducePdDeteriorationRules(manyRules)).length shouldBe 4

    val before = previousShape(portfolio, manyRules).select(resolved.map(col): _*)
    val after = currentShape(portfolio, manyRules).select(resolved.map(col): _*)

    before.except(after).collect() shouldBe empty
    after.except(before).collect() shouldBe empty
    // p1 (geo FR) matches rule 10 as before: the 20k country rules are all reachable, none applies
    after.filter(col("id_technique") === "p1").head().getAs[Double]("__det_value__") shouldBe 1.10
  }
}
