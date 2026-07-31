package com.bnp.str.tseadfwd.dataquality

import com.bnp.str.tseadfwd.utility.PrimaryConstants
import com.bnp.str.tseadfwd.utility.PrimaryConstants._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Data-quality settings, read from the `tseadfwd_app.DATA_QUALITY` block.
 *
 * `excludeEadRaRateGe1` is NOT a business rule — it is the pre-existing
 * `parameters.exclude_ead_ra_rate_ge_1` engine option, whose row filtering moved here out of
 * [[com.bnp.str.tseadfwd.mapping.PrimaryMapper]]. The key keeps its old name and place, so existing
 * configuration files keep working unchanged; only the place the rows are dropped has changed, which
 * is what lets rule R01 see a full-exposure curve at all (previously those rows were already gone).
 */
final case class DqConfig(
                           enabled: Boolean,
                           htmlPath: String,
                           sourcePath: String,
                           allTermsEqualOneEnabled: Boolean,
                           allTermsEqualOneRemoves: Boolean,
                           tolerance: Double,
                           negativeEnabled: Boolean,
                           maxRowsInReport: Int,
                           negativeMarker: String,
                           excludeEadRaRateGe1: Boolean
                         )

object DqConfig {

  /** Conf block holding the data-quality settings. */
  final val BLOCK = "DATA_QUALITY"

  /**
   * Read the block, falling back to sensible defaults for every key so a conf that predates this
   * feature still runs (data quality ON, both rules ON, R01 removing, outputs next to TS_EAD_FWD).
   */
  def from(config: Config): DqConfig = {
    val appConf = config.getConfig(PrimaryConstants.APP_CONF)
    val dq = if (appConf.hasPath(BLOCK)) appConf.getConfig(BLOCK) else ConfigFactory.empty()
    val rules = if (dq.hasPath("rules")) dq.getConfig("rules") else ConfigFactory.empty()

    def sub(name: String): Config =
      if (rules.hasPath(name)) rules.getConfig(name) else ConfigFactory.empty()
    def bool(c: Config, key: String, default: Boolean): Boolean =
      if (c.hasPath(key)) c.getBoolean(key) else default
    def dbl(c: Config, key: String, default: Double): Double =
      if (c.hasPath(key)) c.getDouble(key) else default
    def int(c: Config, key: String, default: Int): Int =
      if (c.hasPath(key)) c.getInt(key) else default
    def str(c: Config, key: String, default: String): String =
      if (c.hasPath(key)) c.getString(key) else default

    // Defaults derived from the output block, so both files land next to the term structure
    // (local or HDFS — everything is written through Hadoop's FileSystem).
    val out = appConf.getConfig(PrimaryConstants.OUTPUT_EAD_FWD)
    val outDir = out.getString("tmpPath")
    val outName = out.getString("tableName")

    val params =
      if (appConf.hasPath("parameters")) appConf.getConfig("parameters") else appConf

    val one = sub("all_terms_equal_one")
    val neg = sub("negative_ead_ra_rate")

    DqConfig(
      enabled                 = bool(dq, "enabled", default = true),
      htmlPath                = str(dq, "htmlPath", s"$outDir/DQ_$outName.html"),
      sourcePath              = str(dq, "sourcePath", s"$outDir/$outName.csv"),
      allTermsEqualOneEnabled = bool(one, "enabled", default = true),
      allTermsEqualOneRemoves = bool(one, "remove", default = true),
      tolerance               = dbl(one, "tolerance", default = 1e-9),
      negativeEnabled         = bool(neg, "enabled", default = true),
      maxRowsInReport         = int(neg, "maxRowsInReport", default = 500),
      // Empty by default: a negative value only exists once `allow_negative_ead_ra_rate` has been
      // opted into, and whoever opted in asked to SEE it. Masking it behind a token would hide the
      // very thing the flag was turned on for. Set a marker (e.g. "NV") only when a downstream
      // consumer cannot take a negative number.
      negativeMarker          = str(neg, "replaceWith", default = ""),
      excludeEadRaRateGe1     = bool(params, "exclude_ead_ra_rate_ge_1", default = false)
    )
  }
}

/** Cleaned output plus the report that explains what was taken out of it. */
final case class DqOutcome(cleaned: DataFrame, report: DqReport)

/**
 * Business data-quality mapper for the TS_EAD_FWD term structure.
 *
 * Two responsibilities, kept deliberately apart:
 *  - [[reportOnly]] EVALUATES the rules and returns a [[DqReport]]. It never changes a row; this is
 *    what the standalone [[com.bnp.str.tseadfwd.job.DataQualityDriver]] runs against an existing CSV.
 *  - [[apply]] evaluates and ALSO returns the cleaned DataFrame. The Main job calls this one and
 *    writes the cleaned frame — so the rows leave the output in the main job, exactly once, and the
 *    report can still name every one of them.
 *
 * Rules (see [[DqRule]]):
 *  - R01 all terms = 1 for one (EAD_MATRIX_ID, SCENARIO_ID) -> the group's rows are removed
 *  - R02 negative EAD_RA_RATE -> reported, never removed
 *
 * `EAD_RA_RATE` and `TERM` reach here as decimal-comma STRINGS (`PrimaryMapper.fmtNumber`), so every
 * comparison goes through [[numeric]] rather than a raw string test.
 */
class DataQualityMapper(dq: DqConfig)(implicit spark: SparkSession) {

  private val log = LoggerFactory.getLogger(this.getClass)

  private val TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  /** Decimal-comma string -> Double; blank/absent -> null (a null is never "equal to 1"). */
  private def numeric(c: Column): Column =
    when(c.isNull || trim(c) === lit(""), lit(null).cast(DoubleType))
      .otherwise(regexp_replace(trim(c), ",", ".").cast(DoubleType))

  private def rate: Column = numeric(col(OUT_EAD_RA_RATE))

  /**
   * Evaluate every rule WITHOUT touching the data. `rowsOut` equals `rowsIn`, and each result is
   * marked `applied = false` — nothing was removed.
   */
  def reportOnly(df: DataFrame, source: String, runId: String): DqReport = {
    val rowsIn = df.count()
    DqReport(
      source = source,
      runId = runId,
      generatedAt = LocalDateTime.now().format(TS),
      rowsIn = rowsIn,
      rowsOut = rowsIn,
      results = Seq(
        r01Result(if (dq.allTermsEqualOneEnabled) allOnesGroups(df) else Seq.empty,
          rowsRemoved = 0L, applied = false, reportOnly = true),
        ruleNegative(df)
      )
    )
  }

  /**
   * Evaluate the rules and return both the report and the cleaned output. Removal is applied for
   * R01 when the rule is enabled and `remove = true`, followed by the `exclude_ead_ra_rate_ge_1`
   * engine option (which moved here from the mapper).
   */
  def apply(df: DataFrame, source: String, runId: String): DqOutcome = {
    val rowsIn = df.count()

    val removeR01 = dq.allTermsEqualOneEnabled && dq.allTermsEqualOneRemoves
    val keys = if (dq.allTermsEqualOneEnabled) allOnesGroups(df) else Seq.empty

    val afterR01 = if (removeR01 && keys.nonEmpty) dropGroups(df, keys.map(f => (f.matrixId, f.scenarioId))) else df
    val afterR01Rows = if (removeR01 && keys.nonEmpty) afterR01.count() else rowsIn

    // `exclude_ead_ra_rate_ge_1`: keep only the terms where loss has started to accrue. A null rate
    // is KEPT — an unparseable value is a finding to look at, not a row to silently drop.
    val filtered = if (dq.excludeEadRaRateGe1) afterR01.where(rate.isNull || rate < lit(1.0)) else afterR01
    val rowsOut = if (dq.excludeEadRaRateGe1) filtered.count() else afterR01Rows

    // R02's marker goes on LAST: it turns the cell into a non-numeric token, so every numeric
    // predicate above has to have run already. The line itself stays — only its value is replaced.
    val markNegatives = dq.negativeEnabled && dq.negativeMarker.nonEmpty
    val replaced = if (markNegatives) filtered.where(rate < lit(0.0)).count() else 0L
    val cleaned = if (markNegatives && replaced > 0L) markNegativeValues(filtered) else filtered

    if (replaced > 0L)
      log.info(s"R02: $replaced negative EAD_RA_RATE value(s) written as '${dq.negativeMarker}'")

    val report = DqReport(
      source = source,
      runId = runId,
      generatedAt = LocalDateTime.now().format(TS),
      rowsIn = rowsIn,
      rowsOut = rowsOut,
      results = Seq(
        r01Result(keys, rowsRemoved = rowsIn - afterR01Rows, applied = removeR01),
        ruleNegative(df, valuesReplaced = replaced, marker = if (markNegatives) dq.negativeMarker else "")
      )
    )

    if (dq.excludeEadRaRateGe1)
      log.info(s"exclude_ead_ra_rate_ge_1 = true -> ${afterR01Rows - rowsOut} full-exposure row(s) " +
        s"dropped after the data-quality rules")

    DqOutcome(cleaned, report)
  }

  // ---- R01: all terms equal to 1 -------------------------------------------

  /**
   * The (EAD_MATRIX_ID, SCENARIO_ID) groups whose EVERY term carries `EAD_RA_RATE = 1` (within
   * [[DqConfig.tolerance]]). A group with a single deviating — or unparseable — term is NOT flagged.
   *
   * NOTE the matrix id carries the frequency suffix (`..._Q` / `..._Y`), so the quarterly and yearly
   * curves of the same matrix are separate groups, which is what "all terms" has to mean.
   */
  private def allOnesGroups(df: DataFrame): Seq[DqFinding] = {
    val isOne = when(rate.isNotNull && abs(rate - lit(1.0)) <= lit(dq.tolerance), lit(1)).otherwise(lit(0))

    df.groupBy(col(OUT_MATRIX_ID), col(OUT_SCENARIO_ID))
      .agg(count(lit(1)).as("terms"), sum(isOne).as("ones"))
      .where(col("terms") > lit(0) && col("terms") === col("ones"))
      .orderBy(col(OUT_MATRIX_ID), col(OUT_SCENARIO_ID))
      .collect()
      .map { r =>
        val terms = r.getAs[Long]("terms")
        DqFinding(
          matrixId = r.getAs[String](OUT_MATRIX_ID),
          scenarioId = r.getAs[String](OUT_SCENARIO_ID),
          term = "",
          value = "1",
          detail = s"all $terms term(s) equal 1 (full exposure over the whole curve)")
      }
      .toSeq
  }

  private def r01Result(findings: Seq[DqFinding], rowsRemoved: Long, applied: Boolean,
                        reportOnly: Boolean = false): DqRuleResult =
    DqRuleResult(
      rule = DqRule.AllTermsEqualOne,
      enabled = dq.allTermsEqualOneEnabled,
      total = findings.size.toLong,
      findings = findings, // groups are few; never truncated
      rowsRemoved = rowsRemoved,
      // `applied` means the removal step RAN for this rule — not that it found something to drop.
      // Conflating the two made a clean run report "removal disabled in the configuration".
      applied = applied,
      reportOnly = reportOnly)

  // ---- R02: negative EAD_RA_RATE -------------------------------------------

  /**
   * Every output row whose `EAD_RA_RATE` is strictly negative. Reporting only — these rows stay in
   * the output. The listing is capped at [[DqConfig.maxRowsInReport]]; the total count is not.
   *
   * With the engine's default settings this rule finds nothing by construction: the run-off freeze
   * truncates the RA series before the cumulative product can go negative, and a sub-zero product
   * would be reported as 1 anyway. Set `parameters.allow_negative_ead_ra_rate = true` to let those
   * values through — see [[com.bnp.str.tseadfwd.mapping.PrimaryView.vectorFactored]].
   */
  private def ruleNegative(df: DataFrame, valuesReplaced: Long = 0L, marker: String = ""): DqRuleResult = {
    if (!dq.negativeEnabled)
      return DqRuleResult(DqRule.NegativeEadRaRate, enabled = false, 0L, Seq.empty, 0L, applied = false)

    val negatives = df.where(rate.isNotNull && rate < lit(0.0))
    val total = negatives.count()

    val listed =
      if (total == 0L) Seq.empty[DqFinding]
      else negatives
        .orderBy(col(OUT_MATRIX_ID), col(OUT_SCENARIO_ID), numeric(col(OUT_TERM)))
        .limit(dq.maxRowsInReport)
        .collect()
        .map { r =>
          DqFinding(
            matrixId = r.getAs[String](OUT_MATRIX_ID),
            scenarioId = r.getAs[String](OUT_SCENARIO_ID),
            term = r.getAs[String](OUT_TERM),
            value = r.getAs[String](OUT_EAD_RA_RATE),
            detail = "negative exposure factor")
        }
        .toSeq

    DqRuleResult(DqRule.NegativeEadRaRate, enabled = true, total, listed, rowsRemoved = 0L,
      applied = false, valuesReplaced = valuesReplaced, marker = marker)
  }

  // ---- removal --------------------------------------------------------------

  /**
   * Replace a negative `EAD_RA_RATE` with the configured marker (e.g. `NV`), in place, keeping the
   * column where it is. The row survives: the term exists, and dropping it would leave a hole in the
   * curve — what must not survive is a number a consumer could mistake for a real exposure factor.
   */
  private def markNegativeValues(df: DataFrame): DataFrame =
    df.withColumn(OUT_EAD_RA_RATE,
      when(rate < lit(0.0), lit(dq.negativeMarker)).otherwise(col(OUT_EAD_RA_RATE)))

  /** Anti-join the flagged (matrix, scenario) keys out of the output. */
  private def dropGroups(df: DataFrame, keys: Seq[(String, String)]): DataFrame = {
    import spark.implicits._
    val keysDf = keys.toDF(OUT_MATRIX_ID, OUT_SCENARIO_ID)
    df.join(broadcast(keysDf), Seq(OUT_MATRIX_ID, OUT_SCENARIO_ID), "left_anti")
  }
}
