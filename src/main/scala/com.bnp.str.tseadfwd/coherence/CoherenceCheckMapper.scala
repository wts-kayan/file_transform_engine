package com.bnp.str.tseadfwd.coherence

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
 * Coherence-check settings, read from the `tseadfwd_app.COHERENCE_CHECK` block.
 *
 * `excludeEadRaRateGe1` is NOT a business rule — it is the pre-existing
 * `parameters.exclude_ead_ra_rate_ge_1` engine option, whose row filtering moved here out of
 * [[com.bnp.str.tseadfwd.mapping.PrimaryMapper]]. The key keeps its old name and place, so existing
 * configuration files keep working unchanged; only the place the rows are dropped has changed, which
 * is what lets rule CR01 see a full-exposure curve at all (previously those rows were already gone).
 *
 * `outputFile` is likewise not a setting but a DERIVED value: the file `TS_EAD_FWD` is written to,
 * resolved from that block (`tmpPath`, `tableName`, `format`, `singleFile`) the same way
 * [[com.bnp.str.tseadfwd.utility.PrimaryUtilities.writeDataframe]] resolves it. The report names it,
 * so a report found on a share says which file it judged.
 */
final case class CheckConfig(
                           enabled: Boolean,
                           htmlPath: String,
                           sourcePath: String,
                           allTermsEqualOneEnabled: Boolean,
                           allTermsEqualOneRemoves: Boolean,
                           tolerance: Double,
                           negativeEnabled: Boolean,
                           negativeIncludesZero: Boolean,
                           maxRowsInReport: Int,
                           negativeMarker: String,
                           excludeEadRaRateGe1: Boolean,
                           outputFile: String = ""
                         )

object CheckConfig {

  /** Conf block holding the coherence-check settings. */
  final val BLOCK = "COHERENCE_CHECK"

  /** The block's former name. Read as a fallback so a conf written before the rename still applies. */
  final val LEGACY_BLOCK = "DATA_QUALITY"

  /**
   * Read the block, falling back to sensible defaults for every key so a conf that predates this
   * feature still runs (coherence check ON, both rules ON, CR01 removing, outputs next to TS_EAD_FWD).
   *
   * A conf still carrying the former `DATA_QUALITY` block is honoured rather than silently defaulted:
   * defaulting would quietly write the report somewhere else and ignore every configured rule setting.
   */
  def from(config: Config): CheckConfig = {
    val appConf = config.getConfig(PrimaryConstants.APP_CONF)
    val block =
      if (appConf.hasPath(BLOCK)) appConf.getConfig(BLOCK)
      else if (appConf.hasPath(LEGACY_BLOCK)) appConf.getConfig(LEGACY_BLOCK)
      else ConfigFactory.empty()
    val rules = if (block.hasPath("rules")) block.getConfig("rules") else ConfigFactory.empty()

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

    // The file the term structure is actually written to, resolved exactly as
    // `PrimaryUtilities.writeDataframe` resolves it — the report has to name the file it judges, and
    // a name that does not match what lands in the directory is worse than no name at all.
    val format = str(out, "format", "csv")
    val excelFormat =
      format.equalsIgnoreCase("excel") || format.equalsIgnoreCase("xlsx") ||
        format.equalsIgnoreCase("com.crealytics.spark.excel")
    val singleFile = bool(out, "singleFile", default = true)
    val outputFile =
      if (excelFormat) s"$outDir/$outName.xlsx"
      // singleFile = false leaves a DIRECTORY of Spark part files; naming the directory is the
      // truthful answer there, so say which it is rather than inventing a file that never exists.
      else if (!singleFile) s"$outDir/$outName (part-file directory)"
      else s"$outDir/$outName.${if (format.equalsIgnoreCase("csv")) "csv" else format}"

    CheckConfig(
      enabled                 = bool(block, "enabled", default = true),
      htmlPath                = str(block, "htmlPath", s"$outDir/CR_$outName.html"),
      sourcePath              = str(block, "sourcePath", s"$outDir/$outName.csv"),
      allTermsEqualOneEnabled = bool(one, "enabled", default = true),
      allTermsEqualOneRemoves = bool(one, "remove", default = true),
      tolerance               = dbl(one, "tolerance", default = 1e-9),
      negativeEnabled         = bool(neg, "enabled", default = true),
      // Widen CR02 from "strictly negative" to "non-positive". A zero rate is not impossible the
      // way a negative one is — it is the exposure fully extinguished — but the business asked to
      // see those terms named too. Default false keeps the historical < 0 scope for a conf that
      // does not mention the key.
      negativeIncludesZero    = bool(neg, "includeZero", default = false),
      maxRowsInReport         = int(neg, "maxRowsInReport", default = 500),
      // Empty by default: a negative value only exists once `allow_negative_ead_ra_rate` has been
      // opted into, and whoever opted in asked to SEE it. Masking it behind a token would hide the
      // very thing the flag was turned on for. Set a marker (e.g. "NV") only when a downstream
      // consumer cannot take a negative number.
      negativeMarker          = str(neg, "replaceWith", default = ""),
      excludeEadRaRateGe1     = bool(params, "exclude_ead_ra_rate_ge_1", default = false),
      outputFile              = outputFile
    )
  }
}

/** Cleaned output plus the report that explains what was taken out of it. */
final case class CheckOutcome(cleaned: DataFrame, report: CheckReport)

/**
 * Business coherence-check mapper for the TS_EAD_FWD term structure.
 *
 * Two responsibilities, kept deliberately apart:
 *  - [[reportOnly]] EVALUATES the rules and returns a [[CheckReport]]. It never changes a row; this is
 *    what the standalone [[com.bnp.str.tseadfwd.job.CoherenceCheckDriver]] runs against an existing CSV.
 *  - [[apply]] evaluates and ALSO returns the cleaned DataFrame. The Main job calls this one and
 *    writes the cleaned frame — so the rows leave the output in the main job, exactly once, and the
 *    report can still name every one of them.
 *
 * Rules (see [[CheckRule]]):
 *  - CR01 all terms = 1 for one (EAD_MATRIX_ID, SCENARIO_ID) -> the group's rows are removed
 *  - CR02 negative (and, with `includeZero`, zero) EAD_RA_RATE -> reported, never removed
 *
 * `EAD_RA_RATE` and `TERM` reach here as decimal-comma STRINGS (`PrimaryMapper.fmtNumber`), so every
 * comparison goes through [[numeric]] rather than a raw string test.
 */
class CoherenceCheckMapper(cfg: CheckConfig)(implicit spark: SparkSession) {

  private val log = LoggerFactory.getLogger(this.getClass)

  private val TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  /**
   * Temp column carrying the PARSED rate into the CR02 finding builder, so "was this hit a zero or
   * a negative?" is answered from the number rather than by re-parsing the decimal-comma string.
   * Underscore-prefixed: the output schema is fixed and must not gain a column by accident.
   */
  private val CR02_RATE = "__cr02_rate"

  /** Decimal-comma string -> Double; blank/absent -> null (a null is never "equal to 1"). */
  private def numeric(c: Column): Column =
    when(c.isNull || trim(c) === lit(""), lit(null).cast(DoubleType))
      .otherwise(regexp_replace(trim(c), ",", ".").cast(DoubleType))

  private def rate: Column = numeric(col(OUT_EAD_RA_RATE))

  /**
   * The CR02 predicate: strictly negative, or non-positive when `includeZero` is on.
   *
   * Defined once and used by all three CR02 code paths — the report, the marker replacement and
   * the replaced-value count — so widening the rule can never widen one of them and not the others.
   * A null rate makes the comparison null, i.e. not a hit: an unparseable cell is a finding for
   * CR01's own null handling, never a silent zero.
   */
  private def cr02Hit: Column =
    if (cfg.negativeIncludesZero) rate <= lit(0.0) else rate < lit(0.0)

  /**
   * Evaluate every rule WITHOUT touching the data. `rowsOut` equals `rowsIn`, and each result is
   * marked `applied = false` — nothing was removed.
   *
   * `outputFile` is the file the rules were run on, named in the report; it defaults to the file the
   * conf points at, which is what a standalone run reads.
   */
  def reportOnly(df: DataFrame, source: String, runId: String,
                 outputFile: String = cfg.sourcePath): CheckReport = {
    val rowsIn = df.count()
    CheckReport(
      source = source,
      runId = runId,
      generatedAt = LocalDateTime.now().format(TS),
      rowsIn = rowsIn,
      rowsOut = rowsIn,
      results = Seq(
        cr01Result(if (cfg.allTermsEqualOneEnabled) allOnesGroups(df) else Seq.empty,
          rowsRemoved = 0L, applied = false, reportOnly = true),
        ruleNegative(df)
      ),
      outputFile = outputFile
    )
  }

  /**
   * Evaluate the rules and return both the report and the cleaned output. Removal is applied for
   * CR01 when the rule is enabled and `remove = true`, followed by the `exclude_ead_ra_rate_ge_1`
   * engine option (which moved here from the mapper).
   *
   * `outputFile` is the file the cleaned frame is about to be written to, named in the report; it
   * defaults to the path resolved from the `TS_EAD_FWD` conf block.
   */
  def apply(df: DataFrame, source: String, runId: String,
            outputFile: String = cfg.outputFile): CheckOutcome = {
    val rowsIn = df.count()

    val removeCr01 = cfg.allTermsEqualOneEnabled && cfg.allTermsEqualOneRemoves
    val keys = if (cfg.allTermsEqualOneEnabled) allOnesGroups(df) else Seq.empty

    val afterCr01 = if (removeCr01 && keys.nonEmpty) dropGroups(df, keys.map(f => (f.matrixId, f.scenarioId))) else df
    val afterCr01Rows = if (removeCr01 && keys.nonEmpty) afterCr01.count() else rowsIn

    // `exclude_ead_ra_rate_ge_1`: keep only the terms where loss has started to accrue. A null rate
    // is KEPT — an unparseable value is a finding to look at, not a row to silently drop.
    val filtered = if (cfg.excludeEadRaRateGe1) afterCr01.where(rate.isNull || rate < lit(1.0)) else afterCr01
    val rowsOut = if (cfg.excludeEadRaRateGe1) filtered.count() else afterCr01Rows

    // CR02's marker goes on LAST: it turns the cell into a non-numeric token, so every numeric
    // predicate above has to have run already. The line itself stays — only its value is replaced.
    val markNegatives = cfg.negativeEnabled && cfg.negativeMarker.nonEmpty
    val replaced = if (markNegatives) filtered.where(cr02Hit).count() else 0L
    val cleaned = if (markNegatives && replaced > 0L) markNegativeValues(filtered) else filtered

    if (replaced > 0L)
      log.info(s"CR02: $replaced ${if (cfg.negativeIncludesZero) "non-positive" else "negative"} " +
        s"EAD_RA_RATE value(s) written as '${cfg.negativeMarker}'")

    val report = CheckReport(
      source = source,
      runId = runId,
      generatedAt = LocalDateTime.now().format(TS),
      rowsIn = rowsIn,
      rowsOut = rowsOut,
      results = Seq(
        cr01Result(keys, rowsRemoved = rowsIn - afterCr01Rows, applied = removeCr01),
        ruleNegative(df, valuesReplaced = replaced, marker = if (markNegatives) cfg.negativeMarker else "")
      ),
      outputFile = outputFile
    )

    if (cfg.excludeEadRaRateGe1)
      log.info(s"exclude_ead_ra_rate_ge_1 = true -> ${afterCr01Rows - rowsOut} full-exposure row(s) " +
        s"dropped after the coherence-check rules")

    CheckOutcome(cleaned, report)
  }

  // ---- CR01: all terms equal to 1 -------------------------------------------

  /**
   * The (EAD_MATRIX_ID, SCENARIO_ID) groups whose EVERY term carries `EAD_RA_RATE = 1` (within
   * [[CheckConfig.tolerance]]). A group with a single deviating — or unparseable — term is NOT flagged.
   *
   * NOTE the matrix id carries the frequency suffix (`..._Q` / `..._Y`), so the quarterly and yearly
   * curves of the same matrix are separate groups, which is what "all terms" has to mean.
   */
  private def allOnesGroups(df: DataFrame): Seq[CheckFinding] = {
    val isOne = when(rate.isNotNull && abs(rate - lit(1.0)) <= lit(cfg.tolerance), lit(1)).otherwise(lit(0))

    df.groupBy(col(OUT_MATRIX_ID), col(OUT_SCENARIO_ID))
      .agg(count(lit(1)).as("terms"), sum(isOne).as("ones"))
      .where(col("terms") > lit(0) && col("terms") === col("ones"))
      .orderBy(col(OUT_MATRIX_ID), col(OUT_SCENARIO_ID))
      .collect()
      .map { r =>
        val terms = r.getAs[Long]("terms")
        CheckFinding(
          matrixId = r.getAs[String](OUT_MATRIX_ID),
          scenarioId = r.getAs[String](OUT_SCENARIO_ID),
          term = "",
          value = "1",
          detail = s"all $terms term(s) equal 1 (full exposure over the whole curve)")
      }
      .toSeq
  }

  private def cr01Result(findings: Seq[CheckFinding], rowsRemoved: Long, applied: Boolean,
                        reportOnly: Boolean = false): CheckRuleResult =
    CheckRuleResult(
      rule = CheckRule.AllTermsEqualOne,
      enabled = cfg.allTermsEqualOneEnabled,
      total = findings.size.toLong,
      findings = findings, // groups are few; never truncated
      rowsRemoved = rowsRemoved,
      // `applied` means the removal step RAN for this rule — not that it found something to drop.
      // Conflating the two made a clean run report "removal disabled in the configuration".
      applied = applied,
      reportOnly = reportOnly)

  // ---- CR02: negative EAD_RA_RATE -------------------------------------------

  /**
   * Every output row whose `EAD_RA_RATE` is strictly negative — or non-positive, when
   * `includeZero` is on. Reporting only: these rows stay in the output, because the term exists
   * and dropping it would leave a hole in the curve. The listing is capped at
   * [[CheckConfig.maxRowsInReport]]; the total count is not.
   *
   * The two hits mean different things and are named apart in the report: a NEGATIVE rate is
   * non-physical, whereas a ZERO rate is a real state — the exposure fully run off — that the
   * business wants listed rather than treated as an error.
   *
   * On the negative side this rule finds nothing under the engine's default settings, by
   * construction: the run-off freeze truncates the RA series before the cumulative product can go
   * negative, and a sub-zero product would be reported as 1 anyway. Set
   * `parameters.allow_negative_ead_ra_rate = true` to let those values through — see
   * [[com.bnp.str.tseadfwd.mapping.PrimaryView.vectorFactored]]. Zeros need no such switch: a
   * cumulative product can reach 0 (or round to it at the emitted scale) on its own.
   */
  private def ruleNegative(df: DataFrame, valuesReplaced: Long = 0L, marker: String = ""): CheckRuleResult = {
    if (!cfg.negativeEnabled)
      return CheckRuleResult(CheckRule.NegativeEadRaRate, enabled = false, 0L, Seq.empty, 0L, applied = false)

    val negatives = df.where(rate.isNotNull && cr02Hit)
    val total = negatives.count()

    val listed =
      if (total == 0L) Seq.empty[CheckFinding]
      else negatives
        .orderBy(col(OUT_MATRIX_ID), col(OUT_SCENARIO_ID), numeric(col(OUT_TERM)))
        .withColumn(CR02_RATE, rate)
        .limit(cfg.maxRowsInReport)
        .collect()
        .map { r =>
          CheckFinding(
            matrixId = r.getAs[String](OUT_MATRIX_ID),
            scenarioId = r.getAs[String](OUT_SCENARIO_ID),
            term = r.getAs[String](OUT_TERM),
            value = r.getAs[String](OUT_EAD_RA_RATE),
            detail =
              if (r.getAs[Double](CR02_RATE) == 0.0) "zero exposure factor (exposure fully run off)"
              else "negative exposure factor")
        }
        .toSeq

    CheckRuleResult(CheckRule.NegativeEadRaRate, enabled = true, total, listed, rowsRemoved = 0L,
      applied = false, valuesReplaced = valuesReplaced, marker = marker)
  }

  // ---- removal --------------------------------------------------------------

  /**
   * Replace a rate the rule fired on with the configured marker (e.g. `NV`), in place, keeping the
   * column where it is. The row survives: the term exists, and dropping it would leave a hole in the
   * curve — what must not survive is a number a consumer could mistake for a real exposure factor.
   *
   * Scope follows [[cr02Hit]], so with `includeZero` a masked zero is masked too: the marker means
   * "this rule fired here", and a reader must not have to know which half of the rule it was.
   */
  private def markNegativeValues(df: DataFrame): DataFrame =
    df.withColumn(OUT_EAD_RA_RATE,
      when(cr02Hit, lit(cfg.negativeMarker)).otherwise(col(OUT_EAD_RA_RATE)))

  /** Anti-join the flagged (matrix, scenario) keys out of the output. */
  private def dropGroups(df: DataFrame, keys: Seq[(String, String)]): DataFrame = {
    import spark.implicits._
    val keysDf = keys.toDF(OUT_MATRIX_ID, OUT_SCENARIO_ID)
    df.join(broadcast(keysDf), Seq(OUT_MATRIX_ID, OUT_SCENARIO_ID), "left_anti")
  }
}
