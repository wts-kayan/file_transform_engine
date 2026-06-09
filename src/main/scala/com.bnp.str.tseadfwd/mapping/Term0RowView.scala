package com.bnp.str.tseadfwd.mapping

/**
 * One computation breakdown for a single (matrix, scenario, term), produced by
 * [[PrimaryMapper.term0AnalysisRows]] and consumed by the analysis generator job
 * ([[com.bnp.str.tseadfwd.job.Term0AnalysisDriver]]).
 *
 * The term grid is configurable: term 0 maps to the first quarter Q1 (period 1), term 0.25 to Q2,
 * etc. (Quarterly). Per period the aggregation uses `Q1 = M1 + M2/2` (RA metrics) / `mean(M1..M3)`
 * (CRD) for the first period and the half-weight window thereafter — see [[PrimaryView.aggregate]].
 * For the Central scenario (and every scenario when FWL=NO) there is no macro shock, so `delta`,
 * `legCrdAgg`, `legFiAgg`, `legReAgg` are `NaN`/0. For an FWL=YES non-Central scenario
 * (`usesShock = true`) the stress-leg aggregates and the macro `delta` drive the shock.
 *
 * Every numeric field is computed through the same validated parsing + [[PrimaryView]] formulas as
 * the production output, so `ead` equals the production `EAD_RA_RATE` at that term. `engineEad` /
 * `status` are filled by the driver when an `enginePath` is configured: the analysis `ead` is then
 * reconciled against the REAL engine output file (`MATCH` / `DIFF` / `MISSING`), so a bad input
 * parse surfaces as a `DIFF` rather than a silently-wrong-but-self-consistent number.
 *
 * `crdMonths` / `statMonths` / `fiMonths` / `reMonths` are the first 3 raw baseline months (a fixed
 * sample of the parsed input, the same for every term row of a matrix) — kept to spot parse issues.
 */
case class Term0RowView(
                         matrixId: String,
                         scenarioName: String,
                         scenarioCode: String,
                         fwlApplied: Boolean,
                         macroVar: String,
                         usesShock: Boolean,
                         segments: Seq[String],
                         rateType: String,
                         term: Double,
                         period: Int,
                         // raw baseline input sample (first 3 months per metric) — surfaces parse issues
                         crdMonths: Seq[Double],
                         statMonths: Seq[Double],
                         fiMonths: Seq[Double],
                         reMonths: Seq[Double],
                         // aggregated baseline metrics at this term's period
                         crdAgg: Double,
                         statAgg: Double,
                         fiAgg: Double,
                         reAgg: Double,
                         // stress-leg aggregates at this period (NaN unless usesShock)
                         legCrdAgg: Double,
                         legFiAgg: Double,
                         legReAgg: Double,
                         // per-term result
                         delta: Double,
                         ra: Double,
                         vector: Double,
                         ead: Double,
                         // engine reconciliation (filled by the driver when enginePath is set)
                         engineEad: Double = Double.NaN,
                         status: String = ""
                       )
