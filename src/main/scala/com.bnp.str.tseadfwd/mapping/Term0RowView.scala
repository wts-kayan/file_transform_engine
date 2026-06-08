package com.bnp.str.tseadfwd.mapping

/**
 * One Term-0 (period 1) computation breakdown for a single (matrix, scenario), produced by
 * [[PrimaryMapper.term0AnalysisRows]] and consumed by the analysis generator job
 * ([[com.bnp.str.tseadfwd.job.Term0AnalysisDriver]]).
 *
 * Term 0 maps to the first quarter Q1 (Quarterly) — the aggregation uses `Q1 = M1 + M2/2` for the
 * RA metrics and `mean(M1,M2,M3)` for CRD (see [[PrimaryView.aggregate]]). For the Central scenario
 * (and every scenario when FWL=NO) there is no macro shock: `delta0`, `legCrdQ1`, `legFiQ1`,
 * `legReQ1` are then `NaN`/0 and the loss rate is the plain rule. For an FWL=YES non-Central
 * scenario (`usesShock = true`) the stress-leg Q1 values and the term-0 macro delta drive the shock.
 *
 * Every numeric field is computed through the same validated parsing + [[PrimaryView]] formulas as
 * the production output, so `ead0` equals the production `EAD_RA_RATE` at TERM 0.
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
                     // baseline monthly inputs feeding the Q1 aggregation (first 3 months per metric)
                     crdMonths: Seq[Double],
                     statMonths: Seq[Double],
                     fiMonths: Seq[Double],
                     reMonths: Seq[Double],
                     // aggregated Q1 baseline values
                     crdQ1: Double,
                     statQ1: Double,
                     fiQ1: Double,
                     reQ1: Double,
                     // stress-leg Q1 values (NaN unless usesShock)
                     legCrdQ1: Double,
                     legFiQ1: Double,
                     legReQ1: Double,
                     // term-0 result
                     delta0: Double,
                     ra0: Double,
                     vector0: Double,
                     ead0: Double
                   )
