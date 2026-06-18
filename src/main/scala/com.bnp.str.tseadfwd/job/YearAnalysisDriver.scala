package com.bnp.str.tseadfwd.job

import com.bnp.str.tseadfwd.mapping.PrimaryView

/**
 * Spark job that auto-generates the EAD_RA_RATE computation breakdown on the YEARLY grid (term step 1,
 * term 0 = Y1), as a Markdown narrative + decimal-comma CSV. It is the yearly twin of
 * [[Term0AnalysisDriver]]: same inputs, same validated parsing and [[PrimaryView]] formulas, same
 * per-scenario STEP-4 shock sign — only the aggregation frequency differs (yearly RA = SUM over the
 * window, Y1 = 6 months / Yn = 12 months; CRD = MEAN). The whole pipeline is shared via
 * [[Term0AnalysisDriver.run]]; this object only fixes the frequency, config block and output paths.
 *
 * Single argument: the path to `application.conf`. Configuration is read from the
 * `tseadfwd_app.YEAR_ANALYSIS` block (same keys as `TERM0_ANALYSIS`: enabled / terms / enginePath /
 * tol / mdPath / csvPath); `terms` is interpreted as YEARS, e.g. `[0, 1, 5, 10, 30]`.
 */
object YearAnalysisDriver {

  def main(args: Array[String]): Unit =
    Term0AnalysisDriver.run(
      confPath   = args.lift(0).getOrElse("localRun/tseadfwd/application.conf"),
      appName    = "year-analysis",
      blockName  = "YEAR_ANALYSIS",
      freq       = PrimaryView.Yearly,
      defaultMd  = "localRun/tseadfwd/output/ANALYSIS_YEAR_GENERATED.md",
      defaultCsv = "localRun/tseadfwd/output/ANALYSIS_YEAR_GENERATED.csv")
}
