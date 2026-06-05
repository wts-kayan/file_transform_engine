package com.bnp.str.tseadfwd.utility

object PrimaryConstants {

  final val APPLICATION_NAME = "file_transform_engine"

  // ---- input table identifiers (config keys + reader switch) ----
  final val RA_BCEF = "RA_BCEF"
  final val RA_BGL = "RA_BGL"
  final val RA_BNL = "RA_BNL"
  final val RA_FORTIS = "RA_FORTIS"
  final val RA_LS = "RA_LS"

  final val MACRO_VARIABLE = "MACRO_VARIABLE"
  final val PARAMETRAGE = "PARAMETRAGE"

  final val ADD_ON_PERIMETER = "add_on_perimeter"
  final val MODE_APPEND = "Append"
  final val APP_CONF = "tseadfwd_app"

  // ---- output ----
  final val CRR_PARAM_ADD_ON_STE = "crr_param_add_on_ste"
  final val OUTPUT_EAD_FWD = "TS_EAD_FWD"

  // ---- RA input column / value names ----
  final val COL_PERIMETER = "PERIMETER"
  final val COL_SEGMENT = "SEGMENT"
  final val COL_RATE_TYPE = "RATE_TYPE"
  final val COL_FWL_TYPE = "FWL_TYPE"
  final val COL_METRIC = "METRIC"

  final val FWL_BASELINE = "BASELINE"
  final val FWL_STRESS_PLUS = "STRESS (+)"
  final val FWL_STRESS_MINUS = "STRESS (-)"

  final val METRIC_CRD = "CRD"
  final val METRIC_RA_STAT = "RA STAT"
  final val METRIC_RA_FI = "RA FI"
  final val METRIC_RE = "RE"

  // ---- PARAMETRAGE column names ----
  final val COL_AGGREGATION = "AGGREGATION"
  final val COL_AGG_SEGMENT_NAME = "AGGREGATED_SEGMENT_NAME"
  final val COL_FWL_TO_BE_APPLIED = "FWL_TO_BE_APPLIED"
  final val COL_MACRO_VARIABLE = "MACRO_VARIABLE"

  final val YES = "YES"
  final val NO = "NO"

  // ---- scenario CSV column names + Scenario_ID mapping ----
  final val COL_SCEN_DATE = "Date"
  final val COL_SCEN_NAME = "scenario"

  /** scenario long name -> Scenario_ID code. S (Secto) intentionally omitted for now. */
  final val SCENARIO_CODES: Seq[(String, String)] = Seq(
    "Central"    -> "C",
    "Adverse"    -> "A",
    "Optimistic" -> "O",
    "Extreme"    -> "E"
  )
  final val SCENARIO_CENTRAL = "Central"
  /** Scenario whose FWL=YES shock blends toward STRESS (+); all others (Adverse/Extreme) use STRESS (-). */
  final val SCENARIO_OPTIMISTIC = "Optimistic"

  // ---- output columns ----
  final val OUT_MATRIX_ID = "EAD_MATRIX_ID"
  final val OUT_SCENARIO_ID = "SCENARIO_ID"
  final val OUT_TERM = "TERM"
  final val OUT_EAD_RA_RATE = "EAD_RA_RATE"
}
