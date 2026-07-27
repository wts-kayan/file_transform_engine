package com.bnp.str.ageing.utility

object PrimaryConstants {
  val APPLICATION_NAME = "AgeingMacroeconomicScenarios"

  // root config block for this module
  val APP_CONF = "ageing_macroeconomic_scenarios_app"

  // config keys (relative to APP_CONF)
  val MACRO_INPUT_PATH = "macroeconomic.path"
  val OUTPUT_PATH = "output.macroeconomic.path"
  val INITIAL_AS_OF_DATE = "as_of_date.initial"
  val AGEING_AS_OF_DATE = "as_of_date.ageing"

  // the central (unshocked) scenario sheet name
  val CENTRAL = "Central"

  // the quarterly date column shared by every scenario sheet
  val COL_DATE = "Date"

  // number of quarters projected forward from an as-of date
  val PROJECTION_QUARTERS = 12
}
