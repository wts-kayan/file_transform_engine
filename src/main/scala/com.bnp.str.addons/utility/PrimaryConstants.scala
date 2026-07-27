package com.bnp.str.addons.utility

object PrimaryConstants {
  val APPLICATION_NAME = "file_transform_engine"

  // input sheet identifiers (config keys + reader switch)
  val ADD_ON_APPLICATION = "add_on_application"
  val ADD_ON_ACTION = "add_on_action"
  val ADD_ON_PERIMETER = "add_on_perimeter"

  // temp-view names shared by the mapper (createOrReplaceTempView) and the join SQL
  val VIEW_ADD_ON_APPLICATION = "add_on_application_view"
  val VIEW_ADD_ON_ACTION = "add_on_action_view"
  val VIEW_ADD_ON_PERIMETER = "add_on_perimeter_view"

  // filter columns / values used to build the output
  val COL_ACTIVE_IND = "ACTIVE_IND"
  val COL_VARIABLE = "VARIABLE"
  val VARIABLE_CCF = "CCF"
  val ACTIVE_IND_ON = "1"

  val MODE_APPEND = "Append"
  val APP_CONF = "addons_app"

  val CRR_PARAM_ADD_ON_STE = "crr_param_add_on_ste"

}
