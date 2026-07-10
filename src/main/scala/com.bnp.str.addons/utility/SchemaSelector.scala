package com.bnp.str.addons.utility

import org.apache.spark.sql.types._

trait SchemaSelector {

  def getSchema(tableName: String) : StructType = {
    tableName.toUpperCase match {
      case "ADD_ON_APPLICATION" => add_on_application_schema
      case "ADD_ON_ACTION" => add_on_action_schema
      case "ADD_ON_PERIMETER" => add_on_perimeter_schema
      case other => throw new IllegalArgumentException(
        s"No schema defined for table '$other'. " +
          "Expected one of: ADD_ON_APPLICATION, ADD_ON_ACTION, ADD_ON_PERIMETER")
    }
  }

  val add_on_application_schema = StructType(List(
    StructField("scenario", StringType, true),
    StructField("variable", StringType, true),
    StructField("date", StringType, true),
    StructField("value", DoubleType, true),
    StructField("maxScenarios", StringType, true)
  ))


  val add_on_action_schema = StructType(List(
    StructField("scenario", StringType, true),
    StructField("variable", StringType, true),
    StructField("date", ArrayType(StringType, true), true),
    StructField("value", ArrayType(DoubleType, true), true)
  ))


  val add_on_perimeter_schema = StructType(Array(
    StructField("layer_1_risk", StringType, true),
    StructField("division_group", StringType, true),
    StructField("risk_id_perimeter", StringType, true),
    StructField("risk_inventory", StringType, true),
    StructField("flag_fortis_sub_conso_for_ic_purpose", StringType, true),
    StructField("flag_bgl_sub_conso_for_ic_purpose", StringType, true),
    StructField("flag_bnl_sub_consolidated_for_ic_purpose", StringType, true),
    StructField("flag_pf_for_ic_purpose", StringType, true),
    StructField("risk_id_exercise", StringType, true),
    StructField("identifier", StringType, true),
    StructField("status", StringType, true),
    StructField("new", StringType, true),
    StructField("creation_date", StringType, true),
    StructField("modification_date", StringType, true),
    StructField("review_date", StringType, true),
    StructField("creator", StringType, true),
    StructField("last_editor", StringType, true),
    StructField("industry_sectors", StringType, true),
    StructField("business_line", StringType, true),
    StructField("main_legal_entities", StringType, true),
    StructField("product_or_business_activities", StringType, true),
    StructField("hashtags", StringType, true),
    StructField("risk_type_level_1", StringType, true),
    StructField("risk_type_level_2", StringType, true),
    StructField("risk_type_level_3", StringType, true),
    StructField("correlations", StringType, true),
    StructField("title", StringType, true),
    StructField("identifier_aux", StringType, true),
    StructField("scenario", StringType, true),
    StructField("pi_reference", StringType, true),
    StructField("layer2", StringType, true),
    StructField("intragroup", StringType, true),
    StructField("risk_driver_1", StringType, true),
    StructField("risk_driver_1_comment", StringType, true),
    StructField("risk_driver_1_weight", StringType, true),
    StructField("risk_driver_1_mat", StringType, true),
    StructField("risk_driver_1_effect", StringType, true),
    StructField("risk_driver_1_transmission_channel", StringType, true),
    StructField("risk_driver_2", StringType, true),
    StructField("risk_driver_2_comment", StringType, true),
    StructField("risk_driver_2_weight", StringType, true),
    StructField("risk_driver_2_mat", StringType, true),
    StructField("risk_driver_2_effect", StringType, true),
    StructField("risk_driver_2_transmission_channel", StringType, true),
    StructField("risk_driver_3", StringType, true),
    StructField("risk_driver_3_comment", StringType, true),
    StructField("risk_driver_3_weight", StringType, true),
    StructField("risk_driver_3_mat", DoubleType, true),
    StructField("risk_driver_3_effect", StringType, true),
    StructField("risk_driver_3_transmission_channel", StringType, true),
    StructField("risk_driver_4", StringType, true),
    StructField("risk_driver_4_comment", StringType, true),
    StructField("risk_driver_4_weight", StringType, true),
    StructField("risk_driver_4_mat", DoubleType, true),
    StructField("risk_driver_4_effect", StringType, true),
    StructField("risk_driver_4_transmission_channel", StringType, true),
    StructField("risk_driver_5", StringType, true),
    StructField("risk_driver_5_comment", StringType, true),
    StructField("risk_driver_5_weight", StringType, true),
    StructField("risk_driver_5_mat", DoubleType, true),
    StructField("risk_driver_5_effect", StringType, true),
    StructField("risk_driver_5_transmission_channel", StringType, true),
    StructField("comments_on_risk_drivers", StringType, true),
    StructField("severity_metric", StringType, true),
    StructField("severity_bucket", StringType, true),
    StructField("severity_amount", DoubleType, true),
    StructField("implied_severity", DoubleType, true),
    StructField("frequency_bucket", StringType, true),
    StructField("frequency_years", StringType, true),
    StructField("implied_frequency", StringType, true),
    StructField("imminence", StringType, true),
    StructField("probability", DoubleType, true),
    StructField("comments_on_materiality_assessment", StringType, true),
    StructField("direction_of_the_risk", StringType, true),
    StructField("long_term_relative_level", StringType, true),
    StructField("prevention", StringType, true),
    StructField("impact_of_prevention", StringType, true),
    StructField("mitigation", StringType, true),
    StructField("impact_of_mitigation", StringType, true),
    StructField("action_plan", StringType, true),
    StructField("materiality_ttc", DoubleType, true),
    StructField("materiality", DoubleType, true),
    StructField("id_risk_event", DoubleType, true),
    StructField("overlap", StringType, true),
    StructField("materiality_status", StringType, true),
    StructField("trigger_variable", StringType, true),
    StructField("trigger_variable_direction", StringType, true),
    StructField("remove_event", StringType, true),
    StructField("type_of_events", StringType, true),
    StructField("type_of_climate_risk", StringType, true),
    StructField("ssm_scope", StringType, true),
    StructField("materiality_q4_colonne_cc_materiality_du_fichier_q4", StringType, true),
    StructField("var_materiality", StringType, true),
    StructField("comments", StringType, true),
    StructField("merge_check", StringType, true),
    StructField("mismatch", DoubleType, true)
  ))

}
