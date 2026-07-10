package com.bnp.str.addons.mapping

import com.bnp.str.addons.common.MapperProvider
import com.bnp.str.addons.mapping.PrimaryView.{get_on_application_active_ind, loadQuery}
import com.bnp.str.addons.utility.PrimaryConstants
import com.typesafe.config.Config
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Builds the `crr_param_add_on_ste` output: registers the (filtered) input sheets as temp views
 * and runs the join query — either the built-in [[PrimaryView.get_on_application_active_ind]] or,
 * when `queryName` is set in the output config, a query loaded from the SQL queries file.
 */
class PrimaryMapper(add_on_application: DataFrame,
                    add_on_action: DataFrame,
                    add_on_perimeter: DataFrame,
                    outputTableName: String)
                   (implicit sparkSession: SparkSession, config: Config) extends MapperProvider {

  override def getDataFrame_addons: DataFrame = {
    add_on_application
      .where(col(PrimaryConstants.COL_ACTIVE_IND) === PrimaryConstants.ACTIVE_IND_ON)
      .createOrReplaceTempView(PrimaryConstants.VIEW_ADD_ON_APPLICATION)
    add_on_action
      .where(col(PrimaryConstants.COL_VARIABLE) =!= PrimaryConstants.VARIABLE_CCF)
      .createOrReplaceTempView(PrimaryConstants.VIEW_ADD_ON_ACTION)
    add_on_perimeter
      .createOrReplaceTempView(PrimaryConstants.VIEW_ADD_ON_PERIMETER)

    val outConfig = config.getConfig(s"${PrimaryConstants.APP_CONF}.$outputTableName")
    val queryName = outConfig.getString("queryName")

    val sql =
      if (queryName.nonEmpty) loadQuery(queryName)(sparkSession, config)
      else get_on_application_active_ind

    sparkSession.sql(sql)
  }
}
