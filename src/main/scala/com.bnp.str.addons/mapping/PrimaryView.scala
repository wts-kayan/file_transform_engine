package com.bnp.str.addons.mapping

import com.bnp.str.addons.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object PrimaryView {
  private val log = LoggerFactory.getLogger(this.getClass)
  def loadQuery(queryName: String)(implicit sparkSession: SparkSession, config: Config): String = {
    // Get the path to the SQL queries file
    val sqlQueriesPath = config.getString(s"${PrimaryConstants.APP_CONF}.sql_queries.path")
    // Load the SQL queries configuration (read via Hadoop FS so local/HDFS paths both work)
    val configPath = PrimaryUtilities.getHdfsReader(sqlQueriesPath)(sparkSession.sparkContext)
    val sqlQueriesConfig = ConfigFactory.parseReader(configPath)
    // Get the specified query
    val sql = sqlQueriesConfig.getString(s"${PrimaryConstants.APP_CONF}.$queryName").stripMargin
    log.info(s"$queryName = ${sql}")
    sql
  }
  // Get the primary view query

  /**
   * Join query behind `crr_param_add_on_ste`.
   *
   * Ordering note: the sheets are read with `inferSchema=false`, so IMPACT_RANK arrives as TEXT and
   * a bare `ORDER BY` on it would sort lexicographically (1, 14, 15, 2, 20...). It is therefore cast
   * to INT for the sort; ranks that are blank or non-numeric cast to NULL and are pushed to the end
   * (NULLS LAST) instead of leading the file. ACTION_ID / PERIMETER_ID / PD_MODEL_NAME stay as
   * tie-breakers within a rank. The projected RANK column itself keeps its original text value.
   */

  val get_on_application_active_ind: String =
    s"""
       |SELECT
       |  app.ACTION_ID as ADDON_ID
       |, app.PERIMETER_ID
       |
       |, perimeter.PD_MODEL_NAME
       |, perimeter.PD_MODEL_NAME_5
       |, perimeter.LGD_MODEL_NAME
       |, perimeter.EAD_MODEL_NAME
       |, perimeter.ORIGIN_FLG
       |, perimeter.UNPAID_FLG
       |, perimeter.ACCOUNTING_SITE_CODE_POST_ACC
       |, perimeter.PRODUCT_CODE
       |, perimeter.CONF_FLG
       |, perimeter.SECURIT_POS_FLG
       |, perimeter.PRODUCT_TYPE
       |
       |, action.OPERAND
       |, action.VARIABLE
       |, action.FACTOR
       |
       |, app.IMPACT_RANK as RANK
       |, app.ACTIVE_IND as ACTIVATE
       |FROM ${PrimaryConstants.VIEW_ADD_ON_APPLICATION} app
       |LEFT JOIN ${PrimaryConstants.VIEW_ADD_ON_ACTION} action
       |ON app.ACTION_ID = action.ACTION_ID
       |
       |LEFT JOIN ${PrimaryConstants.VIEW_ADD_ON_PERIMETER} perimeter
       |ON app.PERIMETER_ID = perimeter.PERIMETER_ID
       |ORDER BY
       |  CAST(app.IMPACT_RANK AS INT) ASC NULLS LAST,
       |  app.ACTION_ID,
       |  app.PERIMETER_ID,
       |  perimeter.PD_MODEL_NAME
       |""".stripMargin

}
