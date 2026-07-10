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
       |FROM add_on_application_view app
       |LEFT JOIN add_on_action_view action
       |ON app.ACTION_ID = action.ACTION_ID
       |
       |LEFT JOIN add_on_perimeter_view perimeter
       |ON app.PERIMETER_ID = perimeter.PERIMETER_ID
       |ORDER BY
       |  app.ACTION_ID,
       |  app.PERIMETER_ID,
       |  perimeter.PD_MODEL_NAME
       |""".stripMargin

}
