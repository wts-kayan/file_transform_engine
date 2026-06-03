package com.bnp.str.tseadfwd.reader

import com.typesafe.config.Config
import com.bnp.str.tseadfwd.utility.{PrimaryConstants, SchemaSelector}
import com.bnp.str.tseadfwd.utility.PrimaryUtilities._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

class PrimaryReader()(implicit sparkSession: SparkSession, conf: Config)
  extends SchemaSelector {

  private val log = LoggerFactory.getLogger(this.getClass)

  val addonInputConfig = conf.getConfig(s"${PrimaryConstants.APP_CONF}")

  private lazy val ra_bcef: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.RA_BCEF)(sparkSession, conf)
/*
  private lazy val ra_bgl: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.RA_BGL)(sparkSession, conf)

  private lazy val ra_bnl: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.RA_BNL)(sparkSession, conf)

  private lazy val ra_fortis: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.RA_FORTIS)(sparkSession, conf)

  private lazy val ra_ls: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.RA_LS)(sparkSession, conf)

 */

  private lazy val macro_variable: DataFrame =
    readScenarioFromExcelSheets(PrimaryConstants.MACRO_VARIABLE)(sparkSession, conf)

  private lazy val parametrage: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.PARAMETRAGE)(sparkSession, conf)

  def getMappingReader(input: String): DataFrame = {
    input.toUpperCase match {
      case "RA_BCEF"        => ra_bcef
      //case "RA_BGL"         => ra_bgl
      //case "RA_BNL"         => ra_bnl
      //case "RA_FORTIS"      => ra_fortis
      //case "RA_LS"          => ra_ls
      case "MACRO_VARIABLE" => macro_variable
      case "PARAMETRAGE"    => parametrage
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid input $input. expected ..."
        )
    }
  }
}