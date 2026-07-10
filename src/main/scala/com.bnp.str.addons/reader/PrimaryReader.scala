package com.bnp.str.addons.reader

import com.bnp.str.addons.utility.PrimaryConstants
import com.bnp.str.addons.utility.PrimaryUtilities.readDataFrameFromExcel
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Reads the three addons input sheets (add_on_application / add_on_action / add_on_perimeter) as
 * lazily-materialized DataFrames and hands them to the runner by name.
 */
class PrimaryReader()(implicit sparkSession: SparkSession, conf: Config) {

  private lazy val add_on_application: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.ADD_ON_APPLICATION)(sparkSession, conf)

  private lazy val add_on_action: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.ADD_ON_ACTION)(sparkSession, conf)

  private lazy val add_on_perimeter: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.ADD_ON_PERIMETER)(sparkSession, conf)

  def getMappingReader(input: String): DataFrame = input match {
    case PrimaryConstants.ADD_ON_APPLICATION => add_on_application
    case PrimaryConstants.ADD_ON_ACTION      => add_on_action
    case PrimaryConstants.ADD_ON_PERIMETER   => add_on_perimeter
    case _ => throw new IllegalArgumentException(
      s"Invalid input '$input'. Expected one of: ${PrimaryConstants.ADD_ON_APPLICATION}, " +
        s"${PrimaryConstants.ADD_ON_ACTION}, ${PrimaryConstants.ADD_ON_PERIMETER}")
  }
}
