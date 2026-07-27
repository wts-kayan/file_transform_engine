package com.bnp.str.ageing.mapping

import com.bnp.str.ageing.utility.PrimaryConstants
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object PrimaryView {

  def buildShock(df: DataFrame,
                 dfCentral: DataFrame,
                 dateForShocks: Seq[String],
                 shockToAge: Map[String, String])(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val dateCol = PrimaryConstants.COL_DATE
    val varMacros = df.schema.fields.map(_.name).filter(_ != dateCol)

    var shock = df
      .join(dfCentral, Seq(dateCol))
      .drop(dfCentral.col(dateCol))
      .filter(col(dateCol).isin(dateForShocks: _*))

    val diffRequest: Array[Column] = col(dateCol) +: varMacros.map(varMacro =>
      (df.col(varMacro) - dfCentral(varMacro)).as(varMacro))
    shock = shock.select(diffRequest: _*)

    val dateMappingDf = shockToAge.toSeq
      .toDF("date_used_for_computing_stress", "date_where_the_stress_should_be_applied")

    shock = shock
      .join(broadcast(dateMappingDf), col(dateCol) === $"date_used_for_computing_stress")
      .drop(dateCol, "date_used_for_computing_stress")

    shock = shock.persist()
    shock.count()
    shock
  }

  def applyAgeing(dfCentral: DataFrame,
                  shock: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val dateCol = PrimaryConstants.COL_DATE
    val varMacros = dfCentral.schema.fields.map(_.name).filter(_ != dateCol)

    var res = dfCentral
      .join(broadcast(shock), col(dateCol) === $"date_where_the_stress_should_be_applied", "left")

    val selectRequest: Array[Column] = col(dateCol) +: varMacros.map(varMacro =>
      when(shock.col(varMacro).isNotNull, dfCentral.col(varMacro) + shock.col(varMacro))
        .otherwise(dfCentral.col(varMacro))
        .as(varMacro))

    res = res.select(selectRequest: _*)
    res = res.persist()
    res.count()
    res
  }
}
