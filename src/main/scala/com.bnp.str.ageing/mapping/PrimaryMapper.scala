package com.bnp.str.ageing.mapping

import com.bnp.str.ageing.common.MapperProvider
import com.bnp.str.ageing.mapping.PrimaryView.{applyAgeing, buildShock}
import org.apache.spark.sql.{DataFrame, SparkSession}

class PrimaryMapper(dfCentral: DataFrame,
                    df: DataFrame,
                    dateForShocks: Seq[String],
                    shockToAge: Map[String, String])
                   (implicit sparkSession: SparkSession) extends MapperProvider {

  override def getDataFrame_ageing: DataFrame = {
    val shock = buildShock(df, dfCentral, dateForShocks, shockToAge)
    applyAgeing(dfCentral, shock)
  }
}
