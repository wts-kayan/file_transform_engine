package com.bnp.str.ageing.common

import org.apache.spark.sql.DataFrame

abstract class MapperProvider() extends Serializable {

  def getDataFrame_ageing: DataFrame

  def getMapping_ageing: DataFrame = {
    getDataFrame_ageing
  }

}
