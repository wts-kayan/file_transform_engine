package com.bnp.str.tseadfwd.common

import org.apache.spark.sql.DataFrame

abstract class MapperProvider() extends Serializable {


  def getDataFrame: DataFrame

  def getMapping_tseadfwd: DataFrame = {
    getDataFrame
  }
}
