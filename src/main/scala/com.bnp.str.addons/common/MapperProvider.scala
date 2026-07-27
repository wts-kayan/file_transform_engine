package com.bnp.str.addons.common

import org.apache.spark.sql.DataFrame

abstract class MapperProvider() extends Serializable {

  def getDataFrame_addons: DataFrame

  def getMapping_addons : DataFrame = {
    getDataFrame_addons
  }

}
