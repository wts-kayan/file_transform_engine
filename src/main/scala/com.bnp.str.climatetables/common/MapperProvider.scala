package com.bnp.str.climatetables.common

import com.bnp.str.climatetables.utility.DataframesResult

abstract class MapperProvider() extends Serializable {


  def getDataFrames_climatetables: DataframesResult

  def getMapping_climatetables: DataframesResult = {
    getDataFrames_climatetables
  }

}
