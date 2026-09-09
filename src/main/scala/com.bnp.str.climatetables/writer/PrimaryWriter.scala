package com.bnp.str.climatetables.writer

import com.bnp.str.climatetables.utility.{DataframesResult, PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

class PrimaryWriter()(implicit sparkSession: SparkSession, conf: Config) {

  private val log = LoggerFactory.getLogger(this.getClass)

  private val baseName = conf.getConfig(PrimaryConstants.APP_CONF).getConfig(PrimaryConstants.OUTPUT_TABLES).getString(PrimaryConstants.BASE_FOLDER_NAME)

  private def getOutputTableName(date: String, scenario: String, dataType: String): String = {
    val prefix = PrimaryConstants.ICAAP_TR_PREFIX
    val baseSuffix = {
      // Strip the prefix, then keep everything after the first '_' (the original date part)
      val withoutPrefix = baseName.stripPrefix(prefix)
      // Find the first '_' that separates the original date from the rest
      val firstUnderscoreIdx = withoutPrefix.indexOf('_')
      if (firstUnderscoreIdx >= 0) withoutPrefix.substring(firstUnderscoreIdx) // includes leading '_'
      else "" // fallback - no suffix
    }

    s"${prefix}${date}_${scenario}_${dataType}${baseSuffix}"
  }

  private def getDirectoryPath(date: String, scenario: String): String = {
    val outConfig = conf.getConfig(s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.OUTPUT_TABLES}")
    val baseDirectory = outConfig.getString("directory_path")
    val prefix = PrimaryConstants.ICAAP_TR_PREFIX
    val baseSuffix = {
      // Strip the prefix, then keep everything after the first '_' (the original date part)
      val withoutPrefix = baseName.stripPrefix(prefix)
      // Find the first '_' that separates the original date from the rest
      val firstUnderscoreIdx = withoutPrefix.indexOf('_')
      if (firstUnderscoreIdx >= 0) withoutPrefix.substring(firstUnderscoreIdx) // includes leading '_'
      else "" // fallback - no suffix
    }

    s"${baseDirectory}/${prefix}${date}_${scenario}${baseSuffix}"
  }

  def write(dataframesResult: DataframesResult)(sparkSession: SparkSession, conf: Config): Unit = {
    val folderNameFem = getOutputTableName(dataframesResult.date, dataframesResult.scenario, dataframesResult.dataTypeDf1)
    val folderNameTiers = getOutputTableName(dataframesResult.date, dataframesResult.scenario, dataframesResult.dataTypeDf2)
    val folderNameTerm = getOutputTableName(dataframesResult.date, dataframesResult.scenario, "TERM")
    val directoryPath = getDirectoryPath(dataframesResult.date, dataframesResult.scenario)

    PrimaryUtilities.writeDataframe(dataframesResult.df1, directoryPath, folderNameFem)(sparkSession, conf)
    PrimaryUtilities.writeDataframe(dataframesResult.df2, directoryPath, folderNameTiers)(sparkSession, conf)

    val pathWithTermToCopy = conf.getString(s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.INPUT_TERM_TABLE_FOLDER}.${PrimaryConstants.PATH}")
    val pathToCopy = s"${directoryPath}/${folderNameTerm}"
    PrimaryUtilities.copyFilesWithOverwrite(pathWithTermToCopy, pathToCopy)(sparkSession.sparkContext)
  }
}
