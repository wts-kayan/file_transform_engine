package com.bnp.str.climatetables.reader

import com.bnp.str.climatetables.utility.PrimaryUtilities.{readAndExtractColumns,
  readDataFrameFromCsv, readDataFrameFromExcel, readDataFrameFromFolder, selectColumns, selectColumnsDebug}
import com.bnp.str.climatetables.utility.{PrimaryConstants, SchemaSelector}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory


class PrimaryReader()(implicit sparkSession: SparkSession, conf: Config)
  extends SchemaSelector {

  private val log = LoggerFactory.getLogger(this.getClass)

  private val femTiersColumns: (Seq[String], Seq[String]) = readAndExtractColumns(PrimaryConstants.INPUT_EXPECTED_COLUMNS)

  private val input_ead_factors_target_data: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.INPUT_EAD_FACTORS, PrimaryConstants.TARGET_DATA_SHEET_NAME)(sparkSession, conf)

  private val input_ead_factors_other_at: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.INPUT_EAD_FACTORS, PrimaryConstants.OTHER_AT_SHEET_NAME)(sparkSession, conf)

  private val input_ead_factors_specific_at: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.INPUT_EAD_FACTORS, PrimaryConstants.SPECIFIC_AT_SHEET_NAME)(sparkSession, conf)

  private val input_ead_factors_ctp_level_adjustments: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.INPUT_EAD_FACTORS, PrimaryConstants.CTP_LEVEL_ADJUSTMENTS_SHEET_NAME)(sparkSession, conf)

  private val input_mapping_stt_ptf_stt_ccirc: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.INPUT_MAPPING_STT, PrimaryConstants.PTF_STT_CCIRC_SHEET_NAME)(sparkSession, conf)

  private val input_mapping_stt_ctm_cec: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.INPUT_MAPPING_STT, PrimaryConstants.CTM_CEC_SHEET_NAME)(sparkSession, conf)

  private val input_mapping_stt_region_stt: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.INPUT_MAPPING_STT, PrimaryConstants.REGION_STT)(sparkSession, conf)

  private val input_mapping_stt_ts_histo: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.INPUT_MAPPING_STT, PrimaryConstants.TS_HISTO_SHEET_NAME)(sparkSession, conf)

  private val input_mapping_stt_perimeter_stt: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.INPUT_MAPPING_STT, PrimaryConstants.PERIMETER_STT_SHEET_NAME)(sparkSession, conf)

  private val input_ref_intern_perimeter_s137: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.INPUT_REF_INTERN, PrimaryConstants.S137_SHEET_NAME)(sparkSession, conf)

  private val input_ref_intern_perimeter_flag_nae_ref: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.INPUT_REF_INTERN, PrimaryConstants.FLAG_NAE_REF_SHEET_NAME)(sparkSession, conf)

  private val input_ref_intern_perimeter_ptf_stt_flag_nae_ref_ctm_ccirc: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.INPUT_REF_INTERN, PrimaryConstants.PTF_STT_FLAG_NAE_REF_CTM_CCIRC_SHEET_NAME)(sparkSession, conf)

  private val input_n015: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.INPUT_REF_INTERN, PrimaryConstants.N015)(sparkSession, conf)

  private val input_deterioration_rating: DataFrame =
    readDataFrameFromFolder(PrimaryConstants.INPUT_DETERIORATION_RATING)(sparkSession, conf)

  private val input_deterioration_pd: DataFrame =
    readDataFrameFromFolder(PrimaryConstants.INPUT_DETERIORATION_PD)(sparkSession, conf)

  private val input_referential_rating_scale: DataFrame = {
    readDataFrameFromCsv(PrimaryConstants.INPUT_REFERENTIAL_RATING_SCALE)(sparkSession, conf)
  }

  private val input_referential_hrc_pd: DataFrame =
    readDataFrameFromCsv(PrimaryConstants.INPUT_REFERENTIAL_HRC_PD)(sparkSession, conf)

  private val input_term_structure_idealised: DataFrame =
    readDataFrameFromCsv(PrimaryConstants.INPUT_TERM_STRUCTURE_IDEALISED)(sparkSession, conf)

  private val input_mapping_idealised_matrix: DataFrame =
    readDataFrameFromCsv(PrimaryConstants.INPUT_MAPPING_IDEALISED_MATRIX)(sparkSession, conf)

  private val input_chr_risk_level_origin_historic: DataFrame =
    readDataFrameFromCsv(PrimaryConstants.INPUT_CHR_RISK_LEVEL_ORIGIN_HISTORIC)(sparkSession, conf)

  private val input_fem_table: DataFrame =
    selectColumns(readDataFrameFromFolder(PrimaryConstants.INPUT_FEM_TABLE_FOLDER), (femTiersColumns._1 ++ getTargetVariables).distinct)

  private val input_tiers_table: DataFrame =
    selectColumns(readDataFrameFromFolder(PrimaryConstants.INPUT_TIERS_TABLE_FOLDER), femTiersColumns._2)

  private val climateProjectionsDates: Array[String] = conf.getStringList(s"${PrimaryConstants.APP_CONF}" +
    s".${PrimaryConstants.INPUT_CONSTANTS}.${PrimaryConstants.PROJECTION_DATES}").toArray.map(_.toString)

  private val scenarios: Array[String] = conf.getStringList(s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.INPUT_CONSTANTS}.${PrimaryConstants.SCENARIOS}").toArray.map(_.toString)

  private val climateBaseDate: String = conf.getString(s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.INPUT_CONSTANTS}.${PrimaryConstants.CLIMATE_BASE_DATE}")

  private val defaultOriginationDate: String = conf.getString(s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.INPUT_CONSTANTS}.${PrimaryConstants.DEFAULT_ORIGINATION_DATE}")

  private val activateChrAtOrigination: Boolean = conf.getBoolean(s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.INPUT_CONSTANTS}.${PrimaryConstants.CHR_AT_ORIGINATION_ACTIVATION}")

  private val activateCdNivRisqChrOrigin: Boolean = conf.getBoolean(s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.INPUT_CONSTANTS}.${PrimaryConstants.CD_NIV_RISQ_CHR_ORIGIN_ACTIVATION}")

  def getMappingReader(input: String): DataFrame = {
    input match {
      case PrimaryConstants.TARGET_DATA_SHEET_NAME => input_ead_factors_target_data
      case PrimaryConstants.OTHER_AT_SHEET_NAME => input_ead_factors_other_at
      case PrimaryConstants.SPECIFIC_AT_SHEET_NAME => input_ead_factors_specific_at
      case PrimaryConstants.CTP_LEVEL_ADJUSTMENTS_SHEET_NAME => input_ead_factors_ctp_level_adjustments
      case PrimaryConstants.INPUT_DETERIORATION_RATING => input_deterioration_rating
      case PrimaryConstants.INPUT_DETERIORATION_PD => input_deterioration_pd
      case PrimaryConstants.INPUT_REFERENTIAL_RATING_SCALE => input_referential_rating_scale
      case PrimaryConstants.INPUT_REFERENTIAL_HRC_PD => input_referential_hrc_pd
      case PrimaryConstants.INPUT_TERM_STRUCTURE_IDEALISED => input_term_structure_idealised
      case PrimaryConstants.INPUT_MAPPING_IDEALISED_MATRIX => input_mapping_idealised_matrix
      case PrimaryConstants.TS_HISTO_SHEET_NAME => input_mapping_stt_ts_histo
      case PrimaryConstants.PTF_STT_CCIRC_SHEET_NAME => input_mapping_stt_ptf_stt_ccirc
      case PrimaryConstants.CTM_CEC_SHEET_NAME => input_mapping_stt_ctm_cec
      case PrimaryConstants.REGION_STT => input_mapping_stt_region_stt
      case PrimaryConstants.PERIMETER_STT_SHEET_NAME => input_mapping_stt_perimeter_stt
      case PrimaryConstants.S137_SHEET_NAME => input_ref_intern_perimeter_s137
      case PrimaryConstants.FLAG_NAE_REF_SHEET_NAME => input_ref_intern_perimeter_flag_nae_ref
      case PrimaryConstants.PTF_STT_FLAG_NAE_REF_CTM_CCIRC_SHEET_NAME => input_ref_intern_perimeter_ptf_stt_flag_nae_ref_ctm_ccirc
      case PrimaryConstants.N015 => input_n015
      case PrimaryConstants.INPUT_CHR_RISK_LEVEL_ORIGIN_HISTORIC => input_chr_risk_level_origin_historic
      case PrimaryConstants.INPUT_FEM_TABLE_FOLDER => input_fem_table
      case PrimaryConstants.INPUT_TIERS_TABLE_FOLDER => input_tiers_table
      case _ =>
        log.error(s"Invalid input $input")
        throw new IllegalArgumentException(s"Invalid input $input")
    }
  }

  def getClimateProjectionDates: Array[String] = {
    climateProjectionsDates
  }

  def getScenarios: Array[String] = {
    scenarios
  }

  def getClimateBaseDate: String = {
    climateBaseDate
  }

  def getDefaultOriginationDate: String = {
    defaultOriginationDate
  }

  def getActivateChrAtOrigination: Boolean = {
    activateChrAtOrigination
  }

  def getActivateCdNivRisqChrOrigin: Boolean = {
    activateCdNivRisqChrOrigin
  }

  def getTargetVariables: Seq[String] = {
    import sparkSession.implicits._

    input_ead_factors_target_data
      .select("variables")
      .as[String]
      .collect().
      toSeq
  }

  def getStepActivation(date: String): Map[String, Boolean] = {
    val path = s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.INPUT_CONSTANTS}.${PrimaryConstants.STEP_ACTIVATION}"
    val dateConfig = conf.getConfig(s"$path.$date")

    Map(
      PrimaryConstants.EAD_FACTOR_STEP -> dateConfig.getBoolean(PrimaryConstants.EAD_FACTOR_STEP),
      PrimaryConstants.RATING_DETERIORATION_STEP -> dateConfig.getBoolean(PrimaryConstants.RATING_DETERIORATION_STEP),
      PrimaryConstants.PD_DETERIORATION_STEP -> dateConfig.getBoolean(PrimaryConstants.PD_DETERIORATION_STEP),
      PrimaryConstants.ORIGINATION_DATE_STEP -> dateConfig.getBoolean(PrimaryConstants.ORIGINATION_DATE_STEP)
    )
  }
}
