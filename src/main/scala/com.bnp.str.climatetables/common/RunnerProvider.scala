package com.bnp.str.climatetables.common

import com.bnp.str.climatetables.reader.PrimaryReader
import com.bnp.str.climatetables.utility.{DataframesResult, FrameMeta, PrimaryConstants}
import org.apache.spark.sql.DataFrame

abstract class RunnerProvider(primaryReader: PrimaryReader) extends Serializable {

  private[common] lazy val input_ead_factors_target_data: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.TARGET_DATA_SHEET_NAME)

  private[common] lazy val input_ead_factors_other_at: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.OTHER_AT_SHEET_NAME)

  private[common] lazy val input_ead_factors_specific_at: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.SPECIFIC_AT_SHEET_NAME)

  private[common] lazy val input_ead_factors_ctp_level_adjustments: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.CTP_LEVEL_ADJUSTMENTS_SHEET_NAME)

  private[common] lazy val input_deterioration_rating: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.INPUT_DETERIORATION_RATING)

  private[common] lazy val input_deterioration_pd: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.INPUT_DETERIORATION_PD)

  private[common] lazy val input_referential_rating_scale: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.INPUT_REFERENTIAL_RATING_SCALE)

  private[common] lazy val input_referential_hrc_pd: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.INPUT_REFERENTIAL_HRC_PD)

  private[common] lazy val input_term_structure_idealised: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.INPUT_TERM_STRUCTURE_IDEALISED)

  private[common] lazy val input_mapping_idealised_matrix: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.INPUT_MAPPING_IDEALISED_MATRIX)

  private[common] lazy val input_mapping_stt_ts_histo: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.TS_HISTO_SHEET_NAME)

  private[common] lazy val input_mapping_stt_ptf_stt_ccirc: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.PTF_STT_CCIRC_SHEET_NAME)

  private[common] lazy val input_mapping_stt_ctm_cec: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.CTM_CEC_SHEET_NAME)

  private[common] lazy val input_mapping_stt_region_stt: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.REGION_STT)

  private[common] lazy val input_mapping_stt_perimeter_stt: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.PERIMETER_STT_SHEET_NAME)

  private[common] lazy val input_ref_intern_perimeter_s137: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.S137_SHEET_NAME)

  private[common] lazy val input_ref_intern_perimeter_flag_nae_ref: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.FLAG_NAE_REF_SHEET_NAME)

  private[common] lazy val input_ref_intern_perimeter_ptf_stt_flag_nae_ref_ctm_ccirc: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.PTF_STT_FLAG_NAE_REF_CTM_CCIRC_SHEET_NAME)

  private[common] lazy val input_n015: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.N015)

  private[common] lazy val input_chr_risk_level_origin_historic: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.INPUT_CHR_RISK_LEVEL_ORIGIN_HISTORIC)

  private[common] lazy val input_fem_table: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.INPUT_FEM_TABLE_FOLDER)

  private[common] lazy val input_tiers_table: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.INPUT_TIERS_TABLE_FOLDER)

  def climate_tables_runner(): DataframesResult
}
