package com.bnp.str.climatetables.common

import com.bnp.str.climatetables.mapping.PrimaryMapper
import com.bnp.str.climatetables.reader.PrimaryReader
import com.bnp.str.climatetables.utility.{DataframesResult, FrameMeta}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

class PrimaryRunner(primaryReader: PrimaryReader, frameMeta: FrameMeta)(implicit sparkSession: SparkSession, conf: Config)
  extends RunnerProvider(primaryReader) {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def climate_tables_runner(): DataframesResult = {
    log.info(s"Run climate_tables_runner")

    val dataframes = new PrimaryMapper(
      input_ead_factors_target_data,
      input_ead_factors_other_at,
      input_ead_factors_specific_at,
      input_ead_factors_ctp_level_adjustments,
      input_deterioration_rating,
      input_deterioration_pd,
      input_referential_rating_scale,
      input_referential_hrc_pd,
      input_term_structure_idealised,
      input_mapping_idealised_matrix,
      input_mapping_stt_ts_histo,
      input_mapping_stt_ptf_stt_ccirc,
      input_mapping_stt_ctm_cec,
      input_mapping_stt_region_stt,
      input_mapping_stt_perimeter_stt,
      input_ref_intern_perimeter_s137,
      input_ref_intern_perimeter_flag_nae_ref,
      input_ref_intern_perimeter_ptf_stt_flag_nae_ref_ctm_ccirc,
      input_n015,
      input_chr_risk_level_origin_historic,
      input_fem_table,
      input_tiers_table,
      frameMeta
    ).getMapping_climatetables
    dataframes
  }
}
