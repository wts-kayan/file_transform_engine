package com.bnp.str.climatetables.mapping

import com.bnp.str.climatetables.utility.FrameMeta
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{lit, _}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

import java.util

object EnrichmentHelper {
  private val log = LoggerFactory.getLogger(this.getClass)


  /** Enriches the FEM-with-TIERS DataFrame with additional portfolio and reference data.
    *
    * @param portfolio                                                 DataFrame containing FEM + TIERS columns to be enriched.
    * @param input_mapping_idealised_matrix                            Mapping table for idealized matrix.
    * @param input_mapping_stt_ts_histo                                Mapping table for historical time series.
    * @param input_mapping_stt_ptf_stt_ccirc                           Mapping table for portfolio stt ccirc.
    * @param input_mapping_stt_ctm_cec                                 Mapping table for CTM cec.
    * @param input_mapping_stt_region_stt                              Mapping table for region stt.
    * @param input_mapping_stt_perimeter_stt                           Mapping table for perimeter stt.
    * @param input_ref_intern_perimeter_s137                           Reference data for internal perimeter S137.
    * @param input_ref_intern_perimeter_flag_nae_ref                   Reference flag data for NAE.
    * @param input_ref_intern_perimeter_ptf_stt_flag_nae_ref_ctm_ccirc Reference  data .
    * @param input_n015                                                Additional reference data table N015.
    * @param sparkSession                                              implicit SparkSession.
    * @return Enriched DataFrame with original columns plus new derived columns from all mapping and reference tables.
    */
  def enrichPortfolio(portfolio: DataFrame,
                      frameMeta: FrameMeta,
                      input_mapping_idealised_matrix: DataFrame,
                      input_mapping_stt_ts_histo: DataFrame,
                      input_mapping_stt_ptf_stt_ccirc: DataFrame,
                      input_mapping_stt_ctm_cec: DataFrame,
                      input_mapping_stt_region_stt: DataFrame,
                      input_mapping_stt_perimeter_stt: DataFrame,
                      input_ref_intern_perimeter_s137: DataFrame,
                      input_ref_intern_perimeter_flag_nae_ref: DataFrame,
                      input_ref_intern_perimeter_ptf_stt_flag_nae_ref_ctm_ccirc: DataFrame,
                      input_n015: DataFrame,
                      input_chr_risk_level_origin_historic: DataFrame
                     )(implicit sparkSession: SparkSession): DataFrame = {

    var enrichPortfolio = join_ctm_cec(portfolio, input_mapping_stt_ctm_cec)
    enrichPortfolio = join_country_stt(enrichPortfolio, input_n015)
    enrichPortfolio = join_region(enrichPortfolio, input_ref_intern_perimeter_s137)
    enrichPortfolio = join_region_stt(enrichPortfolio, input_mapping_stt_region_stt)
    enrichPortfolio = join_segmentation_flag(enrichPortfolio, input_ref_intern_perimeter_flag_nae_ref)
    enrichPortfolio = join_ptf_stt(enrichPortfolio, input_mapping_stt_ptf_stt_ccirc)
    enrichPortfolio = join_pmas_stt(enrichPortfolio)
    enrichPortfolio = join_ptf_stt_flag_nae(enrichPortfolio, input_ref_intern_perimeter_ptf_stt_flag_nae_ref_ctm_ccirc)
    val distinctProjectionAod = getDistinctProjectionAod(input_mapping_stt_ts_histo)
    enrichPortfolio = with_date_projection_aod(enrichPortfolio, frameMeta, distinctProjectionAod)

    val specPerimeterStt = buildWildcardMatchSpecPerimeterStt(input_mapping_stt_perimeter_stt)
    var enrichPortfolio2 = enrichWithWildcardMatchesUDF(enrichPortfolio, specPerimeterStt)

    enrichPortfolio2 = join_chr_risk_level_origin_historic(enrichPortfolio2, frameMeta, input_chr_risk_level_origin_historic)

    val specTsHisto = buildWildcardMatchSpecTsHisto(input_mapping_stt_ts_histo)
    enrichPortfolio2 = enrichWithWildcardMatchesUDF(enrichPortfolio2, specTsHisto)

    enrichPortfolio2 = join_with_mapping_idealised_matrix(enrichPortfolio2, input_mapping_idealised_matrix)
    enrichPortfolio2.select(enrichPortfolio2("*"), col("ste_target_rating_scale"), col("ste_target_rating_scale_origination"))
  }

  private def join_ctm_cec(df: DataFrame, ref_ctm: DataFrame): DataFrame = {
    log.info("Enrich with ctm_cec")
    // Part 1 ref: rows where CD_CLA_EXP_COREP_STD is filled.
    val ref_p1 = ref_ctm
      .filter(
        col("CD_CLA_EXP_COREP_STD").isNotNull &&
          col("CD_CLA_EXP_COREP_STD") =!= ""
      )
      .select(
        col("CD_CLA_EXP_COREP_STD").as("_p1_std"),
        col("CD_TYP_APPRO_BALE_2").as("_p1_bale2"),
        col("CTM_CEC").as("_ctm_p1")
      )
      .dropDuplicates(Seq("_p1_std", "_p1_bale2"))

    // Part 2 ref: rows where CD_CLA_EXP_COREP_IRBA is filled.
    val ref_p2 = ref_ctm
      .filter(
        col("CD_CLA_EXP_COREP_IRBA").isNotNull &&
          col("CD_CLA_EXP_COREP_IRBA") =!= ""
      )
      .select(
        col("CD_CLA_EXP_COREP_IRBA").as("_p2_irba"),
        col("CD_TYP_APPRO_BALE_2").as("_p2_bale2"),
        col("CTM_CEC").as("_ctm_p2")
      )
      .dropDuplicates(Seq("_p2_irba", "_p2_bale2"))

    df
      // Join 1 — pure BroadcastHashJoin on Part 1 keys.
      .join(
        ref_p1.hint("broadcast"),
        (df("cd_cec_fac_std") === col("_p1_std")) &&
          (df("basel_approach_type_arc") === col("_p1_bale2")),
        "left"
      )
      // Join 2 — pure BroadcastHashJoin on Part 2 keys.
      .join(
        ref_p2.hint("broadcast"),
        (df("cd_cec_fac_irba") === col("_p2_irba")) &&
          (df("basel_approach_type_arc") === col("_p2_bale2")),
        "left"
      )
      // Part 1 wins; Part 2 fills in what Part 1 missed.
      .withColumn("ctm_cec", coalesce(col("_ctm_p1"), col("_ctm_p2")))
      .drop("_p1_std", "_p1_bale2", "_ctm_p1",
        "_p2_irba", "_p2_bale2", "_ctm_p2")
  }

  private def join_country_stt(df: DataFrame, ref_n015: DataFrame): DataFrame = {
    log.info("Enrich with country_stt")
    // Prepare reference DataFrame
    val ref = ref_n015
      .select(
        col("Code pays").as("_cp"),
        col("Code pays père").as("_cpp")
      )
      .dropDuplicates(Seq("_cp"))

    df
      .join(ref.hint("broadcast"), df("business_country") === col("_cp"), "left")
      .withColumn("country_stt", col("_cpp"))
      .drop("_cp", "_cpp")
  }
  private def join_region(df: DataFrame, ref_region: DataFrame): DataFrame = {
    // Prepare reference DataFrame
    val ref = ref_region
      .filter(col("Code critère regroupement pays") === "STT")
      .select(
        col("Code pays").as("_region_code"),
        col("Code zone pays").as("_region_val")
      )
    df
      .join(ref.hint("broadcast"), df("business_country") === ref("_region_code"), "left")
      .withColumn("region", coalesce(col("_region_val"), lit("")))
      .drop("_region_code", "_region_val")
  }


  private def join_region_stt(df: DataFrame, ref_region_stt: DataFrame): DataFrame = {
    log.info("Enrich with region_stt")
    // Prepare reference DataFrame
    val ref = ref_region_stt
      .select(
        col("REGION").as("_region"),
        col("REG STT").as("_reg_stt")
      )

    df
      .join(ref.hint("broadcast"), df("region") === col("_region"), "left")
      .withColumn("region_stt", col("_reg_stt"))
      .drop("_region", "_reg_stt")
  }

  private def join_segmentation_flag(df: DataFrame, ref_flag_nae: DataFrame): DataFrame = {
    log.info("Enrich with segmentation_flag")
    // Prepare reference DataFrame
    val ref = ref_flag_nae
      .select(
        col("Code segmentation NAE").as("_cs"),
        col("Flag segmentation NAE").as("_fs")
      )

    df
      .join(ref.hint("broadcast"), df("segmentation_nac") === col("_cs"), "left")
      .withColumn("segmentation_flag", col("_fs"))
      .drop("_cs", "_fs")
  }

  private def join_ptf_stt(df: DataFrame, ref_ptf: DataFrame): DataFrame = {
    log.info("Enrich with ptf_stt")
    // Part 1: CTM_CEC is filled — join on both keys
    val ref_p1 = ref_ptf
      .filter(col("CTM_CEC").isNotNull && col("CTM_CEC") =!= "")
      .select(
        col("CTM_CEC").as("_p1_ctm"),
        col("Code du portefeuille prudentiel").as("_p1_ptf"),
        col("PTF STT").as("_ptf_stt_p1"),
        col("C or R").as("_c_or_r_p1"),
        col("due dilligence exclusions").as("_due_dil_p1")
      )
      .dropDuplicates(Seq("_p1_ctm", "_p1_ptf"))

    // Part 2: CTM_CEC is empty — join on portfolio code only
    val ref_p2 = ref_ptf
      .filter(col("CTM_CEC").isNull || col("CTM_CEC") === "")
      .select(
        col("Code du portefeuille prudentiel").as("_p2_ptf"),
        col("PTF STT").as("_ptf_stt_p2"),
        col("C or R").as("_c_or_r_p2"),
        col("due dilligence exclusions").as("_due_dil_p2")
      )
      .dropDuplicates(Seq("_p2_ptf"))

    df
      // Join 1 — BroadcastHashJoin on (ctm_cec, prudential_portfolio_code)
      .join(ref_p1.hint("broadcast"), df("ctm_cec") === col("_p1_ctm"), "left")
      // Join 2 — BroadcastHashJoin on (prudential_portfolio_code) only
      .join(ref_p2.hint("broadcast"), df("prudential_portfolio_code") === col("_p2_ptf"), "left")
      // Part 1 wins; Part 2 is the fallback
      .withColumn("temp_ptf_stt", coalesce(col("_ptf_stt_p1"), col("_ptf_stt_p2")))
      .withColumn("c_or_r", coalesce(col("_c_or_r_p1"), col("_c_or_r_p2")))
      .drop(
        "_p1_ctm", "_p1_ptf", "_ptf_stt_p1", "_c_or_r_p1", "_due_dil_p1",
        "_p2_ptf", "_ptf_stt_p2", "_c_or_r_p2", "_due_dil_p2"
      )
  }

  private def join_pmas_stt(df: DataFrame): DataFrame = {
    log.info("Enrich with pma_stt")
    df.withColumn(
      "pmas_stt",
      when(
        col("client_coverage").isNotNull &&
          col("pmas_code_post_acc").isNotNull &&
          (col("c_or_r") === "C") &&
          col("pmas_code_post_acc").isin("PMA_05", "PMA_03"),
        when(
          col("client_coverage").substr(1, 3) === "CIB",
          lit("PMA_05")
        ).when(
          col("client_coverage").substr(1, 4) === "BDDF",
          lit("PMA_03")
        ).otherwise(col("pmas_code_post_acc"))
      ).otherwise(col("pmas_code_post_acc"))
    )
  }

  private def join_ptf_stt_flag_nae(df: DataFrame, ref_ptf_flag_nae: DataFrame): DataFrame = {
    log.info("Enrich with ptf_stt_flag_nae")
    // Part 1: CTM_CEC is filled — join on (PMA_STT + CTM_CEC + Flag segmentation NAE)
    val ref_p1 = ref_ptf_flag_nae
      .filter(col("CTM_CEC").isNotNull && col("CTM_CEC") =!= "")
      .select(
        col("PMA_STT").as("_p1_pma"),
        col("CTM_CEC").as("_p1_ctm"),
        col("Flag segmentation NAE").as("_p1_seg"),
        col("PTF STT").as("_ptf_stt_p1")
      )
      .dropDuplicates(Seq("_p1_pma", "_p1_ctm", "_p1_seg"))

    // Part 2: Code portefeuille prudentiel is filled — join on (PMA_STT + Code ptf + Flag segmentation NAE)
    val ref_p2 = ref_ptf_flag_nae
      .filter(col("CTM_CEC").isNull || col("CTM_CEC") === "")
      .select(
        col("PMA_STT").as("_p2_pma"),
        col("Code portefeuille prudentiel").as("_p2_ptf"),
        col("Flag segmentation NAE").as("_p2_seg"),
        col("PTF STT").as("_ptf_stt_p2")
      )
      .dropDuplicates(Seq("_p2_pma", "_p2_ptf", "_p2_seg"))

    df
      // Join 1 — BroadcastHashJoin on (pmas_stt + ctm_cec + segmentation_flag)
      .join(ref_p1.hint("broadcast"),
        (df("pmas_stt") === col("_p1_pma")) &&
          (df("ctm_cec") === col("_p1_ctm")) &&
          (df("segmentation_flag") === col("_p1_seg")),
        "left"
      )
      // Join 2 — BroadcastHashJoin on (pmas_stt + prudential_portfolio_code + segmentation_flag)
      .join(ref_p2.hint("broadcast"),
        (df("pmas_stt") === col("_p2_pma")) &&
          (df("prudential_portfolio_code") === col("_p2_ptf")) &&
          (df("segmentation_flag") === col("_p2_seg")),
        "left"
      )
      // Priority: Part 1 > Part 2 > temp_ptf_stt (fallback from join_ptf_stt)
      .withColumn(
        "ptf_stt",
        coalesce(col("_ptf_stt_p1"), col("_ptf_stt_p2"), col("temp_ptf_stt"))
      )
      .drop(
        "_p1_pma", "_p1_ctm", "_p1_seg", "_ptf_stt_p1",
        "_p2_pma", "_p2_ptf", "_p2_seg", "_ptf_stt_p2",
        "temp_ptf_stt"
      )
  }

  private def getDistinctProjectionAod(df: DataFrame): Seq[String] = {
    log.info("Collecting distinct values for PROJECTION_AOD")
    df.select("PROJECTION_AOD")
      .distinct()
      .filter(col("PROJECTION_AOD").isNotNull)
      .collect()
      .map(_.getString(0))
      .toSeq
  }

  private def with_date_projection_aod(df_portfolio: DataFrame, frameMeta: FrameMeta, distinctProjectionAod: Seq[String]): DataFrame = {
    log.info("Enrich with date_projection_aod")
    val parsedDate = to_date(col("date_origination"), "yyyy-MM-dd")
    val defaultOriginationDate = frameMeta.defaultOriginationDate
    val asOfDate = frameMeta.baseDate
    df_portfolio
      .withColumn("__origination_quarter__",
        when(col("date_origination").isNull || trim(col("date_origination")) === "", lit(null).cast("string"))
          .otherwise(concat(year(parsedDate), lit("Q"), quarter(parsedDate)))
      )
      .withColumn("__date_projection_aod__",
        // 1. If date_origination is in distinctProjectionAod, use it
        when(col("__origination_quarter__").isin(distinctProjectionAod: _*), col("__origination_quarter__"))
          // 2. If date_origination is EMPTY -> defaultOriginationDate
          .when(col("date_origination").isNull || trim(col("date_origination")) === "", lit(defaultOriginationDate))
          // 3. If date_origination > asOfDate -> asOfDate
          .when(col("date_origination") > lit(asOfDate), lit(asOfDate))
          // 4. ELSE -> defaultOriginationDate
          .otherwise(lit(defaultOriginationDate))
      )
  }

  private def join_chr_risk_level_origin_historic(df_portfolio: DataFrame, frameMeta: FrameMeta, chr_risk_level_origin_historic: DataFrame)
                                                 (implicit sparkSession: SparkSession): DataFrame = {

    val pdModelScope = Seq("PMARF101", "PMBRF101", "PMCRF101", "PMDRF101", "PRIBF201", "PRIEF201", "PRICF201", "PRIDF201", "PRIAF201", "PRPBF201", "PRPDF201", "PRPCF201", "PRPAF201")
    val mostRecentDateRow = chr_risk_level_origin_historic.select(max("asofdate")).first()
    val mostRecentDate = Option(mostRecentDateRow).map(_.getString(0)).getOrElse("")

    // 1. Calculate dateToUse for both mappings
    val dfWithDate = df_portfolio.withColumn("date_to_use",
      when(col("__origination_quarter__") < lit(frameMeta.defaultOriginationDate), lit(frameMeta.defaultOriginationDate))
        .when(col("__origination_quarter__") > lit(frameMeta.baseDate), lit(mostRecentDate))
        .otherwise(col("__origination_quarter__"))
    )

    // 2. Join with "Other" mapping (Direct lookup) for chr at origination treatment
    // Expected columns in dfChrRiskLevelHistoricOther: [asofdate;cd_niv_risq_chr_origin;pd_ttc_origin]
    val dfJoinedOther = dfWithDate.join(
      chr_risk_level_origin_historic.hint("broadcast"),
      col("date_to_use") === col("asofdate") && col("chr_at_origination") === col("cd_niv_risq_chr_origin"),
      "left"
    ).select(dfWithDate("*"), col("pd_ttc_origin").as("__new_pd_reg_origination__"))

    val dfWithChrAtOriginationTreatment = dfJoinedOther
      .withColumn("pd_reg_origination_before_det",
        when(lit(frameMeta.activateChrAtOrigination) && col("chr_at_origination").isNotNull && col("__new_pd_reg_origination__").isNotNull,
          col("__new_pd_reg_origination__"))
          .otherwise(col("pd_reg_origination_before_det"))
      )
      .withColumn("chrRiskLevelOrigin",
        // First priority: activateChrAtOrigination
        when(lit(frameMeta.activateChrAtOrigination) && col("chr_at_origination").isNotNull && col("__new_pd_reg_origination__").isNotNull,
          col("chr_at_origination"))
          .otherwise(lit(null))
      )
      .drop("__new_pd_reg_origination__")

    // 3. Handle "Main" mapping (Nearest PD lookup)
    val chrRiskLevelOriginHistoricData: Map[String, util.TreeMap[java.lang.Double, String]] =
      chr_risk_level_origin_historic
        .select(col("AsOfDate"), col("pd_ttc_origin").cast("double"), col("cd_niv_risq_chr_origin"))
        .collect()
        .groupBy(_.getAs[String]("AsOfDate"))
        .map { case (date, rows) =>
          val tm = new util.TreeMap[java.lang.Double, String]()
          rows.foreach(r => tm.put(r.getAs[Double]("pd_ttc_origin"), r.getAs[String]("cd_niv_risq_chr_origin")))
          date -> tm
        }

    val bcChrRiskLevel = sparkSession.sparkContext.broadcast(chrRiskLevelOriginHistoricData)

    val getRiskUdf = udf((dateToUse: String, pd: java.lang.Double) => {
      if (dateToUse == null || pd == null) {
        null
      } else {
        bcChrRiskLevel.value.get(dateToUse) match {
          case None => null
          case Some(map) =>
            if (map.containsKey(pd)) {
              map.get(pd)
            } else {
              val ceilingKey = map.ceilingKey(pd)
              val floorKey = map.floorKey(pd)
              if (ceilingKey == null) map.get(floorKey)
              else if (floorKey == null) map.get(ceilingKey)
              else if (pd - floorKey < ceilingKey - pd) map.get(floorKey)
              else map.get(ceilingKey)
            }
        }
      }
    })

    val dfJoinedMain = dfWithChrAtOriginationTreatment
      .withColumn("__new_cd_niv_risq_chr_origin__", getRiskUdf(col("date_to_use"), col("pd_reg_origination_before_det").cast("double")))

    // 4. Final Logic implementation
    dfJoinedMain
      .withColumn("chrRiskLevelOrigin",
        // Second priority: activateCdNivRisqChrOrigin with specific filters
        when(lit(frameMeta.activateCdNivRisqChrOrigin) &&
          col("chrRiskLevelOrigin").isNull &&
          col("pd_reg_origination_before_det").isNotNull && col("pd_reg_origination_before_det").cast("double") >= 0 && col("pd_reg_origination_before_det").cast("double") <= 1 &&
          (col("__origination_quarter__").isNotNull || col("__origination_quarter__") =!= lit("")) &&
          (col("pd_model").isin(pdModelScope: _*) ||
            col("term_structure_deduced").startsWith("BCEF_RETAIL") ||
            col("term_structure_deduced").startsWith("REB_BCEF_RET_PART") ||
            col("term_structure_deduced").startsWith("REB_BCEF_RET_PRO")) &&
          (col("cd_met_mes_dep_pd") === "1b" || col("cd_met_mes_dep_pd") === "1c"),
          col("__new_cd_niv_risq_chr_origin__"))
          // Third priority: Fallback to current chrRiskLevel
          .when(lit(frameMeta.activateCdNivRisqChrOrigin) &&
            col("chrRiskLevelOrigin").isNull &&
            (col("pd_model").isNull ||
              (!col("pd_model").isin(pdModelScope: _*) && !col("pd_model").startsWith("PLPRWW") && !col("pd_model").startsWith("PRPLLU") &&
                !col("term_structure_deduced").startsWith("BCEF_RETAIL") &&
                !col("term_structure_deduced").startsWith("REB_BCEF_RET_PART") &&
                !col("term_structure_deduced").startsWith("REB_BCEF_RET_PRO"))),
            col("cd_niv_risq_chr"))
          .otherwise(col("chrRiskLevelOrigin"))
      )
      .drop("date_to_use", "__new_cd_niv_risq_chr_origin__", "__pd_diff__", "rank")
  }

  private case class WildcardMatchSpec(
                                        dfReferential: DataFrame,
                                        joinMapping: Seq[(String, String)],
                                        rankCol: String,
                                        valueCol: String,
                                        outputCol: String,
                                        rankOutputCol: String,
                                        termStructureCol: String,
                                        termStructureOutputCol: String
                                      )

  private case class MatchGroup(activeIdx: Array[Int], lookup: Map[Seq[String], (Int, String, String)])

  private def buildWildcardMatchSpecPerimeterStt(input_mapping_stt_perimeter_stt: DataFrame): Seq[WildcardMatchSpec] = {
    Seq(
      WildcardMatchSpec(
        dfReferential = input_mapping_stt_perimeter_stt,
        joinMapping = Seq(
          "PMA_STT" -> "pmas_stt",
          "PTF_STT" -> "ptf_stt",
          "REGION_STT" -> "region_stt",
          "COUNTRY_STT" -> "country_stt",
          "PD_MODEL_NAME" -> "pd_model",
          "ACCOUNTING_SITE_CODE" -> "accounting_site_code_post_acc",
          "CHR Risk Level" -> "cd_niv_risq_chr",
          "CODE_SECTOR_NACE2" -> "cd_sector_nace2",
          "CTM" -> "cd_clas_tie_mut"
        ),
        rankCol = "Ordre",
        valueCol = "MATRIX_ID",
        outputCol = "__matrix_id__",
        rankOutputCol = "mapping_stt_order",
        termStructureCol = "Structure par terme",
        termStructureOutputCol = "term_structure_deduced"
      ),
    )
  }

  private def buildWildcardMatchSpecTsHisto(input_mapping_stt_ts_histo: DataFrame): Seq[WildcardMatchSpec] = {
    Seq(
      WildcardMatchSpec(
        dfReferential = input_mapping_stt_ts_histo,
        joinMapping = Seq(
          "PROJECTION_AOD" -> "__date_projection_aod__",
          "PMA_STT" -> "pmas_stt",
          "PTF_STT" -> "ptf_stt",
          "REGION_STT" -> "region_stt",
          "COUNTRY_STT" -> "country_stt",
          "PD_MODEL_NAME" -> "pd_model_origination",
          "ACCOUNTING_SITE_CODE" -> "accounting_site_code_post_acc",
          "CHR_RISK_LEVEL" -> "chrRiskLevelOrigin",
          "CTM" -> "cd_clas_tie_mut",
          "BUSINESS_COUNTRY" -> "business_country",
          "NACE_CODE" -> "cd_sector_nace2"
        ),
        rankCol = "RANK",
        valueCol = "MATRIX_ID_SPECIFIC",
        outputCol = "__matrix_id_specific__",
        rankOutputCol = "mapping_stt_order_origination",
        termStructureCol = "MATRIX_ID_SPECIFIC",
        termStructureOutputCol = "term_structure_origination_deduced"
      )
    )
  }

  private def buildBroadcastRef(
                                 spec: WildcardMatchSpec
                               )(implicit sparkSession: SparkSession): Broadcast[Array[MatchGroup]] = {

    val refCols = spec.joinMapping.map(_._1)

    val rows = spec.dfReferential
      .select((refCols :+ spec.rankCol :+ spec.valueCol :+ spec.termStructureCol).map(col): _*)
      .withColumn(spec.rankCol, col(spec.rankCol).cast("int"))
      .collect()

    val groups: Array[MatchGroup] = rows.map { row =>
      val activeIdx: Array[Int] = refCols.indices.filter(i => {
        val v = row.getAs[String](refCols(i))
        v != null && v.trim.nonEmpty
      }).toArray
      val key: Seq[String] = activeIdx.map(i => row.getAs[String](refCols(i))).toSeq
      val rank: Int = row.getAs[Int](spec.rankCol)
      val value: String = row.getAs[String](spec.valueCol)
      val termStructure: String = row.getAs[String](spec.termStructureCol)
      (activeIdx.toSeq, key, rank, value, termStructure)
    }
      .groupBy(_._1)
      .map { case (activeIdxSeq, entries) =>
        val lookup: Map[Seq[String], (Int, String, String)] = entries
          .map { case (_, key, rank, value, ts) => key -> (rank, value, ts) }
          .groupBy(_._1)
          .map { case (key, vs) => key -> vs.map(_._2).minBy(_._1) }

        MatchGroup(activeIdxSeq.toArray, lookup)
      }
      .toArray
    sparkSession.sparkContext.broadcast(groups)
  }

  private def enrichWithWildcardMatchesUDF(
                                            dfPortfolio: DataFrame,
                                            specs: Seq[WildcardMatchSpec]
                                          )(implicit sparkSession: SparkSession): DataFrame = {
    log.info("Enrich with multiple wildcard matches")

    val broadcasts: Seq[Broadcast[Array[MatchGroup]]] = specs.map(buildBroadcastRef)

    var result = dfPortfolio
    specs.zip(broadcasts).foreach { case (spec, bc) =>
      val bigCols = spec.joinMapping.map(_._2)

      val matchUdf = udf { values: Row =>
        val groups = bc.value
        var bestRank = Int.MaxValue
        var bestValue: Option[String] = None
        var bestTS: Option[String] = None
        var g = 0
        while (g < groups.length) {
          val group = groups(g)
          val key: Seq[String] = group.activeIdx.map { i =>
            val colName = bigCols(i)
            val rawVal = values.getAs[String](i)
            if (rawVal == null) null else rawVal
          }.toSeq

          // Try matching with the exact key first
          var matchFound = group.lookup.get(key)

          // If no match and it's a pd_model column, try the truncated version
          if (matchFound.isEmpty) {
            val truncatedKey: Seq[String] = group.activeIdx.map { i =>
              val colName = bigCols(i)
              val rawVal = values.getAs[String](i)
              if (rawVal != null && (colName == "pd_model" || colName == "pd_model_origination")) {
                val s = rawVal.trim
                if (s.length >= 6) s.substring(0, 6) else s
              } else {
                rawVal
              }
            }.toSeq
            matchFound = group.lookup.get(truncatedKey)
          }

          matchFound match {
            case Some((rank, value, ts)) if rank < bestRank =>
              bestRank = rank
              bestValue = Some(value)
              bestTS = Some(ts)
            case _ =>
          }
          g += 1
        }

        (bestValue, bestTS) match {
          case (Some(v), Some(ts)) => Seq(v, bestRank.toString, ts)
          case _ => Seq(null, null, null)
        }
      }

      val structCol = struct(bigCols.map(c => col(c).cast("string")): _*)

      val matchResultCol = matchUdf(structCol)
      result = result
        .withColumn("_temp_match", matchResultCol)
        .withColumn(spec.outputCol, col("_temp_match").getItem(0))
        .withColumn(spec.rankOutputCol, col("_temp_match").getItem(1).cast("int"))
        .withColumn(spec.termStructureOutputCol, col("_temp_match").getItem(2))
        .drop("_temp_match")
    }
    result
  }

  private def join_with_mapping_idealised_matrix(df_portfolio: DataFrame, idealised_matrix: DataFrame): DataFrame = {
    log.info("Join with mapping idealised matrix")
    // Keep only the columns we need from the reference and alias them.
    val ref = idealised_matrix
      .select(
        col("MIGRATION_MATRIX_NAME").as("__ref_mig_name__"),
        col("TARGET_NOTATION_CODE").as("__target_rating_scale__")
      )
      .dropDuplicates("__ref_mig_name__")

    // Broadcast the reference and perform an exact-match left join
    // to find target_rating_scale and target_rating_scale_origination.
    df_portfolio
      .join(
        ref.hint("broadcast"),
        df_portfolio("__matrix_id__") === col("__ref_mig_name__"),
        "left"
      )
      .withColumn("ste_target_rating_scale", col("__target_rating_scale__"))
      .join(
        ref.alias("ref2").hint("broadcast"), // Alias the second reference DataFrame
        df_portfolio("__matrix_id_specific__") === col("ref2.__ref_mig_name__"),
        "left"
      )
      .withColumn("ste_target_rating_scale_origination", col("ref2.__target_rating_scale__"))
      .select(df_portfolio("*"), col("ste_target_rating_scale"), col("ste_target_rating_scale_origination"))
  }
}
