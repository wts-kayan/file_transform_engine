package com.bnp.str.climatetables.mapping

import com.bnp.str.climatetables.common.MapperProvider
import com.bnp.str.climatetables.mapping.EnrichmentHelper.enrichPortfolio
import com.bnp.str.climatetables.utility.Preconditions.requireLogged
import com.bnp.str.climatetables.utility.{DataframesResult, FrameMeta}
import com.typesafe.config.Config
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import java.time.LocalDate
import java.time.temporal.ChronoUnit

class PrimaryMapper(input_ead_factors_target_data: DataFrame,
                    input_ead_factors_other_at: DataFrame,
                    input_ead_factors_specific_at: DataFrame,
                    input_ead_factors_ctp_level_adjustments: DataFrame,
                    input_deterioration_rating: DataFrame,
                    input_deterioration_pd: DataFrame,
                    input_referential_rating_scale: DataFrame,
                    input_referential_hrc_pd: DataFrame,
                    input_term_structure_idealised: DataFrame,
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
                    input_chr_risk_level_origin_historic: DataFrame,
                    input_fem_table: DataFrame,
                    input_tiers_table: DataFrame,
                    frameMeta: FrameMeta)
                   (implicit sparkSession: SparkSession, config: Config) extends MapperProvider {

  private val log = LoggerFactory.getLogger(this.getClass)

  private val persistedDataFrames = scala.collection.mutable.ArrayBuffer.empty[DataFrame]

  private def computeEadFactor(): DataFrame = {
    log.info(s"---- Part 1 : Apply ead factors ----")
    // Resolve factor column name to consider for the treatment
    val factorCol = s"factor_${frameMeta.date}"

    // Target variables names from target data sheet
    val targetVariables: Seq[String] = frameMeta.targetVariables

    requireLogged(targetVariables.nonEmpty, "target data sheet must contain at least one variable.", log)

    val femColumns = input_fem_table.columns.toSet
    val missingCols = targetVariables.filterNot(femColumns.contains)

    requireLogged(missingCols.isEmpty, s"The following variables are missing from FEM: ${missingCols.mkString(", ")}", log)

    val targetSet = targetVariables.toSet

    // Prepare ctp_level_adjustment - highest priority
    log.info(s"Filter ctp_level_adjustment sheet")

    val ctpLevelAdjustmentsFiltered = input_ead_factors_ctp_level_adjustments
      .filter(col("scenario") === frameMeta.scenario)
      .select(
        col("original_ctpr_id"),
        col("nace_sector_ecb_epc_rating"),
        col("cd_pays_residence_cal"),
        regexp_replace(col(factorCol), ",", ".").cast("double").alias("__factor__first__")
      )

    // Prepare specfic_at - second priority
    // - filter scenario + year
    // - explode "1-2" stage into two rows
    // - cast factor value
    log.info(s"Filter specific_at sheet")

    val specificAtFiltered = input_ead_factors_specific_at
      .filter(col("scenario") === frameMeta.scenario && col("year") === frameMeta.date)
      .withColumn(
        "stage",
        split(col("stage"), "-") // "1-2" --> ["1","2"], "1" --> ["1"]
      )
      .withColumn("stage", explode(col("stage"))) // one row per stage value
      .select(
        col("dr_model"),
        col("nace_sector_ecb_epc_rating"),
        col("geographical_breakdown"),
        col("asset_classes"),
        col("stage"),
        regexp_replace(col("value"), ",", ".").cast("double").alias("__factor__second__")
      )

    // Prepare other_at - lowest priority
    // Filter other_at sheet to the requested scenario and keep only the join key and factor column to consider
    log.info(s"Filter other_at sheet")

    val otherAtFiltered = input_ead_factors_other_at
      .filter(col("scenario") === frameMeta.scenario)
      .select(
        col("cd_pays_residence_cal"),
        regexp_replace(col(factorCol), ",", ".").cast("double").alias("__factor__third__")
      )

    // Join the 3 sheets of ead factor

    // Join FEM + TIERS on ctpr_id to retrieve the original_ctpr_id value from TIERS table
    log.info(s"Join the 3 referentials")
    log.info(s"Join with ctp_level_adjustments sheet")

    val withCtpLevelAdjustments = input_fem_table
      .join(input_tiers_table, Seq("ctpr_id"), "left")
      .select(input_fem_table("*"), col("original_ctpr_id"))
      .join(
        ctpLevelAdjustmentsFiltered.hint("broadcast"),
        input_tiers_table("original_ctpr_id") === ctpLevelAdjustmentsFiltered("original_ctpr_id") &&
          input_fem_table("nace_sector_ecb_epc_rating") === ctpLevelAdjustmentsFiltered("nace_sector_ecb_epc_rating") &&
          input_fem_table("cd_pays_residence_cal") === ctpLevelAdjustmentsFiltered("cd_pays_residence_cal") &&
          input_fem_table("stage") =!= lit("3"),
        "left"
      )
      .select(input_fem_table("*"), col("__factor__first__")) // only FEM columns and factor first

    log.info(s"Join with specific_at sheet")

    val withSpecific = withCtpLevelAdjustments
      .join(
        specificAtFiltered.hint("broadcast"),
        (input_fem_table("z_model_name") === specificAtFiltered("dr_model") || specificAtFiltered("dr_model").isNull) && // wildcard row for dr_model
          input_fem_table("nace_sector_ecb_epc_rating") === specificAtFiltered("nace_sector_ecb_epc_rating") &&
          input_fem_table("cd_pays_residence_cal") === specificAtFiltered("geographical_breakdown") &&
          input_fem_table("ecb_climate_asset_classes") === specificAtFiltered("asset_classes") &&
          input_fem_table("stage") === specificAtFiltered("stage"),
        "left"
      )
      .select(withCtpLevelAdjustments("*"), col("__factor__second__")) // only FEM columns and factor second choice

    log.info(s"Join with other_at sheet")
    val withFallback = withSpecific
      .join(
        otherAtFiltered.hint("broadcast"),
        input_fem_table("cd_pays_residence_cal") === otherAtFiltered("cd_pays_residence_cal") &&
          input_fem_table("stage") =!= lit("3"),
        "left"
      )
      .select(withSpecific("*"), col("__factor__third__")) // only FEM columns and factor third choice

    // Only select the most prioritized factor to use
    val joined = withFallback.
      withColumn("__effective_factor__",
        coalesce(
          col("__factor__first__"),
          col("__factor__second__"),
          col("__factor__third__")
        )
      )

    // Applied the factor on every targeted columns
    log.info(s"Applied the best factor on every targeted columns")

    val selectExprs = input_fem_table.columns.map {
      case c if targetSet.contains(c) => (col(c) * col("__effective_factor__")).alias(c)
      case c => col(c)
    }

    joined.select(selectExprs: _*)
  }

  private def computeJoinFemWithTiers(fem_table: DataFrame, tiers_table: DataFrame): (DataFrame, Seq[String], Seq[String], Seq[String]) = {
    val fem_before_det_table = fem_table
      .withColumnRenamed("rating_code_origin", "rating_code_origin_before_det")
      .withColumnRenamed("id_rating_class", "id_rating_class_before_det")
      .withColumnRenamed("id_rating_class_ori", "id_rating_class_ori_before_det")
      .withColumnRenamed("pd_ttc_chr", "pd_ttc_chr_before_det")
      .withColumnRenamed("tx_pd", "tx_pd_before_det")
      .withColumnRenamed("pd_reg_origination", "pd_reg_origination_before_det")
      .withColumn("pd_reg_origination_src", col("pd_reg_origination_before_det"))

    val tiers_before_det_table = tiers_table.
      withColumnRenamed("rating_code", "rating_code_before_det")

    /// Join FEM + tiers on ctpr_id
    log.info(s"Join FEM + tiers on ctpr_id")

    val femAliased = fem_before_det_table

    val commonColumns = fem_before_det_table.columns.intersect(tiers_before_det_table.columns)

    val tiersBeforeDetColumns = tiers_before_det_table.columns

    var renamedTiersDF = tiers_before_det_table
    for (colName <- commonColumns) {
      renamedTiersDF = renamedTiersDF.withColumnRenamed(colName, s"tiers_$colName")
    }

    // Perform the join and immediately rename the overlapping columns from the tiers side
    val femWithTiers = femAliased.join(renamedTiersDF, femAliased("ctpr_id") === renamedTiersDF("tiers_ctpr_id"), "left")

    val femOutputCols = (fem_before_det_table.columns.toSet ++
      Set(
        "det_source", "id_rating_class", "tx_pd", "det_source_ori",
        "id_rating_class_ori", "rating_code_origin", "pd_reg_origination", "det_source_ori", "pd_ttc_chr",
        "ste_target_rating_scale", "ste_target_rating_scale_origination",
        "mapping_stt_order", "mapping_stt_order_origination",
        "term_structure_origination_deduced", "term_structure_deduced",
        "chrRiskLevelOrigin"
      )).toSeq
    val tiersOutputCols = (tiersBeforeDetColumns.toSet ++ Set("rating_code")).toSeq

    (femWithTiers, femOutputCols, tiersOutputCols, commonColumns)
  }

  private def computeRatingDeterioration(femWithTiersEnrich: DataFrame): (DataFrame) = {
    log.info(s"---- Part 3 : Apply rating deterioration ----")
    // Filter input_deterioration_rating on scenario + year and select lowest rule_num on common lines
    log.info(s"Filter input_deterioration_rating on scenario + year")

    val windowSpec = Window
      .partitionBy(col("original_ctpr_id"), col("rating_scale"))
      .orderBy(col("rule_num").cast("long"))

    val inputDeteriorationRatingFiltered = input_deterioration_rating
      .filter(col("scenario") === frameMeta.scenario && col("year") === frameMeta.date)
      .withColumn("row_num", row_number().over(windowSpec))
      .filter(col("row_num") === 1)
      .select(
        col("original_ctpr_id").alias("__det_original_ctpr_id__"),
        col("value").alias("__det_value__"),
        col("rating_scale").alias("__det_rating_scale__")
      )


    // Compute the worst rating before default from input referential rating scale
    log.info(s"Compute the worst rating before default from input referential rating scale")

    val worstOrderIds = input_referential_rating_scale.
      filter(col("DEFAULT_COLUMN") === 0)
      .groupBy("NOTATION_CODE")
      // Cast ORDER_ID to integer before taking the max
      .agg(max(col("ORDER_ID").cast("integer")).alias("__worst_rating_order_id_before_default__"))
      .as("agg")

    val ratingScale = input_referential_rating_scale.as("src")

    val worstRatingRef = worstOrderIds
      .join(
        ratingScale.hint("broadcast"),
        col("agg.NOTATION_CODE") === col("src.NOTATION_CODE") &&
          col("agg.__worst_rating_order_id_before_default__") === col("src.ORDER_ID").cast("integer")
      )
      .select(
        col("src.NOTATION_CODE").as("NOTATION_CODE"),
        col("agg.__worst_rating_order_id_before_default__").as("__worst_rating_order_id_before_default__"),
        col("src.RATING_NAME").as("__worst_rating_name_before_default__")
      )

    // Filter for STEP1 as of date
    val noDefaultAtAsOfDate = col("stage") =!= "3"
    val isRbScale = col("ste_target_rating_scale").startsWith("RB_") && noDefaultAtAsOfDate
    val isDefaultScale = col("ste_target_rating_scale").isNotNull &&
      col("ste_target_rating_scale") =!= "" &&
      !col("ste_target_rating_scale").startsWith("RB_") &&
      noDefaultAtAsOfDate
    val idRatingClassNotEmpty = col("id_rating_class_before_det").isNotNull && col("id_rating_class_before_det") =!= lit("")
    val ratingCodeNotEmpty = col("rating_code_before_det").isNotNull && col("rating_code_before_det") =!= lit("")

    // Filter for STEP2 origination
    val noDefaultAtOrigination = col("stage") =!= "3" &&
      (col("rating_code_origin_before_det").isNull ||
        col("rating_code_origin_before_det") === lit("") ||
        regexp_replace(col("rating_code_origin_before_det"), "[+-]", "").cast("int") < 11
      ) &&
      col("pd_reg_origination_before_det") =!= "1"
    val isRbScaleOri = col("ste_target_rating_scale_origination").startsWith("RB_") && noDefaultAtOrigination
    val isDefaultScaleOri = col("ste_target_rating_scale_origination").isNotNull &&
      col("ste_target_rating_scale_origination") =!= "" &&
      !col("ste_target_rating_scale_origination").startsWith("RB_") &&
      noDefaultAtOrigination
    val idRatingClassOriNotEmpty = col("id_rating_class_ori_before_det").isNotNull && col("id_rating_class_ori_before_det") =!= lit("")
    val ratingCodeOriNotEmpty = col("rating_code_origin_before_det").isNotNull && col("rating_code_origin_before_det") =!= lit("")

    // Prepare the idealised term structure dataframe from input
    log.info(s"Prepare the idealised term structure dataframe from input")
    // Retrieve only 1Y column

    val idealisedTermStructureFiltered = input_term_structure_idealised
      .select(
        col("matrixName"),
        col("rating"),
        col("1Y")
      )

    val idealisedTermStructureFilteredWithMapping = idealisedTermStructureFiltered
      .join(
        input_mapping_idealised_matrix.hint("broadcast"),
        input_mapping_idealised_matrix("MIGRATION_MATRIX_NAME") === idealisedTermStructureFiltered("matrixname"),
        "left"
      )
      .select(idealisedTermStructureFiltered("*"), col("TARGET_NOTATION_CODE"))

    val idealisedTermStructureFilteredWithMappingAndOrderId = idealisedTermStructureFilteredWithMapping
      .join(
        input_referential_rating_scale.hint("broadcast"),
        idealisedTermStructureFilteredWithMapping("TARGET_NOTATION_CODE") === input_referential_rating_scale("NOTATION_CODE") &&
          idealisedTermStructureFilteredWithMapping("rating") === input_referential_rating_scale("RATING_NAME"),
        "left"
      )
      .select(idealisedTermStructureFilteredWithMapping("*"), col("ORDER_ID").cast("integer").as("ORDER_ID"))

    // Window to get the next value line 1Y
    val idealisedWindow = Window.partitionBy("matrixname").orderBy("ORDER_ID")
    // Window with no bound over current matrixname to get the max
    val maxWindow = Window.partitionBy("matrixname")
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    val idealisedTermStructureRanked = idealisedTermStructureFilteredWithMappingAndOrderId
      .withColumn("__next_1y__", lead(col("1Y"), 1).over(idealisedWindow))
      .withColumn("__max_row_num__", max(col("ORDER_ID")).over(maxWindow))

    // Join with input deterioration rating on original_ctpr_id
    log.info(s"Join with input deterioration rating on original_ctpr_id")

    val femWithDetAsOf = femWithTiersEnrich
      .join(
        inputDeteriorationRatingFiltered.hint("broadcast").as("det_as_of"),
        femWithTiersEnrich("original_ctpr_id") === col("det_as_of.__det_original_ctpr_id__") &&
          femWithTiersEnrich("ste_target_rating_scale") === col("det_as_of.__det_rating_scale__"),
        "left"
      )
      .select(
        femWithTiersEnrich("*"),
        col("det_as_of.__det_value__").as("__det_value__")
      )

    val femWithDetAsOri = femWithDetAsOf
      .join(
        inputDeteriorationRatingFiltered.hint("broadcast").as("det_orig"),
        col("original_ctpr_id") === col("det_orig.__det_original_ctpr_id__") &&
          col("ste_target_rating_scale_origination") === col("det_orig.__det_rating_scale__"),
        "left"
      )
      .select(
        femWithDetAsOf("*"),
        col("det_orig.__det_value__").as("__det_value_origination__")
      )

    // Join with worst rating df
    log.info(s"Join with worst rating df")

    val femWithWorst = femWithDetAsOri
      .join(
        worstRatingRef.hint("broadcast"),
        femWithDetAsOri("ste_target_rating_scale") === worstRatingRef("NOTATION_CODE"),
        "left"
      )
      .select(
        femWithDetAsOri("*"),
        //        col("__worst_rating_name_before_default__"), //TODO dont need it now ?
        col("__worst_rating_order_id_before_default__")
      )

    val femWithWorstOrigination = femWithWorst
      .join(
        worstRatingRef.hint("broadcast").as("ref_2"),
        femWithWorst("ste_target_rating_scale_origination") === col("ref_2.NOTATION_CODE"),
        "left"
      )
      .select(
        femWithWorst("*"),
        //        col("ref_2.__worst_rating_name_before_default__").as("__worst_rating_name_origination_before_default__"), //TODO dont neet it now ?
        col("ref_2.__worst_rating_order_id_before_default__").as("__worst_rating_order_id_origination_before_default__")
      )
    // Apply STEP 1: As-of date rating deterioration
    log.info(s"Apply STEP 1: As-of date rating deterioration")

    // Step 1.A - compute new id_rating_class / rating_code / det_source columns
    log.info(s"Step 1.A - compute  new id_rating_class / rating_code / det_source columns")

    val withComputedCols = femWithWorstOrigination
      //        .withColumn(
      //          "worst_rating_before_default",
      //          when(isRbScale && idRatingClassNotEmpty, col("__worst_rating_name_before_default__"))
      //            .when(isDefaultScale, col("__worst_rating_name_before_default__"))
      //            .otherwise(lit(null).cast("string"))
      //      )
      .withColumn(
        "__new_id_rating_class__",
        when(isRbScale && idRatingClassNotEmpty && col("__det_value__").isNotNull, // Case null no match in referential rating
          //In case of RB_ rate are 1 from 8 so can do directly the addition with the order_id which should correspond
          least(col("id_rating_class_before_det") + col("__det_value__"), col("__worst_rating_order_id_before_default__").cast("integer")).cast("integer")
        )
          .otherwise(lit(null).cast("integer"))
      )
      //Apply new columns values before tx_pd lookup
      .withColumn("id_rating_class", col("__new_id_rating_class__"))

    // Step 1.2 Step idealized term structure lookup for DEFAULT / rating code empty
    log.info(s"Step 1.2 Step idealized term structure lookup for DEFAULT / rating code empty")

    val withIdealised = withComputedCols
      .join(idealisedTermStructureRanked.hint("broadcast"),
        withComputedCols("term_structure_deduced") === idealisedTermStructureRanked("matrixname") &&
          isDefaultScale && !ratingCodeNotEmpty,
        "left"
      )
      .select(
        withComputedCols("*"),
        col("1Y"),
        col("rating"),
        col("ORDER_ID").as("__order_id__").cast("integer"),
        col("__next_1y__"),
        col("__max_row_num__")
      )

    val withIdealisedRatingCode = withIdealised.
      withColumn(
        "__idealised_rating_code__",
        when(isDefaultScale && !ratingCodeNotEmpty,
          when(col("pd_ttc_chr_before_det").cast("double") < col("1Y").cast("double") && col("__order_id__") === 1,
            // PD_TTC_CHR < first threshold --> first rating
            col("rating")
          )
            .when(col("pd_ttc_chr_before_det").cast("double") === col("1Y").cast("double"),
              //Exact match --> corresponding rating
              col("rating")
            )
            .when(col("pd_ttc_chr_before_det").cast("double") > col("1Y").cast("double") && col("__order_id__") === col("__max_row_num__"),
              // PD_TTC_CHR > max threshold --> last rating
              col("rating")
            )
            .when(col("pd_ttc_chr_before_det").cast("double") > col("1Y").cast("double") && col("pd_ttc_chr_before_det").cast("double") < col("__next_1y__").cast("double"),
              // PD_TTC_CHR falls between this and next --> last preceding rating
              col("rating")
            )
            .otherwise(lit(null).cast("string"))
        )
      )
      // Collapse : keep the single non-null matched rating per FEM row
      .withColumn(
        "__idealised_rating_code__",
        when(
          isDefaultScale && !ratingCodeNotEmpty,
          first(col("__idealised_rating_code__"), ignoreNulls = true)
            .over(Window.partitionBy(col("id_technique")))
        )
      )

    // Add rating code in the 2 possibles cases
    log.info(s"Add rating code in the 2 possibles cases")

    // Retrieve corresponding rating_code order id for every rating_code
    val withCorrespondingOrderIdRatingCode = withIdealisedRatingCode
      .join(
        input_referential_rating_scale.hint("broadcast"),
        withIdealisedRatingCode("ste_target_rating_scale") === input_referential_rating_scale("NOTATION_CODE") &&
          coalesce(
            when(col("rating_code_before_det") =!= "", col("rating_code_before_det")),
            col("__idealised_rating_code__")
          ) === input_referential_rating_scale("RATING_NAME"),
        "left"
      )
      .select(withIdealisedRatingCode("*"), col("ORDER_ID").cast("integer").as("__rating_code_order_id__"))

    // Determine new rank for default scale and rating code not empty or rating code deduced with idealised
    val withNewRatingCodeOrderId = withCorrespondingOrderIdRatingCode
      .withColumn("__new_rating_code_order_id__",
        when(isDefaultScale && col("__det_value__").isNotNull,
          least(col("__rating_code_order_id__") + col("__det_value__"), col("__worst_rating_order_id_before_default__").cast("integer")).cast("integer"))
          .otherwise(lit(null)).cast("integer")
      )

    val witNewRatingCode = withNewRatingCodeOrderId
      .join(
        input_referential_rating_scale.hint("broadcast"),
        withNewRatingCodeOrderId("ste_target_rating_scale") === input_referential_rating_scale("NOTATION_CODE") &&
          withNewRatingCodeOrderId("__new_rating_code_order_id__") === input_referential_rating_scale("ORDER_ID"),
        "left"
      )
      .select(withNewRatingCodeOrderId("*"), col("RATING_NAME").as("__new_rating_code__"))

    val withRatingCode = witNewRatingCode
      .withColumn(
        "rating_code",
        when(isDefaultScale && col("__new_rating_code__").isNotNull,
          col("__new_rating_code__")
        )
          .otherwise(col("rating_code_before_det"))
      )
      .withColumn(
        "det_source",
        when(isRbScale && idRatingClassNotEmpty && col("__det_value__").isNotNull, lit("HRC"))
          .when(isDefaultScale && ratingCodeNotEmpty && col("__det_value__").isNotNull, lit("Rating"))
          .when(isDefaultScale && !ratingCodeNotEmpty && col("__idealised_rating_code__").isNotNull && col("__det_value__").isNotNull, lit("deduced Rating"))
          .otherwise(lit(null).cast("string"))
      )
      .drop("__idealised_rating_code__", "__new_rating_code__", "1Y", "__order_id__", "rating", "__next_1y__", "__max_row_num__")

    // Step 1.1 TX_PD fpr RB_ rows: join with REF_HRC_PD
    log.info(s"Step 1.1 TX_PD for RB_ rows: join with REF_HRC_PD")

    val withTxPd = withRatingCode
      .join(
        input_referential_hrc_pd.hint("broadcast"),
        withRatingCode("pd_model") === input_referential_hrc_pd("PD_MODEL") &&
          withRatingCode("id_rating_class") === input_referential_hrc_pd("HRC"),
        "left"
      )
      .select(withRatingCode("*"), input_referential_hrc_pd("PD").alias("__new_tx_pd__"))

    // Apply tx_pd only for RB_ rows, keep original otherwise
    val withFinalTxPd = withTxPd
      .withColumn(
        "tx_pd",
        when(isRbScale && idRatingClassNotEmpty && col("__det_value__").isNotNull, col("__new_tx_pd__"))
          .otherwise(col("tx_pd_before_det"))
      )

    // Apply STEP 2: Origination rating deterioration
    log.info(s"Apply STEP 2: Origination rating deterioration")

    val updatedFemWithOri = withFinalTxPd
      .withColumn(
        "__new_id_rating_class_ori__",
        when(isRbScaleOri && idRatingClassOriNotEmpty && col("__det_value_origination__").isNotNull,
          least(col("id_rating_class_ori_before_det") + col("__det_value_origination__"),
            col("__worst_rating_order_id_origination_before_default__").cast("integer")).cast("integer")
        )
          .otherwise(lit(null).cast("integer"))
      )

    val withIdealisedAtOri = updatedFemWithOri
      .join(idealisedTermStructureRanked.hint("broadcast"),
        updatedFemWithOri("term_structure_origination_deduced") === idealisedTermStructureRanked("matrixname") &&
          isDefaultScaleOri && !ratingCodeOriNotEmpty,
        "left"
      )
      .select(
        updatedFemWithOri("*"),
        col("1Y"),
        col("rating"),
        col("ORDER_ID").as("__order_id__").cast("integer"),
        col("__next_1y__"),
        col("__max_row_num__")
      )

    val updateFemWithRatingCodeOriginIdealised = withIdealisedAtOri.
      withColumn(
        "__idealised_rating_code_ori__",
        when(isDefaultScaleOri && !ratingCodeOriNotEmpty,
          // Deduce rating_code_origin from pd_reg_origination
          when(col("pd_reg_origination_before_det").cast("double") < col("1Y").cast("double") && col("__order_id__") === 1,
            col("rating")
          )
            .when(col("pd_reg_origination_before_det").cast("double") === col("1Y").cast("double"),
              col("rating")
            )
            .when(col("pd_reg_origination_before_det").cast("double") > col("1Y").cast("double") && col("__order_id__") === col("__max_row_num__"),
              col("rating")
            )
            .when(col("pd_reg_origination_before_det").cast("double") > col("1Y").cast("double") && col("pd_reg_origination_before_det").cast("double") < col("__next_1y__").cast("double"),
              col("rating")
            )
            .otherwise(lit(null).cast("string"))
        )
      )
      .withColumn(
        "__idealised_rating_code_ori__",
        when(isDefaultScaleOri && !ratingCodeOriNotEmpty,
          first(col("__idealised_rating_code_ori__"), ignoreNulls = true)
            .over(Window.partitionBy(col("id_technique")))
        )
      )
      .drop(col("__order_id__"))

    log.info(s"Add rating code origination in the 2 possibles cases")

    val withCorrespondingOrderIdRatingCodeOri = updateFemWithRatingCodeOriginIdealised
      .join(
        input_referential_rating_scale.hint("broadcast"),
        updateFemWithRatingCodeOriginIdealised("ste_target_rating_scale_origination") === input_referential_rating_scale("NOTATION_CODE") &&
          coalesce(
            when(col("rating_code_origin_before_det") =!= "", col("rating_code_origin_before_det")),
            col("__idealised_rating_code_ori__")
          ) === input_referential_rating_scale("RATING_NAME"),
        "left"
      )
      .select(updateFemWithRatingCodeOriginIdealised("*"), col("ORDER_ID").cast("integer").as("__rating_code_origin_order_id__"))

    val withNewRatingCodeOriginOrderId = withCorrespondingOrderIdRatingCodeOri
      .withColumn("__new_rating_code_origin_order_id__",
        when(isDefaultScaleOri && col("__det_value_origination__").isNotNull,
          least(col("__rating_code_origin_order_id__") + col("__det_value_origination__"),
            col("__worst_rating_order_id_origination_before_default__").cast("integer")).cast("integer"))
          .otherwise(lit(null)).cast("integer")
      )

    val witNewRatingCodeOrigin = withNewRatingCodeOriginOrderId
      .join(
        input_referential_rating_scale.hint("broadcast"),
        withNewRatingCodeOriginOrderId("ste_target_rating_scale_origination") === input_referential_rating_scale("NOTATION_CODE") &&
          withNewRatingCodeOriginOrderId("__new_rating_code_origin_order_id__") === input_referential_rating_scale("ORDER_ID"),
        "left"
      )
      .select(withNewRatingCodeOriginOrderId("*"), col("RATING_NAME").as("__new_rating_code_origin__"))

    val updateFemWithNewRatingCodeOri = witNewRatingCodeOrigin.
      withColumn(
        "rating_code_origin",
        when(isDefaultScaleOri && col("__new_rating_code_origin__").isNotNull,
          col("__new_rating_code_origin__")
        )
          .otherwise(col("rating_code_origin_before_det"))
      )

    val updateFemWithDetSource = updateFemWithNewRatingCodeOri
      .withColumn(
        "det_source_ori",
        when(isRbScaleOri && idRatingClassOriNotEmpty && col("__det_value_origination__").isNotNull, lit("HRC"))
          .when(isDefaultScaleOri && ratingCodeOriNotEmpty && col("__det_value_origination__").isNotNull, lit("Rating"))
          .when(isDefaultScaleOri && !ratingCodeOriNotEmpty && col("__idealised_rating_code_ori__").isNotNull && col("__det_value_origination__").isNotNull, lit("deduced Rating"))
          .otherwise(lit(null).cast("string"))
      )
      .withColumn("id_rating_class_ori",
        when(isRbScaleOri && idRatingClassOriNotEmpty && col("__det_value_origination__").isNotNull,
          col("__new_id_rating_class_ori__"))
          .otherwise(lit(null)).cast("integer")
      )
      .drop("1Y", "rating", "__row_num__", "__next_1y__", "__max_row_num__",
        "__idealised_rating_code_ori__", "__new_rating_code_origin__",
        "__new_id_rating_class_ori__")


    val updatedFemWithTxPdOri = updateFemWithDetSource
      .join(
        input_referential_hrc_pd.hint("broadcast").as("pd_ref_ori"),
        updateFemWithDetSource("pd_model_origination") === input_referential_hrc_pd("PD_MODEL") &&
          updateFemWithDetSource("id_rating_class_ori") === input_referential_hrc_pd("HRC"),
        "left"
      )
      .select(
        updateFemWithDetSource("*"),
        col("pd_ref_ori.PD").alias("__new_pd_reg_origination__")
      )

    val withTxPdOri = updatedFemWithTxPdOri
      .withColumn(
        "pd_reg_origination",
        when(isRbScaleOri && idRatingClassOriNotEmpty && col("__det_value_origination__").isNotNull, col("__new_pd_reg_origination__"))
          .otherwise(col("pd_reg_origination_before_det"))
      )

    // Drop all internal working columns
    val finalDf = withTxPdOri
      .drop("__det_value__",
        "__det_value_origination__",
        //        "worst_rating_before_default",
        "__worst_rating_order_id_origination_before_default__",
        "__worst_rating_order_id_before_default__",
        "__rating_code_order_id__",
        "__new_rating_code_order_id__",
        "__rating_code_origin_order_id__",
        "__new_rating_code_origin_order_id__",
        "__new_id_rating_class__",
        "__new_rating_code__",
        "__new_tx_pd__",
        "__new_pd_reg_origination__")

    finalDf
  }

  private def computePdDeterioration(df: DataFrame): DataFrame = {

    val dfDistinct = df.distinct()

    log.info(s"---- Part 4 : Apply pd extension deterioration ----")

    val inputDeteriorationPdFiltered = input_deterioration_pd
      .filter(col("scenario") === frameMeta.scenario && col("year") === frameMeta.date)
      .withColumn("cap",
        (regexp_replace(regexp_replace(trim(col("cap")), "%", ""), ",", ".").cast("double") / 100)
      )
      .select(
        col("rule_num").alias("__det_rule_num__"),
        col("pd_model_short_name").alias("__det_pd_model_short_name__"),
        col("geographical_breakdown").alias("__det_geographical_breakdown__"),
        col("nace_sector_ecb").alias("__det_nace_sector_ecb__"),
        col("epc_rating").alias("__det_epc_rating__"),
        col("cd_niv_risq_chr").alias("__det_cd_niv_risq_chr__"),
        col("rating_scale").alias("__det_rating_scale__"),
        col("value").alias("__det_value__"),
        col("cap").alias("__det_cap__")
      )

    log.info(s"Join with input deterioration pd extension")
    val windowSpecAsOf = Window
      .partitionBy(col("id_technique"))
      .orderBy(col("det_as_of.__det_rule_num__").cast("long").asc)

    val withPdDeteriorationAsOf = dfDistinct.join(
      inputDeteriorationPdFiltered.as("det_as_of").hint("broadcast"),
      (
        (col("pd_model").substr(1, 6) === col("det_as_of.__det_pd_model_short_name__").substr(1, 6) ||
          col("det_as_of.__det_pd_model_short_name__") === "") &&
        (col("cd_pays_residence_cal") === col("det_as_of.__det_geographical_breakdown__") ||
          col("det_as_of.__det_geographical_breakdown__") === "") &&
        (col("cd_sect_wiod") === col("det_as_of.__det_nace_sector_ecb__") ||
          col("det_as_of.__det_nace_sector_ecb__") === "") &&
        (col("nace_sector_ecb_epc_rating") === col("det_as_of.__det_epc_rating__") ||
          col("det_as_of.__det_epc_rating__") === "") &&
        (col("cd_niv_risq_chr") === col("det_as_of.__det_cd_niv_risq_chr__") ||
          col("det_as_of.__det_cd_niv_risq_chr__") === "") &&
        (col("ste_target_rating_scale") === col("det_as_of.__det_rating_scale__") ||
          col("det_as_of.__det_rating_scale__") === "")
      ),
      "left"
    )
      .withColumn("__row_num__", row_number().over(windowSpecAsOf))
      .filter(col("__row_num__") === 1)
      .select(
        dfDistinct("*"),
        col("det_as_of.__det_value__").as("__det_value__"),
        col("det_as_of.__det_cap__").as("__det_cap__")
      )

    val windowSpecAsOrig = Window
      .partitionBy(col("id_technique"))
      .orderBy(col("det_orig.__det_rule_num__").cast("long").asc)

    val withPdDeteriorationAsOri = withPdDeteriorationAsOf.join(
      inputDeteriorationPdFiltered.as("det_orig").hint("broadcast"),
      (
        (col("pd_model_origination").substr(1, 6) === col("det_orig.__det_pd_model_short_name__").substr(1, 6) ||
          col("det_orig.__det_pd_model_short_name__") === "") &&
        (col("cd_pays_residence_cal") === col("det_orig.__det_geographical_breakdown__") ||
          col("det_orig.__det_geographical_breakdown__") === "") &&
        (col("cd_sect_wiod") === col("det_orig.__det_nace_sector_ecb__") ||
          col("det_orig.__det_nace_sector_ecb__") === "") &&
        (col("nace_sector_ecb_epc_rating") === col("det_orig.__det_epc_rating__") ||
          col("det_orig.__det_epc_rating__") === "") &&
        (col("chr_at_origination") === col("det_orig.__det_cd_niv_risq_chr__") ||
          col("det_orig.__det_cd_niv_risq_chr__") === "") &&
        (col("ste_target_rating_scale_origination") === col("det_orig.__det_rating_scale__") ||
          col("det_orig.__det_rating_scale__") === "")
      ),
      "left"
    )
      .withColumn("__row_num__", row_number().over(windowSpecAsOrig))
      .filter(col("__row_num__") === 1)
      .select(
        withPdDeteriorationAsOf("*"),
        col("det_orig.__det_value__").as("__det_value_origination__"),
        col("det_orig.__det_cap__").as("__det_cap_origination__")
      )

    log.info(s"Prepare idealized term structure data for mapping")

    val idealisedTermStructureFiltered = input_term_structure_idealised
      .select(
        col("matrixName"),
        col("rating"),
        col("1Y")
      )
    val idealisedTermStructureFilteredWithMapping = idealisedTermStructureFiltered
      .join(
        input_mapping_idealised_matrix.hint("broadcast"),
        input_mapping_idealised_matrix("MIGRATION_MATRIX_NAME") === idealisedTermStructureFiltered("matrixname"),
        "left"
      )
      .select(idealisedTermStructureFiltered("*"), col("TARGET_NOTATION_CODE"))

    val idealisedTermStructureFilteredWithMappingAndOrderId = idealisedTermStructureFilteredWithMapping
      .join(
        input_referential_rating_scale.hint("broadcast"),
        idealisedTermStructureFilteredWithMapping("TARGET_NOTATION_CODE") === input_referential_rating_scale("NOTATION_CODE") &&
          idealisedTermStructureFilteredWithMapping("rating") === input_referential_rating_scale("RATING_NAME"),
        "left"
      )
      .select(idealisedTermStructureFilteredWithMapping("*"), col("ORDER_ID").cast("integer").as("ORDER_ID"))

    val idealisedWindow = Window.partitionBy("matrixname").orderBy("ORDER_ID")
    val maxWindow = Window.partitionBy("matrixname")
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    val idealisedTermStructureRanked = idealisedTermStructureFilteredWithMappingAndOrderId
      .withColumn("__next_1y__", lead(col("1Y"), 1).over(idealisedWindow))
      .withColumn("__max_row_num__", max(col("ORDER_ID")).over(maxWindow))

    log.info(s"Apply STEP 1: As-of date PD deterioration")

    val noDefaultAtAsOfDate = col("stage") =!= "3"
    val isRbScale = col("ste_target_rating_scale").startsWith("RB_") && noDefaultAtAsOfDate
    val isDefaultScale = col("ste_target_rating_scale").isNotNull &&
      col("ste_target_rating_scale") =!= "" &&
      !col("ste_target_rating_scale").startsWith("RB_") &&
      noDefaultAtAsOfDate
    val idRatingClassNotEmpty = col("id_rating_class_before_det").isNotNull && col("id_rating_class_before_det") =!= lit("")
    val txPdNotEmpty = col("tx_pd_before_det").isNotNull
    val ratingCodeNotEmpty = col("rating_code_before_det").isNotNull && col("rating_code_before_det") =!= lit("")
    val pdTtcChrNotEmpty = col("pd_ttc_chr_before_det").isNotNull

    log.info(s"Find new_tx_pd / new_pd_ttc_chr / det_source")

    val withStep1AppliedPart1 = withPdDeteriorationAsOri
      .join(
        input_referential_hrc_pd.hint("broadcast").as("pd_extension_ref_1"),
        withPdDeteriorationAsOri("pd_model") === input_referential_hrc_pd("PD_MODEL") &&
          withPdDeteriorationAsOri("id_rating_class_before_det") === input_referential_hrc_pd("HRC"),
        "left"
      )
      .select(
        withPdDeteriorationAsOri("*"),
        col("pd_extension_ref_1.PD").alias("__new_tx_pd_deduced__")
      )
      .withColumn(
        "__new_tx_pd__",
        when(isRbScale && idRatingClassNotEmpty && col("__det_value__").isNotNull && col("__new_tx_pd_deduced__").isNotNull,
          least(
            greatest(col("__new_tx_pd_deduced__").cast("double"), lit(0.0000001)) * col("__det_value__").cast("double"),
            col("__det_cap__").cast("double")
          )
        )
          .when(isRbScale && !idRatingClassNotEmpty && col("__det_value__").isNotNull && txPdNotEmpty,
            least(
              greatest(col("tx_pd_before_det").cast("double"), lit(0.0000001)) * col("__det_value__").cast("double"),
              col("__det_cap__").cast("double")
            )
          )
      )
      .join(
        input_referential_hrc_pd.hint("broadcast").as("pd_extension_ref_2"),
        withPdDeteriorationAsOri("pd_model") === input_referential_hrc_pd("PD_MODEL") &&
          col("__new_tx_pd__").cast("double") >= input_referential_hrc_pd("PD_MIN").cast("double") &&
          col("__new_tx_pd__").cast("double") < input_referential_hrc_pd("PD_MAX").cast("double"),
        "left"
      )
      .select(withPdDeteriorationAsOri("*"), col("pd_extension_ref_2.HRC").alias("__new_id_rating_class__").cast("integer"), col("__new_tx_pd_deduced__"), col("__new_tx_pd__"))


    val withStep1AppliedPart2 = withStep1AppliedPart1
      .join(
        idealisedTermStructureRanked.hint("broadcast").as("term_1"),
        col("term_structure_deduced") === col("term_1.matrixname") &&
          col("rating_code_before_det") === col("term_1.rating"),
        "left"
      )
      .select(withStep1AppliedPart1("*"), col("term_1.1Y").alias("__new_pd_ttc_chr_deduced__"))
      .withColumn(
        "__new_pd_ttc_chr__",
        when(isDefaultScale && ratingCodeNotEmpty && col("__det_value__").isNotNull && col("__new_pd_ttc_chr_deduced__").isNotNull,
          least(
            greatest(col("__new_pd_ttc_chr_deduced__").cast("double"), lit(0.0000001)) * col("__det_value__").cast("double"),
            col("__det_cap__").cast("double")
          )
        )
          .when(isDefaultScale && !ratingCodeNotEmpty && col("__det_value__").isNotNull && pdTtcChrNotEmpty,
            least(
              greatest(col("pd_ttc_chr_before_det").cast("double"), lit(0.0000001)) * col("__det_value__").cast("double"),
              col("__det_cap__").cast("double")
            )
          )
      )
      .withColumn(
        "det_source",
        when(isRbScale && idRatingClassNotEmpty && col("det_source").isNull && txPdNotEmpty && col("__det_value__").isNotNull && col("__new_tx_pd_deduced__").isNotNull,
          lit("PD | deduced HRC"))
          .when(isRbScale && !idRatingClassNotEmpty && col("det_source").isNull && txPdNotEmpty && col("__det_value__").isNotNull, lit("PD"))
          .when(isDefaultScale && ratingCodeNotEmpty && col("det_source").isNull && col("__det_value__").isNotNull && col("__new_pd_ttc_chr_deduced__").isNotNull,
            lit("PD | deduced rating"))
          .when(isDefaultScale && !ratingCodeNotEmpty && col("det_source").isNull && pdTtcChrNotEmpty && col("__det_value__").isNotNull, lit("PD"))
          .otherwise(col("det_source"))
      )

    val withStep1AppliedPart3 = withStep1AppliedPart2
      .join(
        idealisedTermStructureRanked.hint("broadcast").as("term"),
        col("term_structure_deduced") === col("term.matrixname") &&
          col("__new_pd_ttc_chr__").cast("double") >= col("term.1Y").cast("double") &&
          col("__new_pd_ttc_chr__").cast("double") < col("term.__next_1y__").cast("double"),
        "left"
      )
      .select(withStep1AppliedPart2("*"), col("term.rating").alias("__new_rating_code__").cast("integer"))
      .withColumn(
        "__new_rating_code__",
        when(isDefaultScale && ratingCodeNotEmpty && pdTtcChrNotEmpty && col("__new_pd_ttc_chr__").isNotNull && col("det_source") === "PD | deduced rating",
          first(col("__new_rating_code__"), ignoreNulls = true)
            .over(Window.partitionBy(col("id_technique")))
        )
      )
      .withColumn("rating_code",
        when(isDefaultScale && ratingCodeNotEmpty && pdTtcChrNotEmpty && col("__new_rating_code__").isNotNull,
          col("__new_rating_code__"))
          .otherwise(col("rating_code"))
      )
      .withColumn("id_rating_class",
        when(isRbScale && idRatingClassNotEmpty && txPdNotEmpty && col("__new_id_rating_class__").isNotNull && col("det_source") === "PD | deduced HRC",
          col("__new_id_rating_class__"))
          .otherwise(col("id_rating_class"))
      )
      .withColumn("pd_ttc_chr",
        when(isDefaultScale && col("__new_pd_ttc_chr__").isNotNull && col("det_source").startsWith("PD"),
          col("__new_pd_ttc_chr__"))
          .otherwise(col("pd_ttc_chr_before_det"))
      )
      .withColumn("tx_pd",
        when(isRbScale && col("__new_tx_pd__").isNotNull && col("det_source").startsWith("PD"),
          col("__new_tx_pd__"))
          .otherwise(col("tx_pd"))
      )
      .drop("__new_tx_pd_deduced__", "__new_pd_ttc_chr_deduced__", "__new_tx_pd__", "__new_pd_ttc_chr__", "__new_id_rating_class__", "__new_rating_code__")

    // STEP 2: Origination PD deterioration
    log.info(s"Apply STEP 2: Origination PD deterioration")

    val noDefaultAtOrigination = col("stage") =!= "3" &&
      (col("rating_code_origin_before_det").isNull ||
        col("rating_code_origin_before_det") === lit("") ||
        regexp_replace(col("rating_code_origin_before_det"), "[+-]", "").cast("int") < 11
      ) &&
      col("pd_reg_origination_before_det") =!= "1"
    val isRbScaleOri = col("ste_target_rating_scale_origination").startsWith("RB_") && noDefaultAtOrigination
    val isDefaultScaleOri = col("ste_target_rating_scale_origination").isNotNull &&
      col("ste_target_rating_scale_origination") =!= "" &&
      !col("ste_target_rating_scale_origination").startsWith("RB_") &&
      noDefaultAtOrigination
    val idRatingClassOriNotEmpty = col("id_rating_class_ori_before_det").isNotNull && col("id_rating_class_ori_before_det") =!= lit("")
    val pdRegOriginationNotEmpty = col("pd_reg_origination_before_det").isNotNull
    val ratingCodeOriginNotEmpty = col("rating_code_origin_before_det").isNotNull && col("rating_code_origin_before_det") =!= lit("")

    log.info(s"Find new_pd_reg_origination / det_source_ori")

    val withStep2AppliedPart1 = withStep1AppliedPart3
      .join(
        input_referential_hrc_pd.hint("broadcast").as("pd_extension_ref_3"),
        withPdDeteriorationAsOri("pd_model_origination") === input_referential_hrc_pd("PD_MODEL") &&
          withPdDeteriorationAsOri("id_rating_class_ori_before_det") === input_referential_hrc_pd("HRC"),
        "left"
      )
      .select(
        withStep1AppliedPart3("*"),
        col("pd_extension_ref_3.PD").alias("__new_pd_reg_origination_deduced__")
      )

    val withStep2AppliedPart11 = withStep2AppliedPart1
      .join(
        idealisedTermStructureRanked.hint("broadcast").as("term_4"),
        col("term_structure_origination_deduced") === col("term_4.matrixname") &&
          col("rating_code_origin_before_det") === col("term_4.rating"),
        "left"
      )
      .select(withStep2AppliedPart1("*"), col("term_4.1Y").alias("__new_pd_reg_origination_deduced_2__"))
      .withColumn(
        "__new_pd_reg_origination__",
        when(isRbScaleOri && idRatingClassOriNotEmpty && col("__det_value_origination__").isNotNull && col("__new_pd_reg_origination_deduced__").isNotNull,
          least(
            greatest(col("__new_pd_reg_origination_deduced__").cast("double"), lit(0.0000001)) * col("__det_value_origination__").cast("double"),
            col("__det_cap_origination__").cast("double")
          )
        )
          .when(isRbScaleOri && !idRatingClassOriNotEmpty && pdRegOriginationNotEmpty && col("__det_value_origination__").isNotNull,
            least(
              greatest(col("pd_reg_origination_before_det").cast("double"), lit(0.0000001)) * col("__det_value_origination__").cast("double"),
              col("__det_cap_origination__").cast("double")
            )
          )
          .when(isDefaultScaleOri && ratingCodeOriginNotEmpty && col("__det_value_origination__").isNotNull && col("__new_pd_reg_origination_deduced_2__").isNotNull,
            least(
              greatest(col("__new_pd_reg_origination_deduced_2__").cast("double"), lit(0.0000001)) * col("__det_value_origination__").cast("double"),
              col("__det_cap_origination__").cast("double")
            )
          )
          .when(isDefaultScaleOri && !ratingCodeOriginNotEmpty && pdRegOriginationNotEmpty && col("__det_value_origination__").isNotNull,
            least(
              greatest(col("pd_reg_origination_before_det").cast("double"), lit(0.0000001)) * col("__det_value_origination__").cast("double"),
              col("__det_cap_origination__").cast("double")
            )
          )
      )
      .join(
        input_referential_hrc_pd.hint("broadcast").as("pd_extension_ref_ori"),
        withStep2AppliedPart1("pd_model_origination") === input_referential_hrc_pd("PD_MODEL") &&
          col("__new_pd_reg_origination__").cast("double") >= input_referential_hrc_pd("PD_MIN").cast("double") &&
          col("__new_pd_reg_origination__").cast("double") < input_referential_hrc_pd("PD_MAX").cast("double"),
        "left"
      )
      .select(withStep2AppliedPart1("*"), col("__new_pd_reg_origination__"), col("pd_extension_ref_ori.HRC").alias("__new_id_rating_class_ori__").cast("integer"))
      .withColumn(
        "det_source_ori",
        when(isRbScaleOri && idRatingClassOriNotEmpty && pdRegOriginationNotEmpty && col("__det_value_origination__").isNotNull && col("det_source_ori").isNull, lit("PD | deduced HRC"))
          .when(isRbScaleOri && !idRatingClassOriNotEmpty && pdRegOriginationNotEmpty && col("__det_value_origination__").isNotNull && col("det_source_ori").isNull, lit("PD"))
          .when(isDefaultScaleOri && ratingCodeOriginNotEmpty && pdRegOriginationNotEmpty && col("__det_value_origination__").isNotNull
            && col("det_source_ori").isNull && col("__new_pd_reg_origination__").isNotNull, lit("PD | deduced rating"))
          .when(isDefaultScaleOri && !ratingCodeOriginNotEmpty && pdRegOriginationNotEmpty && col("__det_value_origination__").isNotNull && col("det_source_ori").isNull, lit("PD"))
          .otherwise(col("det_source_ori"))
      )


    val withStep2AppliedPart2 = withStep2AppliedPart11
      .join(
        idealisedTermStructureRanked.hint("broadcast").as("term_2"),
        col("term_structure_origination_deduced") === col("term_2.matrixname") &&
          col("__new_pd_reg_origination__").cast("double") >= col("term_2.1Y").cast("double") &&
          col("__new_pd_reg_origination__").cast("double") < col("term_2.__next_1y__").cast("double"),
        "left"
      )
      .select(withStep2AppliedPart11("*"), col("term_2.rating").alias("__new_rating_code_ori__").cast("integer"))
      .withColumn(
        "__new_rating_code_ori__",
        when(isDefaultScaleOri && ratingCodeOriginNotEmpty && pdRegOriginationNotEmpty && col("__new_pd_reg_origination__").isNotNull && col("det_source_ori") === "PD | deduced rating",
          first(col("__new_rating_code_ori__"), ignoreNulls = true)
            .over(Window.partitionBy(col("id_technique")))
        )
      )
      .withColumn("rating_code_origin",
        when(isDefaultScaleOri && ratingCodeOriginNotEmpty && pdRegOriginationNotEmpty && col("__new_rating_code_ori__").isNotNull,
          col("__new_rating_code_ori__"))
          .otherwise(col("rating_code_origin"))
      )
      .withColumn("id_rating_class_ori",
        when(isRbScaleOri && idRatingClassOriNotEmpty && pdRegOriginationNotEmpty && col("__new_id_rating_class_ori__").isNotNull && col("det_source_ori") === "PD | deduced HRC",
          col("__new_id_rating_class_ori__"))
          .otherwise(col("id_rating_class_ori"))
      )
      .withColumn("pd_reg_origination",
        when((isRbScaleOri || isDefaultScaleOri) && col("__new_pd_reg_origination__").isNotNull && col("det_source_ori").startsWith("PD"),
          col("__new_pd_reg_origination__"))
          .otherwise(col("pd_reg_origination"))
      )
      .drop("__new_pd_reg_origination__", "__new_id_rating_class_ori__", "__new_rating_code_ori__", "__new_pd_reg_origination_deduced__")

    withStep2AppliedPart2
  }

  private def computeOriginationDate(df: DataFrame): DataFrame = {
    log.info(s"---- Part 2 : Apply new origination date ----")
    // Last month of each quarter
    val lastMonthQuarter: Map[Int, Int] = Map(
      1 -> 3,
      2 -> 6,
      3 -> 9,
      4 -> 12
    )

    // ---- 1. Parse reference_date "YYYYQX" --> (year, quarter) ----
    val refPattern = """^(\d{4})Q([1-4])$""".r
    val (refYear, refQuarter) = frameMeta.baseDate match {
      case refPattern(year, quarter) => (year.toInt, quarter.toInt)
      case _ => throw new IllegalArgumentException(
        s"Reference date must match YYYYQ[1-4], got : $frameMeta.baseDate"
      )
    }

    val refMonth = lastMonthQuarter(refQuarter)

    // ---- 2. Parse projection_date "YYYY" --> projection year ----
    val projYear = frameMeta.date.trim.toInt

    // anchor for projection side is always Q1 of projYear ==> month = 12
    val anchorProjYear = projYear - 2
    val anchorProjMonth = 12 // month for Q4

    // ---- 3. Build the two fixed anchor dates as literals (day = 15) ----
    val anchorReferenceDate = LocalDate.of(refYear, refMonth, 15)
    val anchorProjectionDate = LocalDate.of(anchorProjYear, anchorProjMonth, 15)

    // ---- 4. Compute the constant month offset once ----
    // month_between(end, start): positive if end > start
    val offsetMonths = ChronoUnit.MONTHS.between(anchorReferenceDate, anchorProjectionDate).toInt

    // ---- 5. Apply the offset to date_origination with day preservation + EOM clamp ----
    // add_months handles day-of-month preservation with automatic end-of-month
    // we use date_origination_old as for 2025Q1 exercise, this column had the value without any modification for 2030/2040/2050
    df.withColumn(
      "date_origination",
      add_months(to_date(col("date_origination_old"), "yyyy-MM-dd"), offsetMonths).cast("string")
    )
  }

  private def computeDefaultValue(df: DataFrame): DataFrame = {
    log.info(s"---- Part 5 : Apply original before det values if not computed during treatment ----")
    val columnsToDefault = Seq(
      "rating_code",
      "id_rating_class",
      "rating_code_origin",
      "id_rating_class_ori",
      "tx_pd",
      "pd_ttc_chr",
      "pd_reg_origination"
    )

    // 1. Identify columns that are NOT part of the defaulting logic to keep them as is
    val otherCols = df.columns.filterNot(c =>
      columnsToDefault.contains(c) || c == "det_source" || c == "det_source_ori"
    ).map(col)

    // 2. Build the expressions for the defaulting columns
    val defaultCols = columnsToDefault.map { colName =>
      val beforeDetCol = s"${colName}_before_det"

      if (df.columns.contains(colName)) {
        // Case: Column exists -> use coalesce logic (if null, use before_det)
        when(col(colName).isNull, col(beforeDetCol)).otherwise(col(colName)).as(colName)
      } else {
        // Case: Column is missing -> just use the before_det column and rename it
        col(beforeDetCol).as(colName)
      }
    }

    // 3. Handle det_source and det_source_ori
    val sourceCols = Seq("det_source", "det_source_ori").map { colName =>
      if (df.columns.contains(colName)) {
        col(colName)
      } else {
        lit(null).cast("string").as(colName)
      }
    }

    df.select((otherCols ++ defaultCols ++ sourceCols): _*)
  }

  override def getDataFrames_climatetables: DataframesResult = {
    // --- Step 1: EAD Factor ---
    val femWithEadFactorDf = if (frameMeta.eadFactorStep) {
      val result = computeEadFactor()
      result.persist(StorageLevel.MEMORY_AND_DISK_SER)
      persistedDataFrames += result
      result
    } else {
      log.info("Skipping Part 1: EAD Factor step is deactivated")
      input_fem_table
    }

    val (femWithTiers, femOutputCols, tiersOutputCols, commonColumns) = computeJoinFemWithTiers(femWithEadFactorDf, input_tiers_table)

    // --- Step 2: Origination Date ---
    val dfWithOriginationDate = if (frameMeta.originationDateStep) {
      computeOriginationDate(femWithTiers)
    } else {
      log.info("Skipping Part 2: Origination Date step is deactivated")
      femWithTiers
    }

    log.info(s"Enrich portofolio with computed target_rating_scale / target_rating_scale_origination")
    val femWithTiersEnrich = enrichPortfolio(dfWithOriginationDate,
      frameMeta,
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
      input_chr_risk_level_origin_historic
    )

    // --- Step 3: Rating Deterioration ---
    val dfWithRating = if (frameMeta.ratingDeteriorationStep) {
      var result = computeRatingDeterioration(femWithTiersEnrich)
      result = result.distinct()
      result.persist(StorageLevel.MEMORY_AND_DISK_SER)
      persistedDataFrames += result
      result
    } else {
      log.info("Skipping Part 3: Rating Deterioration step is deactivated")
      femWithTiersEnrich
    }

    // --- Step 4: PD Deterioration ---
    val dfWithPd = if (frameMeta.pdDeteriorationStep) {
      val result = computePdDeterioration(dfWithRating)
      result.persist(StorageLevel.MEMORY_AND_DISK_SER)
      persistedDataFrames += result
      result
    } else {
      log.info("Skipping Part 4: PD Deterioration step is deactivated")
      dfWithRating
    }

    var dfWithDefaultValue = computeDefaultValue(dfWithPd)

    dfWithDefaultValue = dfWithDefaultValue.persist(StorageLevel.MEMORY_AND_DISK_SER)
    dfWithDefaultValue.count()

    val updatedFem = dfWithDefaultValue
      .select(femOutputCols.sorted.map(c => dfWithDefaultValue(c)): _*)

    val updatedTiers = dfWithDefaultValue.select(
      tiersOutputCols.sorted.map { colName =>
        if (commonColumns.contains(colName)) {
          // For renamed columns: select with tiers_ prefix and rename back
          col(s"tiers_$colName").as(colName)
        } else {
          // For non-renamed columns: select directly (no prefix)
          col(colName)
        }
      }: _*
    )
      .dropDuplicates("ctpr_id")

    val result = DataframesResult(
      date = frameMeta.date,
      scenario = frameMeta.scenario,
      dataTypeDf1 = "FEM",
      dataTypeDf2 = "TIERS",
      df1 = updatedFem,
      df2 = updatedTiers,
      fullDf = dfWithDefaultValue
    )

    persistedDataFrames.foreach(_.unpersist())
    persistedDataFrames.clear()

    result
  }
}

/* TODO[EXTRACTION] lines 170-171 were hidden behind the IDE sticky header and
   are reconstructed: 170 is assumed to be the closing brace of computeEadFactor
   and 171 a blank separator line.

   TODO[EXTRACTION] line 174 was partially obscured by the sticky header; read as
   .withColumnRenamed("rating_code_origin", "rating_code_origin_before_det")
   from the low-contrast scrolled-under row. Verify against source. */
