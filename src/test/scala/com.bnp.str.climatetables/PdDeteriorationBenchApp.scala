package com.bnp.str.climatetables.mapping

import com.bnp.str.climatetables.mapping.PrimaryMapper._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/** Throwaway timing harness for the pd deterioration resolution at the real rule-table size. */
object PdDeteriorationBenchApp {

  private val ruleColumns = Seq(
    "__det_pd_model_short_name__", "__det_geographical_breakdown__", "__det_nace_sector_ecb__",
    "__det_epc_rating__", "__det_cd_niv_risq_chr__", "__det_rating_scale__")

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("PdDeteriorationBench").master("local[4]")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val portfolio = spark.range(0, 400000).toDF("i")
      .select(
        concat(lit("p"), col("i")).as("id_technique"),
        concat(lit("PDM"), col("i") % 50).as("pd_model"),
        concat(lit("PDM"), (col("i") + 7) % 50).as("pd_model_origination"),
        concat(lit("C"), col("i") % 200).as("cd_pays_residence_cal"),
        concat(lit("S"), col("i") % 20).as("cd_sect_wiod"),
        concat(lit("E"), col("i") % 5).as("nace_sector_ecb_epc_rating"),
        concat(lit("R"), col("i") % 10).as("cd_niv_risq_chr"),
        concat(lit("R"), (col("i") + 3) % 10).as("chr_at_origination"),
        concat(lit("SC"), col("i") % 8).as("ste_target_rating_scale"),
        concat(lit("SC"), (col("i") + 1) % 8).as("ste_target_rating_scale_origination"))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    println(s"portfolio rows: ${portfolio.count()}")

    // One wildcard pattern of the rule table: `set` gives, per rule column, the (tag, divisor,
    // modulus) that enumerates its distinct values; every other column is a wildcard.
    def pattern(rows: Long, set: (String, (String, Long, Long))*): DataFrame = {
      val keyed = set.toMap
      val cells: Seq[Column] = ruleColumns.map { c =>
        keyed.get(c) match {
          case Some((tag, div, mod)) => concat(lit(tag), (col("i") / div).cast("long") % mod).as(c)
          case None => lit("").as(c)
        }
      }
      spark.range(0, rows).toDF("i")
        .select(cells ++ Seq(lit(1.2).as("__det_value__"), lit(0.2).as("__det_cap__")): _*)
    }

    val model = "__det_pd_model_short_name__" -> ("PDM", 1L, 50L)
    val geo = "__det_geographical_breakdown__" -> ("C", 50L, 200L)
    val sector = "__det_nace_sector_ecb__" -> ("S", 10000L, 20L)
    val chr = "__det_cd_niv_risq_chr__" -> ("R", 10000L, 10L)
    val scale = "__det_rating_scale__" -> ("SC", 50L, 8L)

    val rules = Seq(
      pattern(1),                                 // catch-all
      pattern(10000, model, geo),
      pattern(100000, model, geo, chr),
      pattern(200000, model, geo, sector),
      pattern(47000, model, scale)                // 400 distinct keys, so mostly redundant
    ).reduce(_ union _)
      .withColumn("__det_rule_num__", (monotonically_increasing_id() + 1).cast("string"))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    println(s"rule rows: ${rules.count()}")

    def run(label: String): Unit = {
      val start = System.nanoTime()
      val reduced = reducePdDeteriorationRules(rules).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val masks = pdDeteriorationMasks(reduced)
      val reducedRows = reduced.count()
      val asOf = resolvePdDeterioration(portfolio, reduced, masks, PD_DET_AS_OF_KEYS, PD_DET_AS_OF)
        .select(portfolio("*"), col(s"$PD_DET_AS_OF.value").as("__det_value__"))
      val both = resolvePdDeterioration(asOf, reduced, masks, PD_DET_ORIGINATION_KEYS, PD_DET_ORIG)
        .select(asOf("*"), col(s"$PD_DET_ORIG.value").as("__det_value_origination__"))
      val matched = both.filter(col("__det_value__").isNotNull &&
        col("__det_value_origination__").isNotNull).count()
      val elapsed = (System.nanoTime() - start) / 1e9
      println(f"$label: ${masks.length} patterns, $reducedRows reduced rules, " +
        f"$matched matched rows, $elapsed%.1f s")
      reduced.unpersist()
    }

    run("cold")
    run("warm")
    spark.stop()
  }
}
