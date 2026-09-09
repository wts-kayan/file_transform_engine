package com.bnp.str.climatetables.utility

import org.apache.spark.sql.DataFrame

/**
 * All the information that identifies a set of DataFrames output (FEM / TIER).
 *
 * @param date        e.g. "2025"
 * @param scenario    e.g. "Adverse"
 * @param dataTypeDf1 e.g. "FEM"
 * @param dataTypeDf2 e.g. "TIERS"
 * @param df1         e.g. "dataframe for fem table"
 * @param df2         e.g. "dataframe for tier table"
 * @param fullDf      e.g. "dataframe with fem tiers table join"
 */
case class DataframesResult(date: String, scenario: String, dataTypeDf1: String, dataTypeDf2: String,
                            df1: DataFrame, df2: DataFrame, fullDf: DataFrame)
