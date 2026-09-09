package com.bnp.str.climatetables.utility

/**
 * All the information that identifies a DataFrame representing a climate table.
 *
 * @param date                      e.g. "2030"
 * @param scenario                  e.g. "Adverse"
 * @param baseDate                  e.g. "2025Q1"
 * @param defaultOriginationDate    e.g. "2018Q1"
 * @param targetVariables           e.g. "[used_ead, factor_ead_term, ead_ifrs9]"
 * @param eadFactorStep             Flag to activate/deactivate the EAD Factor step
 * @param ratingDeteriorationStep   Flag to activate/deactivate the Rating Deterioration step
 * @param pdDeteriorationStep       Flag to activate/deactivate the PD Deterioration step
 * @param originationDateStep       Flag to activate/deactivate the Origination Date step
 * @param activateChrAtOrigination  Flag to activate/deactivate the chrAtOrigination treatment
 * @param activateCdNivRisqChrOrigin Flag to activate/deactivate the cdNivRisqChrOrigin treatment
 */
case class FrameMeta(
                      date: String,
                      scenario: String,
                      baseDate: String,
                      defaultOriginationDate: String,
                      targetVariables: Seq[String],
                      eadFactorStep: Boolean,
                      ratingDeteriorationStep: Boolean,
                      pdDeteriorationStep: Boolean,
                      originationDateStep: Boolean,
                      activateChrAtOrigination: Boolean,
                      activateCdNivRisqChrOrigin: Boolean
                    )
