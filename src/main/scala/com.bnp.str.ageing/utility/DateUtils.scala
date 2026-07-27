package com.bnp.str.ageing.utility

object DateUtils {

  def addQuarters(date: String, n: Int): String = {
    // Extraction des années et trimestres
    val Array(year, quarter) = date.split("Q").map(_.toInt)

    // Calcul du nouveau trimestre et de la nouvelle année
    val newQuarter = quarter + n
    val newYear = year + (newQuarter - 1) / 4
    val newQuarterNormalized = (newQuarter - 1) % 4 + 1

    // Formatage de la nouvelle date
    s"${newYear}Q$newQuarterNormalized"
  }
  def getDatesBetween(dateStart: String, dateEnd: String): Seq[String] = {
    // Extraction des années et trimestres
    val Array(yearStart, quarterStart) = dateStart.split("Q").map(_.toInt)
    val Array(yearEnd, quarterEnd) = dateEnd.split("Q").map(_.toInt)

    // Génération de la liste de dates
    val dates = for {
      year <- yearStart to yearEnd
      quarter <- if (year == yearStart) quarterStart to 4
      else if (year == yearEnd) 1 to quarterEnd
      else 1 to 4
    } yield s"${year}Q$quarter"

    dates
  }

  def quarterDiff(date1: String, date2: String): Int = {
    // Extract year and quarter from input dates
    val year1 = date1.take(4).toInt
    val quarter1 = date1.drop(5).toInt
    val year2 = date2.take(4).toInt
    val quarter2 = date2.drop(5).toInt

    // Calculate total quarters for each date
    val totalQuarters1 = year1 * 4 + quarter1
    val totalQuarters2 = year2 * 4 + quarter2

    totalQuarters1 - totalQuarters2
  }
}
