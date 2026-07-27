package com.bnp.str.ageing.utility

object DateUtils {

  def addQuarters(date: String, n: Int): String = {
    val Array(year, quarter) = date.split("Q").map(_.toInt)

    val newQuarter = quarter + n
    val newYear = year + (newQuarter - 1) / 4
    val newQuarterNormalized = (newQuarter - 1) % 4 + 1

    s"${newYear}Q$newQuarterNormalized"
  }
  def getDatesBetween(dateStart: String, dateEnd: String): Seq[String] = {
    val Array(yearStart, quarterStart) = dateStart.split("Q").map(_.toInt)
    val Array(yearEnd, quarterEnd) = dateEnd.split("Q").map(_.toInt)

    val dates = for {
      year <- yearStart to yearEnd
      quarter <- if (year == yearStart) quarterStart to 4
      else if (year == yearEnd) 1 to quarterEnd
      else 1 to 4
    } yield s"${year}Q$quarter"

    dates
  }

  def quarterDiff(date1: String, date2: String): Int = {
    val year1 = date1.take(4).toInt
    val quarter1 = date1.drop(5).toInt
    val year2 = date2.take(4).toInt
    val quarter2 = date2.drop(5).toInt

    val totalQuarters1 = year1 * 4 + quarter1
    val totalQuarters2 = year2 * 4 + quarter2

    totalQuarters1 - totalQuarters2
  }
}
