package com.bnp.str.climatetables.utility


object ColumnSelector {

  /**
   * Retrieves a sequence of column names for specified tables.
   * The columns selected are essential for respective tables to ensure that only relevant data is processed.
   *
   * NOTE: like [[SchemaSelector]], the original file was never captured (see
   * EXTRACTION_NOTES_climatetables_utility.md); the per-table column lists are still empty.
   *
   * @param tableName The name of the table for which columns need to be fetched.
   *                  The table name should match one of the constants defined in `PrimaryConstants`.
   * @return Array[String] - An array of column names corresponding to the table's requirements.
   */
  def getColumnSequence(tableName: String): Array[String] = {

    tableName.toLowerCase match {
      case _ =>
        Array(
        )

    }
  }
}
