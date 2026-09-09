package com.bnp.str.climatetables.utility

import org.apache.spark.sql.types._

/**
 * Schema catalogue for the climate tables inputs read through
 * [[PrimaryUtilities.readDataFrame]].
 *
 * `PrimaryUtilities extends SchemaSelector` and `PrimaryReader extends SchemaSelector`, so every
 * schema declared here is available to both without an extra import.
 *
 * NOTE: the source file this module was extracted from was never captured (see
 * EXTRACTION_NOTES_climatetables_utility.md, "Unresolved dependencies"). The per-table schemas are
 * therefore not populated yet: [[getSchema]] falls back to an empty [[StructType]] for every table.
 * Only `readDataFrame` uses it, and no current caller goes through `readDataFrame`.
 */
trait SchemaSelector {

  /**
   * Returns the schema to apply when reading `tableName`.
   *
   * @param tableName the logical table name, matching a constant of `PrimaryConstants`
   * @return the [[StructType]] for that table, or an empty one when no schema is declared
   */
  def getSchema(tableName: String): StructType = {

    tableName.toLowerCase match {
      case _ =>
        StructType(
          Array.empty[StructField]
        )
    }
  }
}
