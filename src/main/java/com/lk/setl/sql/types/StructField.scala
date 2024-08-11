package com.lk.setl.sql.types

import com.lk.setl.sql.catalyst.util.StringUtils.StringConcat
import com.lk.setl.sql.catalyst.util.quoteIdentifier


/**
 * A field inside a StructType.
 *
 * @param name The name of this field.
 * @param dataType The data type of this field.
 * @param nullable Indicates if values of this field can be `null` values.
 * @param metadata The metadata of this field. The metadata should be preserved during
 *                 transformation if the content of the column is not modified, e.g, in selection.
 * @since 1.3.0
 */
case class StructField(
    name: String,
    dataType: DataType) {

  /** No-arg constructor for kryo. */
  protected def this() = this(null, null)

  private[sql] def buildFormattedString(
      prefix: String,
      stringConcat: StringConcat,
      maxDepth: Int): Unit = {
    if (maxDepth > 0) {
      stringConcat.append(s"$prefix-- $name: ${dataType.typeName}\n")
      DataType.buildFormattedString(dataType, s"$prefix    |", stringConcat, maxDepth)
    }
  }

  // override the default toString to be compatible with legacy parquet files.
  override def toString: String = s"StructField($name,$dataType)"

  /**
   * Returns a string containing a schema in SQL format. For example the following value:
   * `StructField("eventId", IntegerType)` will be converted to `eventId`: INT.
   */
  private[sql] def sql = s"${quoteIdentifier(name)}: ${dataType.sql}"

  /**
   * Returns a string containing a schema in DDL format. For example, the following value:
   * `StructField("eventId", IntegerType)` will be converted to `eventId` INT.
   *
   * @since 2.4.0
   */
  def toDDL: String = s"${quoteIdentifier(name)} ${dataType.sql}"
}
