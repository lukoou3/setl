
package com.lk.setl.sql.types

import com.lk.setl.sql.catalyst.expressions.Expression
import com.lk.setl.sql.catalyst.util.StringUtils.StringConcat

import java.util.Locale


/**
 * The base type of all Spark SQL data types.
 *
 * @since 1.3.0
 */
abstract class DataType extends AbstractDataType {
  /**
   * Enables matching against DataType for expressions:
   * {{{
   *   case Cast(child @ BinaryType(), StringType) =>
   *     ...
   * }}}
   */
  private[sql] def unapply(e: Expression): Boolean = e.dataType == this

  /**
   * The default size of a value of this data type, used internally for size estimation.
   */
  def defaultSize: Int

  /** Name of the type used in JSON serialization. */
  def typeName: String = {
    this.getClass.getSimpleName
      .stripSuffix("$").stripSuffix("Type").stripSuffix("UDT")
      .toLowerCase(Locale.ROOT)
  }

  /** Readable string representation for the type. */
  def simpleString: String = typeName

  /** String representation for the type saved in external catalogs. */
  def catalogString: String = simpleString

  /** Readable string representation for the type with truncation */
  private[sql] def simpleString(maxNumberFields: Int): String = simpleString

  def sql: String = simpleString.toUpperCase(Locale.ROOT)

  /**
   * Check if `this` and `other` are the same data type when ignoring nullability
   * (`StructField.nullable`, `ArrayType.containsNull`, and `MapType.valueContainsNull`).
   */
  private[setl] def sameType(other: DataType): Boolean =
    this == other


  /**
   * Returns true if any `DataType` of this DataType tree satisfies the given function `f`.
   */
  private[setl] def existsRecursively(f: (DataType) => Boolean): Boolean = f(this)

  override private[sql] def defaultConcreteType: DataType = this

  override private[sql] def acceptsType(other: DataType): Boolean = sameType(other)
}


/**
 * @since 1.3.0
 */
object DataType {

  private val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
  private val CHAR_TYPE = """char\(\s*(\d+)\s*\)""".r
  private val VARCHAR_TYPE = """varchar\(\s*(\d+)\s*\)""".r

  /**
   * Returns true if the two data types share the same "shape", i.e. the types
   * are the same, but the field names don't need to be the same.
   *
   * @param ignoreNullability whether to ignore nullability when comparing the types
   */
  def equalsStructurally(
    from: DataType,
    to: DataType): Boolean = {
    (from, to) match {
      case (left: ArrayType, right: ArrayType) =>
        equalsStructurally(left.elementType, right.elementType) && left.containsNull == right.containsNull

      case (StructType(fromFields), StructType(toFields)) =>
        fromFields.length == toFields.length &&
          fromFields.zip(toFields)
            .forall { case (l, r) =>
              equalsStructurally(l.dataType, r.dataType)
            }

      case (fromDataType, toDataType) => fromDataType == toDataType
    }
  }

  private val SparkGeneratedName = """col\d+""".r
  private def isSparkGeneratedName(name: String): Boolean = name match {
    case SparkGeneratedName(_*) => true
    case _ => false
  }

  protected[types] def buildFormattedString(
    dataType: DataType,
    prefix: String,
    stringConcat: StringConcat,
    maxDepth: Int): Unit = {
    dataType match {
      case struct: StructType =>
        struct.buildFormattedString(prefix, stringConcat, maxDepth - 1)
      case _ =>
    }
  }
}
