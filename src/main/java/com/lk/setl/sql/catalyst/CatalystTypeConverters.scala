package com.lk.setl.sql.catalyst

import com.lk.setl.sql.Row
import com.lk.setl.sql.types._
import com.lk.setl.sql.catalyst.util.DateTimeUtils

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}
import javax.annotation.Nullable

/**
 * Functions to convert Scala types to Catalyst types and vice versa.
 */
object CatalystTypeConverters {
  // The Predef.Map is scala.collection.immutable.Map.
  // Since the map values can be mutable, we explicitly import scala.collection.Map at here.
  import scala.collection.Map

  private[sql] def isPrimitive(dataType: DataType): Boolean = {
    dataType match {
      case BooleanType => true
      case IntegerType => true
      case LongType => true
      case FloatType => true
      case DoubleType => true
      case _ => false
    }
  }

  /**
   * Converts a Scala type to its Catalyst equivalent (and vice versa).
   *
   * @tparam ScalaInputType The type of Scala values that can be converted to Catalyst.
   * @tparam ScalaOutputType The type of Scala values returned when converting Catalyst to Scala.
   * @tparam CatalystType The internal Catalyst type used to represent values of this Scala type.
   */
  private abstract class CatalystTypeConverter[ScalaInputType, ScalaOutputType, CatalystType]
    extends Serializable {

    /**
     * Converts a Scala type to its Catalyst equivalent while automatically handling nulls
     * and Options.
     */
    final def toCatalyst(@Nullable maybeScalaValue: Any): CatalystType = {
      if (maybeScalaValue == null) {
        null.asInstanceOf[CatalystType]
      } else if (maybeScalaValue.isInstanceOf[Option[ScalaInputType]]) {
        val opt = maybeScalaValue.asInstanceOf[Option[ScalaInputType]]
        if (opt.isDefined) {
          toCatalystImpl(opt.get)
        } else {
          null.asInstanceOf[CatalystType]
        }
      } else {
        toCatalystImpl(maybeScalaValue.asInstanceOf[ScalaInputType])
      }
    }

    /**
     * Given a Catalyst row, convert the value at column `column` to its Scala equivalent.
     */
    final def toScala(row: Row, column: Int): ScalaOutputType = {
      if (row.isNullAt(column)) null.asInstanceOf[ScalaOutputType] else toScalaImpl(row, column)
    }

    /**
     * Convert a Catalyst value to its Scala equivalent.
     */
    def toScala(@Nullable catalystValue: CatalystType): ScalaOutputType

    /**
     * Converts a Scala value to its Catalyst equivalent.
     * @param scalaValue the Scala value, guaranteed not to be null.
     * @return the Catalyst value.
     */
    protected def toCatalystImpl(scalaValue: ScalaInputType): CatalystType

    /**
     * Given a Catalyst row, convert the value at column `column` to its Scala equivalent.
     * This method will only be called on non-null columns.
     */
    protected def toScalaImpl(row: Row, column: Int): ScalaOutputType
  }
  

  private object DateConverter extends CatalystTypeConverter[Date, Date, Any] {
    override def toCatalystImpl(scalaValue: Date): Int = DateTimeUtils.fromJavaDate(scalaValue)
    override def toScala(catalystValue: Any): Date =
      if (catalystValue == null) null else DateTimeUtils.toJavaDate(catalystValue.asInstanceOf[Int])
    override def toScalaImpl(row: Row, column: Int): Date =
      DateTimeUtils.toJavaDate(row.get(column).asInstanceOf[Int])
  }

  private object LocalDateConverter extends CatalystTypeConverter[LocalDate, LocalDate, Any] {
    override def toCatalystImpl(scalaValue: LocalDate): Int = {
      DateTimeUtils.localDateToDays(scalaValue)
    }
    override def toScala(catalystValue: Any): LocalDate = {
      if (catalystValue == null) null
      else DateTimeUtils.daysToLocalDate(catalystValue.asInstanceOf[Int])
    }
    override def toScalaImpl(row: Row, column: Int): LocalDate =
      DateTimeUtils.daysToLocalDate(row.get(column).asInstanceOf[Int])
  }

  private object TimestampConverter extends CatalystTypeConverter[Timestamp, Timestamp, Any] {
    override def toCatalystImpl(scalaValue: Timestamp): Long =
      DateTimeUtils.fromJavaTimestamp(scalaValue)
    override def toScala(catalystValue: Any): Timestamp =
      if (catalystValue == null) null
      else DateTimeUtils.toJavaTimestamp(catalystValue.asInstanceOf[Long])
    override def toScalaImpl(row: Row, column: Int): Timestamp =
      DateTimeUtils.toJavaTimestamp(row.get(column).asInstanceOf[Long])
  }

  private object InstantConverter extends CatalystTypeConverter[Instant, Instant, Any] {
    override def toCatalystImpl(scalaValue: Instant): Long =
      DateTimeUtils.instantToMicros(scalaValue)
    override def toScala(catalystValue: Any): Instant =
      if (catalystValue == null) null
      else DateTimeUtils.microsToInstant(catalystValue.asInstanceOf[Long])
    override def toScalaImpl(row: Row, column: Int): Instant =
      DateTimeUtils.microsToInstant(row.get(column).asInstanceOf[Long])
  }

  /**
   *  Converts Scala objects to Catalyst rows / types.
   *
   *  Note: This should be called before do evaluation on Row
   *        (It does not support UDT)
   *  This is used to create an RDD or test results with correct types for Catalyst.
   */
  def convertToCatalyst(a: Any): Any = a match {
    case d: Date => DateConverter.toCatalyst(d)
    case ld: LocalDate => LocalDateConverter.toCatalyst(ld)
    case t: Timestamp => TimestampConverter.toCatalyst(t)
    case i: Instant => InstantConverter.toCatalyst(i)
    case arr: Array[Byte] => arr
    case other => other
  }
}
