package com.lk.setl.sql.types

import scala.math.Ordering
import scala.reflect.runtime.universe.typeTag

/**
 * The date type represents a valid date in the proleptic Gregorian calendar.
 * Valid range is [0001-01-01, 9999-12-31].
 *
 * Please use the singleton `DataTypes.DateType` to refer the type.
 * @since 1.3.0
 */
class DateType private() extends AtomicType {
  /**
   * Internally, a date is stored as a simple incrementing count of days
   * where day 0 is 1970-01-01. Negative numbers represent earlier days.
   */
  private[sql] type InternalType = Int

  @transient private[sql] lazy val tag = typeTag[InternalType]

  private[sql] val ordering = implicitly[Ordering[InternalType]]

  /**
   * The default size of a value of the DateType is 4 bytes.
   */
  override def defaultSize: Int = 4
}

/**
 * The companion case object and the DateType class is separated so the companion object
 * also subclasses the class. Otherwise, the companion object would be of type "DateType$"
 * in byte code. The DateType class is defined with a private constructor so its companion
 * object is the only possible instantiation.
 *
 * @since 1.3.0
 */
case object DateType extends DateType
