package com.lk.setl.sql.types

import scala.math.Ordering
import scala.reflect.runtime.universe.typeTag

/**
 * The timestamp type represents a time instant in microsecond precision.
 * Valid range is [0001-01-01T00:00:00.000000Z, 9999-12-31T23:59:59.999999Z] where
 * the left/right-bound is a date and time of the proleptic Gregorian
 * calendar in UTC+00:00.
 *
 * Please use the singleton `DataTypes.TimestampType` to refer the type.
 * @since 1.3.0
 */
class TimestampType private() extends AtomicType {
  /**
   * Internally, a timestamp is stored as the number of microseconds from
   * the epoch of 1970-01-01T00:00:00.000000Z (UTC+00:00)
   */
  private[sql] type InternalType = Long

  @transient private[sql] lazy val tag = typeTag[InternalType]

  private[sql] val ordering = implicitly[Ordering[InternalType]]

  /**
   * The default size of a value of the TimestampType is 8 bytes.
   */
  override def defaultSize: Int = 8
}

/**
 * The companion case object and its class is separated so the companion object also subclasses
 * the TimestampType class. Otherwise, the companion object would be of type "TimestampType$"
 * in byte code. Defined with a private constructor so the companion object is the only possible
 * instantiation.
 *
 * @since 1.3.0
 */
case object TimestampType extends TimestampType
