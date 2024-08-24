package com.lk.setl.sql.types

/**
 * The data type representing calendar intervals. The calendar interval is stored internally in
 * three components:
 *   an integer value representing the number of `months` in this interval,
 *   an integer value representing the number of `days` in this interval,
 *   a long value representing the number of `microseconds` in this interval.
 *
 * Please use the singleton `DataTypes.CalendarIntervalType` to refer the type.
 *
 * @note Calendar intervals are not comparable.
 *
 * @since 1.5.0
 */
class CalendarIntervalType private() extends DataType {

  override def defaultSize: Int = 16

  override def typeName: String = "interval"
}

/**
 * @since 1.5.0
 */
case object CalendarIntervalType extends CalendarIntervalType
