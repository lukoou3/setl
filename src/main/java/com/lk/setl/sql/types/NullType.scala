package com.lk.setl.sql.types

/**
 * The data type representing `NULL` values. Please use the singleton `DataTypes.NullType`.
 *
 * @since 1.3.0
 */
class NullType private() extends DataType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "NullType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  override def defaultSize: Int = 1
}

/**
 * @since 1.3.0
 */
case object NullType extends NullType
