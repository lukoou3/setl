package com.lk.setl.sql.types

import com.lk.setl.sql.catalyst.util.TypeUtils

import scala.reflect.runtime.universe.typeTag

/**
 * The data type representing `Array[Byte]` values.
 * Please use the singleton `DataTypes.BinaryType`.
 */
class BinaryType private() extends AtomicType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "BinaryType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.

  private[sql] type InternalType = Array[Byte]

  @transient private[sql] lazy val tag = typeTag[InternalType]

  private[sql] val ordering =
    (x: Array[Byte], y: Array[Byte]) => TypeUtils.compareBinary(x, y)

  /**
   * The default size of a value of the BinaryType is 100 bytes.
   */
  override def defaultSize: Int = 100

}

/**
 * @since 1.3.0
 */
case object BinaryType extends BinaryType
