package com.lk.setl.sql

import com.lk.setl.sql.types.StructType

abstract class Row extends Serializable {
  def numFields: Int = length

  /** Number of elements in the Row. */
  def size: Int = length

  /** Number of elements in the Row. */
  def length: Int

  /**
   * Schema for the row.
   */
  def schema: StructType = null

  def apply(i: Int): Any = get(i)

  def get(i: Int): Any

  def isNullAt(i: Int): Boolean

  def setNullAt(i: Int): Unit

  def update(i: Int, value: Any): Unit

  def copy(): Row

  override def toString: String = this.mkString("[", ",", "]")

  /** Displays all elements of this sequence in a string (without a separator). */
  def mkString: String = mkString("")

  /** Displays all elements of this sequence in a string using a separator string. */
  def mkString(sep: String): String = mkString("", sep, "")

  /**
   * Displays all elements of this traversable or iterator in a string using
   * start, end, and separator strings.
   */
  def mkString(start: String, sep: String, end: String): String = {
    val n = length
    val builder = new StringBuilder
    builder.append(start)
    if (n > 0) {
      builder.append(get(0))
      var i = 1
      while (i < n) {
        builder.append(sep)
        builder.append(get(i))
        i += 1
      }
    }
    builder.append(end)
    builder.toString()
  }
}

object Row {
  /**
   * This method can be used to construct a [[Row]] with the given values.
   */
  def apply(values: Any*): Row = new GenericRow(values.toArray)

  /**
   * This method can be used to construct a [[Row]] from a [[Seq]] of values.
   */
  def fromSeq(values: Seq[Any]): Row = new GenericRow(values.toArray)

  /** Returns an empty [[Row]]. */
  val empty = apply()

  /**
   * Copies the given value if it'sstruct/array/map type.
   */
  def copyValue(value: Any): Any = value match {
    case v: Row => v.copy()
    case v: ArrayData => v.copy()
    case _ => value
  }
}