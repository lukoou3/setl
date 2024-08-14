package com.lk.setl.sql

class JoinedRow extends Row {
  private[this] var row1: Row = _
  private[this] var row2: Row = _

  def this(left: Row, right: Row) = {
    this()
    row1 = left
    row2 = right
  }

  def apply(r1: Row, r2: Row): JoinedRow = {
    row1 = r1
    row2 = r2
    this
  }

  def withLeft(newLeft: Row): JoinedRow = {
    row1 = newLeft
    this
  }

  def withRight(newRight: Row): JoinedRow = {
    row2 = newRight
    this
  }

  def getLeft: Row = {
    row1
  }

  def getRight: Row = {
    row2
  }

  override def length: Int = row1.length + row2.length

  override def get(i: Int): Any =
    if (i < row1.length) row1.get(i) else row2.get(i - row1.length)

  override def isNullAt(i: Int): Boolean =
    if (i < row1.length) row1.isNullAt(i) else row2.isNullAt(i - row1.length)

  override def setNullAt(i: Int): Unit = {
    if (i < row1.length) {
      row1.setNullAt(i)
    } else {
      row2.setNullAt(i - row1.length)
    }
  }

  override def update(i: Int, value: Any): Unit = {
    if (i < row1.length) {
      row1.update(i, value)
    } else {
      row2.update(i - row1.length, value)
    }
  }

  override def copy(): Row = {
    val copy1 = row1.copy()
    val copy2 = row2.copy()
    new JoinedRow(copy1, copy2)
  }

  override def toString: String = {
    // Make sure toString never throws NullPointerException.
    if ((row1 eq null) && (row2 eq null)) {
      "[ empty row ]"
    } else if (row1 eq null) {
      row2.toString
    } else if (row2 eq null) {
      row1.toString
    } else {
      s"{${row1.toString} + ${row2.toString}}"
    }
  }
}
