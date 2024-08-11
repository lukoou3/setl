package com.lk.setl.sql

class GenericRow(val values: Array[Any]) extends Row {

  override def length: Int = values.length

  override def get(i: Int): Any = values(i)

  override def update(i: Int, value: Any): Unit = values(i) = value

  override def isNullAt(i: Int): Boolean = get(i) == null

  override def setNullAt(i: Int): Unit = values(i) = null
}
