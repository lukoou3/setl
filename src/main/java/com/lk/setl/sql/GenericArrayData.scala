package com.lk.setl.sql

class GenericArrayData(val array: Array[Any]) extends ArrayData {

  override def length: Int = array.length

  override def copy(): ArrayData = {
    val newValues = new Array[Any](array.length)
    var i = 0
    while (i < array.length) {
      // 暂时浅拷贝
      newValues(i) = array(i)
      i += 1
    }
    new GenericArrayData(newValues)
  }

  override def get(ordinal: Int): Any = array(ordinal)

  override def setNullAt(i: Int): Unit = array(i) = null

  override def update(i: Int, value: Any): Unit = array(i) = value
}
