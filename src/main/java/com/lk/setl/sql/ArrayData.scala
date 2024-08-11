package com.lk.setl.sql

abstract class ArrayData {

  def numElements(): Int = length

  def size: Int = length

  def length: Int

  def copy(): ArrayData

  def array: Array[Any]

  def get(ordinal: Int): Any

  def isNullAt(i: Int): Boolean = get(i) == null

  def setNullAt(i: Int): Unit

  def update(i: Int, value: Any): Unit
}
