/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lk.setl.sql.catalyst.util

import com.lk.setl.sql.catalyst.analysis.TypeCheckResult
import com.lk.setl.sql.catalyst.expressions.RowOrdering
import com.lk.setl.sql.types.{AtomicType, DataType, NullType, NumericType, StructType}

/**
 * Functions to help with checking for valid data types and value comparison of various types.
 */
object TypeUtils {
  def checkForNumericExpr(dt: DataType, caller: String): TypeCheckResult = {
    if (dt.isInstanceOf[NumericType] || dt == NullType) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(s"$caller requires numeric types, not ${dt.catalogString}")
    }
  }

  def checkForOrderingExpr(dt: DataType, caller: String): TypeCheckResult = {
    if (RowOrdering.isOrderable(dt)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(
        s"$caller does not support ordering on type ${dt.catalogString}")
    }
  }

  def checkForSameTypeInputExpr(types: Seq[DataType], caller: String): TypeCheckResult = {
    if (haveSameType(types)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(
        s"input to $caller should all be the same type, but it's " +
          types.map(_.catalogString).mkString("[", ", ", "]"))
    }
  }


  def getNumeric(t: DataType, exactNumericRequired: Boolean = false): Numeric[Any] = {
    if (exactNumericRequired) {
      t.asInstanceOf[NumericType].exactNumeric.asInstanceOf[Numeric[Any]]
    } else {
      t.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]]
    }
  }

  def getInterpretedOrdering(t: DataType): Ordering[Any] = {
    t match {
      case i: AtomicType => i.ordering.asInstanceOf[Ordering[Any]]
    }
  }

  def compareBinary(x: Array[Byte], y: Array[Byte]): Int = {
    val limit = if (x.length <= y.length) x.length else y.length
    var i = 0
    while (i < limit) {
      val res = (x(i) & 0xff) - (y(i) & 0xff)
      if (res != 0) return res
      i += 1
    }
    x.length - y.length
  }

  /**
   * Returns true if the equals method of the elements of the data type is implemented properly.
   * This also means that they can be safely used in collections relying on the equals method,
   * as sets or maps.
   */
  def typeWithProperEquals(dataType: DataType): Boolean = dataType match {
    case _: AtomicType => true
    case _ => false
  }

  def failWithIntervalType(dataType: DataType): Unit = {
    dataType match {
      case s: StructType => s.foreach(f => failWithIntervalType(f.dataType))
      case _ =>
    }
  }

  /**
   * Check whether the given types are equal ignoring nullable, containsNull and valueContainsNull.
   */
  def haveSameType(types: Seq[DataType]): Boolean = {
    if (types.size <= 1) {
      true
    } else {
      val head = types.head
      types.tail.forall(_.sameType(head))
    }
  }
}
