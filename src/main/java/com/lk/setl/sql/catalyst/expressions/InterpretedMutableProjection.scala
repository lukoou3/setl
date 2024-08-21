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

package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.catalyst.expressions.BindReferences.bindReferences
import com.lk.setl.sql.catalyst.expressions.aggregate.NoOp
import com.lk.setl.sql.{GenericRow, Row}


/**
 * A [[MutableProjection]] that is calculated by calling `eval` on each of the specified
 * expressions.
 *
 * @param expressions a sequence of expressions that determine the value of each column of the
 *                    output row.
 */
class InterpretedMutableProjection(expressions: Seq[Expression]) extends MutableProjection {
  def this(expressions: Seq[Expression], inputSchema: Seq[Attribute]) =
    this(bindReferences(expressions, inputSchema))

  private[this] val subExprEliminationEnabled = false
  private[this] lazy val runtime =  new SubExprEvaluationRuntime(100)
  private[this] val exprs = if (subExprEliminationEnabled) {
    runtime.proxyExpressions(expressions)
  } else {
    expressions
  }

  override def initialize(partitionIndex: Int): Unit = {
    expressions.foreach(_.foreach {
      case n: Nondeterministic => n.initialize(partitionIndex)
      case _ =>
    })
  }

  private[this] val validExprs = expressions.zipWithIndex.filter {
    case (NoOp, _) => false
    case _ => true
  }
  private[this] var mutableRow: Row = new GenericRow(new Array[Any](expressions.size))
  def currentValue: Row = mutableRow

  override def target(row: Row): MutableProjection = {
    mutableRow = row
    this
  }

  override def apply(input: Row): Row = {
    if (subExprEliminationEnabled) {
      runtime.setInput(input)
    }

    // 就是把flatMap后的AggregateFunction处理结果update到mutableRow
    var i = 0
    while (i < validExprs.length) {
      val (_, ordinal) = validExprs(i)
      // Store the result into buffer first, to make the projection atomic (needed by aggregation)
      mutableRow.update(ordinal, exprs(ordinal).eval(input))
      i += 1
    }
    mutableRow
  }
}

/**
 * Helper functions for creating an [[InterpretedMutableProjection]].
 */
object InterpretedMutableProjection {

  /**
   * Returns a [[MutableProjection]] for given sequence of bound Expressions.
   */
  def createProjection(exprs: Seq[Expression]): MutableProjection = {
    // We need to make sure that we do not reuse stateful expressions.
    val cleanedExpressions = exprs
    new InterpretedMutableProjection(cleanedExpressions)
  }
}
