package com.lk.setl.sql.catalyst.expressions.aggregate

import com.lk.setl.sql.catalyst.expressions.codegen.GenerateSafeProjection
import com.lk.setl.sql.{JoinedRow, Row}
import com.lk.setl.sql.catalyst.expressions.Attribute
import com.lk.setl.sql.catalyst.expressions.BindReferences.bindReferences

/**
 * Evaluator for a [[DeclarativeAggregate]].
 */
case class DeclarativeAggregateEvaluator(function: DeclarativeAggregate, input: Seq[Attribute]) {

  // lazy val initializer = SafeProjection.create(function.initialValues)
  lazy val initializer = GenerateSafeProjection.generate(function.initialValues)//SafeProjection.create(function.initialValues)

  // lazy val updater = SafeProjection.create(function.updateExpressions,function.aggBufferAttributes ++ input)
  lazy val updater = GenerateSafeProjection.generate(
    bindReferences(function.updateExpressions, function.aggBufferAttributes ++ input))

  // lazy val merger = SafeProjection.create(function.mergeExpressions, function.aggBufferAttributes ++ function.inputAggBufferAttributes)
  lazy val merger = GenerateSafeProjection.generate(
    bindReferences(function.mergeExpressions, function.aggBufferAttributes ++ function.inputAggBufferAttributes))

  // lazy val evaluator = SafeProjection.create(function.evaluateExpression :: Nil, function.aggBufferAttributes)
  lazy val evaluator = GenerateSafeProjection.generate(bindReferences(function.evaluateExpression :: Nil, function.aggBufferAttributes))


  def initialize(): Row = initializer.apply(Row.empty).copy()

  def update(values: Row*): Row = {
    val joiner = new JoinedRow
    val buffer = values.foldLeft(initialize()) { (buffer, input) =>
      updater(joiner(buffer, input))
    }
    buffer.copy()
  }

  def merge(buffers: Row*): Row = {
    val joiner = new JoinedRow
    val buffer = buffers.foldLeft(initialize()) { (left, right) =>
      merger(joiner(left, right))
    }
    buffer.copy()
  }

  def eval(buffer: Row): Row = evaluator(buffer).copy()
}
