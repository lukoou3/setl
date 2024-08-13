package com.lk.setl.sql.catalyst.expressions.aggregate

import com.lk.setl.sql.catalyst.analysis.{FunctionRegistry, TypeCheckResult}
import com.lk.setl.sql.catalyst.dsl.expressions._
import com.lk.setl.sql.catalyst.expressions._
import com.lk.setl.sql.catalyst.util.TypeUtils
import com.lk.setl.sql.types._

case class Average(child: Expression) extends DeclarativeAggregate with ImplicitCastInputTypes {

  override def prettyName: String = getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("avg")

  override def children: Seq[Expression] = child :: Nil

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function average")

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  private lazy val resultType = child.dataType match {
    case _ => DoubleType
  }

  private lazy val sumDataType = child.dataType match {
    case _ => DoubleType
  }

  private lazy val sum = AttributeReference("sum", sumDataType)()
  private lazy val count = AttributeReference("count", LongType)()

  override lazy val aggBufferAttributes = sum :: count :: Nil

  override lazy val initialValues = Seq(
    /* sum = */ Literal.default(sumDataType),
    /* count = */ Literal(0L)
  )

  override lazy val mergeExpressions = Seq(
    /* sum = */ sum.left + sum.right,
    /* count = */ count.left + count.right
  )

  // If all input are nulls, count will be 0 and we will get null after the division.
  // We can't directly use `/` as it throws an exception under ansi mode.
  override lazy val evaluateExpression = child.dataType match {
    case _ =>
      Divide(sum.cast(resultType), count.cast(resultType), failOnError = false)
  }

  override lazy val updateExpressions: Seq[Expression] = Seq(
    /* sum = */
    Add(
      sum,
      coalesce(child.cast(sumDataType), Literal.default(sumDataType))),
    /* count = */ If(child.isNull, count, count + 1L)
  )
}
