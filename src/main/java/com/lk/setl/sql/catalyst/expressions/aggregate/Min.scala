package com.lk.setl.sql.catalyst.expressions.aggregate

import com.lk.setl.sql.catalyst.analysis.TypeCheckResult
import com.lk.setl.sql.catalyst.dsl.expressions._
import com.lk.setl.sql.catalyst.expressions._
import com.lk.setl.sql.catalyst.util.TypeUtils
import com.lk.setl.sql.types._

case class Min(child: Expression) extends DeclarativeAggregate {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = child.dataType

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function min")

  private lazy val min = AttributeReference("min", child.dataType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = min :: Nil

  override lazy val initialValues: Seq[Expression] = Seq(
    /* min = */ Literal.create(null, child.dataType)
  )

  override lazy val updateExpressions: Seq[Expression] = Seq(
    /* min = */ least(min, child)
  )

  override lazy val mergeExpressions: Seq[Expression] = {
    Seq(
      /* min = */ least(min.left, min.right)
    )
  }

  override lazy val evaluateExpression: AttributeReference = min
}
