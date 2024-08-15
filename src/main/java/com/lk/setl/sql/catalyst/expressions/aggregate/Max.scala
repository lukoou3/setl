package com.lk.setl.sql.catalyst.expressions.aggregate

import com.lk.setl.sql.catalyst.analysis.TypeCheckResult
import com.lk.setl.sql.catalyst.dsl.expressions._
import com.lk.setl.sql.catalyst.expressions._
import com.lk.setl.sql.catalyst.util.TypeUtils
import com.lk.setl.sql.types._

case class Max(child: Expression) extends DeclarativeAggregate {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = child.dataType

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function max")

  private lazy val max = AttributeReference("max", child.dataType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = max :: Nil

  override lazy val initialValues: Seq[Literal] = Seq(
    /* max = */ Literal.create(null, child.dataType)
  )

  override lazy val updateExpressions: Seq[Expression] = Seq(
    /* max = */ greatest(max, child)
  )

  override lazy val mergeExpressions: Seq[Expression] = {
    Seq(
      /* max = */ greatest(max.left, max.right)
    )
  }

  override lazy val evaluateExpression: AttributeReference = max
}
