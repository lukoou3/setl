package com.lk.setl.sql.catalyst.expressions.aggregate

import com.lk.setl.sql.AnalysisException
import com.lk.setl.sql.catalyst.analysis.TypeCheckResult
import com.lk.setl.sql.catalyst.analysis.TypeCheckResult.TypeCheckSuccess
import com.lk.setl.sql.catalyst.dsl.expressions._
import com.lk.setl.sql.catalyst.expressions._
import com.lk.setl.sql.types._

/**
 * Returns the first value of `child` for a group of rows. If the first value of `child`
 * is `null`, it returns `null` (respecting nulls). Even if [[First]] is used on an already
 * sorted column, if we do partial aggregation and final aggregation (when mergeExpression
 * is used) its result will not be deterministic (unless the input table is sorted and has
 * a single partition, and we use a single reducer to do the aggregation.).
 */
case class First(child: Expression, ignoreNulls: Boolean)
  extends DeclarativeAggregate with ExpectsInputTypes {

  def this(child: Expression) = this(child, false)

  def this(child: Expression, ignoreNullsExpr: Expression) = {
    this(child, FirstLast.validateIgnoreNullExpr(ignoreNullsExpr, "first"))
  }

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // First is not a deterministic function.
  override lazy val deterministic: Boolean = false

  // Return data type.
  override def dataType: DataType = child.dataType

  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, BooleanType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else {
      TypeCheckSuccess
    }
  }

  private lazy val first = AttributeReference("first", child.dataType)()

  private lazy val valueSet = AttributeReference("valueSet", BooleanType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = first :: valueSet :: Nil

  override lazy val initialValues: Seq[Literal] = Seq(
    /* first = */ Literal.create(null, child.dataType),
    /* valueSet = */ Literal.create(false, BooleanType)
  )

  override lazy val updateExpressions: Seq[Expression] = {
    if (ignoreNulls) {
      Seq(
        /* first = */ If(valueSet || child.isNull, first, child),
        /* valueSet = */ valueSet || child.isNotNull
      )
    } else {
      Seq(
        /* first = */ If(valueSet, first, child),
        /* valueSet = */ Literal.create(true, BooleanType)
      )
    }
  }

  override lazy val mergeExpressions: Seq[Expression] = {
    // For first, we can just check if valueSet.left is set to true. If it is set
    // to true, we use first.right. If not, we use first.right (even if valueSet.right is
    // false, we are safe to do so because first.right will be null in this case).
    Seq(
      /* first = */ If(valueSet.left, first.left, first.right),
      /* valueSet = */ valueSet.left || valueSet.right
    )
  }

  override lazy val evaluateExpression: AttributeReference = first

  override def toString: String = s"$prettyName($child)${if (ignoreNulls) " ignore nulls"}"
}

object FirstLast {
  def validateIgnoreNullExpr(exp: Expression, funcName: String): Boolean = exp match {
    case Literal(b: Boolean, BooleanType) => b
    case _ => throw new AnalysisException(
      s"The second argument in $funcName should be a boolean literal.")
  }
}
