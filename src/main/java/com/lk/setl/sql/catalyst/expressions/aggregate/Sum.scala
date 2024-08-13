package com.lk.setl.sql.catalyst.expressions.aggregate

import com.lk.setl.sql.catalyst.analysis.TypeCheckResult
import com.lk.setl.sql.catalyst.dsl.expressions._
import com.lk.setl.sql.catalyst.expressions._
import com.lk.setl.sql.catalyst.util.TypeUtils
import com.lk.setl.sql.types._

case class Sum(child: Expression) extends DeclarativeAggregate with ImplicitCastInputTypes {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function sum")

  private lazy val resultType = child.dataType match {
    case _: IntegralType => LongType
    case _ => DoubleType
  }

  private lazy val sum = AttributeReference("sum", resultType)()

  private lazy val zero = Literal.default(resultType)

  override lazy val aggBufferAttributes = resultType match {
    case _ => sum :: Nil
  }

  override lazy val initialValues: Seq[Expression] = resultType match {
    case _ => Seq(Literal(null, resultType))
  }

  override lazy val updateExpressions: Seq[Expression] = {
    resultType match {
      case _ =>
        // For non-decimal type, the initial value of `sum` is null, which indicates no value.
        // We need `coalesce(sum, zero)` to start summing values. And we need an outer `coalesce`
        // in case the input is nullable. The `sum` can only be null if there is no value, as
        // non-decimal type can produce overflowed value under non-ansi mode.
        if (child.nullable) {
          Seq(coalesce(coalesce(sum, zero) + child.cast(resultType), sum))
        } else {
          Seq(coalesce(sum, zero) + child.cast(resultType))
        }
    }
  }

  /**
   * For decimal type:
   * If isEmpty is false and if sum is null, then it means we have had an overflow.
   *
   * update of the sum is as follows:
   * Check if either portion of the left.sum or right.sum has overflowed
   * If it has, then the sum value will remain null.
   * If it did not have overflow, then add the sum.left and sum.right
   *
   * isEmpty:  Set to false if either one of the left or right is set to false. This
   * means we have seen atleast a value that was not null.
   */
  override lazy val mergeExpressions: Seq[Expression] = {
    resultType match {
      case _ => Seq(coalesce(coalesce(sum.left, zero) + sum.right, sum.left))
    }
  }

  /**
   * If the isEmpty is true, then it means there were no values to begin with or all the values
   * were null, so the result will be null.
   * If the isEmpty is false, then if sum is null that means an overflow has happened.
   * So now, if ansi is enabled, then throw exception, if not then return null.
   * If sum is not null, then return the sum.
   */
  override lazy val evaluateExpression: Expression = resultType match {
    case _ => sum
  }
}
