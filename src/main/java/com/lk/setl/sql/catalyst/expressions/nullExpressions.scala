package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.Row
import com.lk.setl.sql.catalyst.analysis.TypeCheckResult
import com.lk.setl.sql.catalyst.util.TypeUtils
import com.lk.setl.sql.types.{AbstractDataType, DataType, DoubleType, FloatType, TypeCollection}


case class Coalesce(children: Seq[Expression]) extends ComplexTypeMergingExpression {

  // Coalesce is foldable if all children are foldable.
  override def foldable: Boolean = children.forall(_.foldable)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length < 1) {
      TypeCheckResult.TypeCheckFailure(
        s"input to function $prettyName requires at least one argument")
    } else {
      TypeUtils.checkForSameTypeInputExpr(children.map(_.dataType), s"function $prettyName")
    }
  }

  override def eval(input: Row): Any = {
    var result: Any = null
    val childIterator = children.iterator
    while (childIterator.hasNext && result == null) {
      result = childIterator.next().eval(input)
    }
    result
  }

}

case class IfNull(left: Expression, right: Expression, child: Expression)
  extends RuntimeReplaceable {

  def this(left: Expression, right: Expression) = {
    this(left, right, Coalesce(Seq(left, right)))
  }

  override def flatArguments: Iterator[Any] = Iterator(left, right)
  override def exprsReplaced: Seq[Expression] = Seq(left, right)
}


case class NullIf(left: Expression, right: Expression, child: Expression)
  extends RuntimeReplaceable {

  def this(left: Expression, right: Expression) = {
    this(left, right, If(EqualTo(left, right), Literal.create(null, left.dataType), left))
  }

  override def flatArguments: Iterator[Any] = Iterator(left, right)
  override def exprsReplaced: Seq[Expression] = Seq(left, right)
}

case class Nvl(left: Expression, right: Expression, child: Expression) extends RuntimeReplaceable {

  // nvl就是调的Coalesce
  def this(left: Expression, right: Expression) = {
    this(left, right, Coalesce(Seq(left, right)))
  }

  override def flatArguments: Iterator[Any] = Iterator(left, right)
  override def exprsReplaced: Seq[Expression] = Seq(left, right)
}

case class Nvl2(expr1: Expression, expr2: Expression, expr3: Expression, child: Expression)
  extends RuntimeReplaceable {

  def this(expr1: Expression, expr2: Expression, expr3: Expression) = {
    this(expr1, expr2, expr3, If(IsNotNull(expr1), expr2, expr3))
  }

  override def flatArguments: Iterator[Any] = Iterator(expr1, expr2, expr3)
  override def exprsReplaced: Seq[Expression] = Seq(expr1, expr2, expr3)
}


case class IsNaN(child: Expression) extends UnaryExpression
  with Predicate with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(DoubleType, FloatType))

  override def eval(input: Row): Any = {
    val value = child.eval(input)
    if (value == null) {
      false
    } else {
      child.dataType match {
        case DoubleType => value.asInstanceOf[Double].isNaN
        case FloatType => value.asInstanceOf[Float].isNaN
      }
    }
  }

}


case class NaNvl(left: Expression, right: Expression)
    extends BinaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = left.dataType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(DoubleType, FloatType), TypeCollection(DoubleType, FloatType))

  override def eval(input: Row): Any = {
    val value = left.eval(input)
    if (value == null) {
      null
    } else {
      left.dataType match {
        case DoubleType =>
          if (!value.asInstanceOf[Double].isNaN) value else right.eval(input)
        case FloatType =>
          if (!value.asInstanceOf[Float].isNaN) value else right.eval(input)
      }
    }
  }

}


case class IsNull(child: Expression) extends UnaryExpression with Predicate {

  override def eval(input: Row): Any = {
    child.eval(input) == null
  }

  override def sql: String = s"(${child.sql} IS NULL)"
}


case class IsNotNull(child: Expression) extends UnaryExpression with Predicate {

  override def eval(input: Row): Any = {
    child.eval(input) != null
  }

  override def sql: String = s"(${child.sql} IS NOT NULL)"
}


/**
 * A predicate that is evaluated to be true if there are at least `n` non-null and non-NaN values.
 */
case class AtLeastNNonNulls(n: Int, children: Seq[Expression]) extends Predicate {
  override def foldable: Boolean = children.forall(_.foldable)
  override def toString: String = s"AtLeastNNulls(n, ${children.mkString(",")})"

  private[this] val childrenArray = children.toArray

  override def eval(input: Row): Boolean = {
    var numNonNulls = 0
    var i = 0
    while (i < childrenArray.length && numNonNulls < n) {
      val evalC = childrenArray(i).eval(input)
      if (evalC != null) {
        childrenArray(i).dataType match {
          case DoubleType =>
            if (!evalC.asInstanceOf[Double].isNaN) numNonNulls += 1
          case FloatType =>
            if (!evalC.asInstanceOf[Float].isNaN) numNonNulls += 1
          case _ => numNonNulls += 1
        }
      }
      i += 1
    }
    numNonNulls >= n
  }

}
