package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.Row
import com.lk.setl.sql.catalyst.analysis.FunctionRegistry
import com.lk.setl.sql.catalyst.util.TypeUtils
import com.lk.setl.sql.types._

case class UnaryMinus(
  child: Expression,
  failOnError: Boolean = false)
  extends UnaryExpression with ExpectsInputTypes with NullIntolerant {

  def this(child: Expression) = this(child, false)

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def dataType: DataType = child.dataType

  override def toString: String = s"-$child"

  private lazy val numeric = TypeUtils.getNumeric(dataType, failOnError)

  protected override def nullSafeEval(input: Any): Any = dataType match {
    case _:NumericType => numeric.negate(input)
  }

  override def sql: String = {
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("-") match {
      case "-" => s"(- ${child.sql})"
      case funcName => s"$funcName(${child.sql})"
    }
  }
}


case class UnaryPositive(child: Expression)
  extends UnaryExpression with ExpectsInputTypes with NullIntolerant {

  override def prettyName: String = "positive"

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def dataType: DataType = child.dataType

  protected override def nullSafeEval(input: Any): Any = input

  override def sql: String = s"(+ ${child.sql})"
}

abstract class BinaryArithmetic extends BinaryOperator with NullIntolerant {

  // 代码生成函数使用
  protected val failOnError: Boolean

  override def dataType: DataType = left.dataType

  override lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess

  // Name of the function for the exact version of this expression in [[Math]].
  // If the option "spark.sql.ansi.enabled" is enabled and there is corresponding
  // function in [[Math]], the exact function will be called instead of evaluation with [[symbol]].
  def exactMathMethod: Option[String] = None
}

object BinaryArithmetic {
  def unapply(e: BinaryArithmetic): Option[(Expression, Expression)] = Some((e.left, e.right))
}

case class Add(
  left: Expression,
  right: Expression,
  failOnError: Boolean = false) extends BinaryArithmetic {
  val test_value = 1

  def this(left: Expression, right: Expression) = this(left, right, false)

  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "+"

  private lazy val numeric = TypeUtils.getNumeric(dataType, failOnError)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = dataType match {
    case _ => numeric.plus(input1, input2)
  }

  override def exactMathMethod: Option[String] = Some("addExact")
}

case class Subtract(
  left: Expression,
  right: Expression,
  failOnError: Boolean = false) extends BinaryArithmetic {

  def this(left: Expression, right: Expression) = this(left, right, false)

  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "-"

  private lazy val numeric = TypeUtils.getNumeric(dataType, failOnError)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = dataType match {
    case _ => numeric.minus(input1, input2)
  }

  override def exactMathMethod: Option[String] = Some("subtractExact")
}

case class Multiply(
  left: Expression,
  right: Expression,
  failOnError: Boolean = false) extends BinaryArithmetic {

  def this(left: Expression, right: Expression) = this(left, right, false)

  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "*"

  private lazy val numeric = TypeUtils.getNumeric(dataType, failOnError)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = numeric.times(input1, input2)

  override def exactMathMethod: Option[String] = Some("multiplyExact")
}

// Common base trait for Divide and Remainder, since these two classes are almost identical
trait DivModLike extends BinaryArithmetic {
  private lazy val isZero: Any => Boolean = right.dataType match {
    case _ => x => x == 0
  }

  final override def eval(input: Row): Any = {
    // evaluate right first as we have a chance to skip left if right is 0
    val input2 = right.eval(input)
    if (input2 == null || (!failOnError && isZero(input2))) {
      null
    } else {
      val input1 = left.eval(input)
      if (input1 == null) {
        null
      } else {
        if (isZero(input2)) {
          // when we reach here, failOnError must bet true.
          throw new ArithmeticException("divide by zero")
        }
        evalOperation(input1, input2)
      }
    }
  }

  def evalOperation(left: Any, right: Any): Any

}

case class Divide(
  left: Expression,
  right: Expression,
  failOnError: Boolean = false) extends DivModLike {

  def this(left: Expression, right: Expression) = this(left, right, false)

  override def inputType: AbstractDataType = DoubleType

  override def symbol: String = "/"

  private lazy val div: (Any, Any) => Any = dataType match {
    case ft: FractionalType => ft.fractional.asInstanceOf[Fractional[Any]].div
  }

  override def evalOperation(left: Any, right: Any): Any = div(left, right)
}

case class IntegralDivide(
  left: Expression,
  right: Expression,
  failOnError: Boolean = false) extends DivModLike {

  def this(left: Expression, right: Expression) = this(left, right, false)

  override def inputType: AbstractDataType = LongType

  override def dataType: DataType = LongType

  override def symbol: String = "/"

  override def sqlOperator: String = "div"

  private lazy val div: (Any, Any) => Any = {
    val integral = left.dataType match {
      case i: IntegralType =>
        i.integral.asInstanceOf[Integral[Any]]
    }
    (x, y) => {
      val res = integral.quot(x, y)
      if (res == null) {
        null
      } else {
        integral.toLong(res)
      }
    }
  }

  override def evalOperation(left: Any, right: Any): Any = div(left, right)
}

case class Remainder(
  left: Expression,
  right: Expression,
  failOnError: Boolean = false) extends DivModLike {

  def this(left: Expression, right: Expression) = this(left, right, false)

  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "%"
  override def toString: String = {
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse(sqlOperator) match {
      case operator if operator == sqlOperator => s"($left $sqlOperator $right)"
      case funcName => s"$funcName($left, $right)"
    }
  }
  override def sql: String = {
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse(sqlOperator) match {
      case operator if operator == sqlOperator => s"(${left.sql} $sqlOperator ${right.sql})"
      case funcName => s"$funcName(${left.sql}, ${right.sql})"
    }
  }

  private lazy val mod: (Any, Any) => Any = dataType match {
    // special cases to make float/double primitive types faster
    case DoubleType =>
      (left, right) => left.asInstanceOf[Double] % right.asInstanceOf[Double]
    case FloatType =>
      (left, right) => left.asInstanceOf[Float] % right.asInstanceOf[Float]

    // catch-all cases
    case i: IntegralType =>
      val integral = i.integral.asInstanceOf[Integral[Any]]
      (left, right) => integral.rem(left, right)
  }

  override def evalOperation(left: Any, right: Any): Any = mod(left, right)
}