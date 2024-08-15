package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.Row
import com.lk.setl.sql.catalyst.analysis.{FunctionRegistry, TypeCheckResult, TypeCoercion}
import com.lk.setl.sql.catalyst.expressions.codegen.Block.BlockHelper
import com.lk.setl.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, JavaCode}
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

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = dataType match {
    case IntegerType | LongType if failOnError =>
      nullSafeCodeGen(ctx, ev, eval => {
        val mathClass = classOf[Math].getName
        s"${ev.value} = $mathClass.negateExact($eval);"
      })
    case dt: NumericType => nullSafeCodeGen(ctx, ev, eval => {
      val originValue = ctx.freshName("origin")
      // codegen would fail to compile if we just write (-($c))
      // for example, we could not write --9223372036854775808L in code
      s"""
        ${CodeGenerator.javaType(dt)} $originValue = (${CodeGenerator.javaType(dt)})($eval);
        ${ev.value} = (${CodeGenerator.javaType(dt)})(-($originValue));
      """})
  }

}


case class UnaryPositive(child: Expression)
  extends UnaryExpression with ExpectsInputTypes with NullIntolerant {

  override def prettyName: String = "positive"

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def dataType: DataType = child.dataType

  protected override def nullSafeEval(input: Any): Any = input

  override def sql: String = s"(+ ${child.sql})"

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, c => c)

}

abstract class BinaryArithmetic extends BinaryOperator with NullIntolerant {

  // 代码生成函数使用
  protected val failOnError: Boolean

  override def dataType: DataType = left.dataType

  override lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess

  // 代码生成函数使用
  /** Name of the function for this expression on a [[Decimal]] type. */
  def decimalMethod: String =
    sys.error("BinaryArithmetics must override either decimalMethod or genCode")

  // 代码生成函数使用
  /** Name of the function for this expression on a [[CalendarInterval]] type. */
  def calendarIntervalMethod: String =
    sys.error("BinaryArithmetics must override either calendarIntervalMethod or genCode")

  // Name of the function for the exact version of this expression in [[Math]].
  // If the option "spark.sql.ansi.enabled" is enabled and there is corresponding
  // function in [[Math]], the exact function will be called instead of evaluation with [[symbol]].
  def exactMathMethod: Option[String] = None

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    dataType match {
      case IntegerType | LongType =>
        if(true){
          //throw new Exception("test")
        }
        nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
          val operation = if (failOnError && exactMathMethod.isDefined) {
            val mathClass = classOf[Math].getName
            s"$mathClass.${exactMathMethod.get}($eval1, $eval2)"
          } else {
            s"$eval1 $symbol $eval2"
          }
          s"""
             |${ev.value} = $operation;
         """.stripMargin
        })
      case DoubleType | FloatType =>
        // When Double/Float overflows, there can be 2 cases:
        // - precision loss: according to SQL standard, the number is truncated;
        // - returns (+/-)Infinite: same behavior also other DBs have (eg. Postgres)
        nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
          s"""
             |${ev.value} = $eval1 $symbol $eval2;
         """.stripMargin
        })
    }
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

/**
 * A function that returns the least value of all parameters, skipping null values.
 * It takes at least 2 parameters, and returns null iff all parameters are null.
 */
case class Least(children: Seq[Expression]) extends ComplexTypeMergingExpression {

  override def nullable: Boolean = children.forall(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)

  private lazy val ordering = TypeUtils.getInterpretedOrdering(dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length <= 1) {
      TypeCheckResult.TypeCheckFailure(
        s"input to function $prettyName requires at least two arguments")
    } else if (!TypeCoercion.haveSameType(inputTypesForMerging)) {
      TypeCheckResult.TypeCheckFailure(
        s"The expressions should all have the same type," +
          s" got LEAST(${children.map(_.dataType.catalogString).mkString(", ")}).")
    } else {
      TypeUtils.checkForOrderingExpr(dataType, s"function $prettyName")
    }
  }

  override def eval(input: Row): Any = {
    children.foldLeft[Any](null)((r, c) => {
      val evalc = c.eval(input)
      if (evalc != null) {
        if (r == null || ordering.lt(evalc, r)) evalc else r
      } else {
        r
      }
    })
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evalChildren = children.map(_.genCode(ctx))
    ev.isNull = JavaCode.isNullGlobal(ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, ev.isNull))
    val evals = evalChildren.map(eval =>
      s"""
         |${eval.code}
         |${ctx.reassignIfSmaller(dataType, ev, eval)}
      """.stripMargin
    )

    val resultType = CodeGenerator.javaType(dataType)
    val codes = ctx.splitExpressionsWithCurrentInputs(
      expressions = evals,
      funcName = "least",
      extraArguments = Seq(resultType -> ev.value),
      returnType = resultType,
      makeSplitFunction = body =>
        s"""
          |$body
          |return ${ev.value};
        """.stripMargin,
      foldFunctions = _.map(funcCall => s"${ev.value} = $funcCall;").mkString("\n"))
    ev.copy(code =
      code"""
         |${ev.isNull} = true;
         |$resultType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
         |$codes
      """.stripMargin)
  }
}

/**
 * A function that returns the greatest value of all parameters, skipping null values.
 * It takes at least 2 parameters, and returns null iff all parameters are null.
 */
case class Greatest(children: Seq[Expression]) extends ComplexTypeMergingExpression {

  override def nullable: Boolean = children.forall(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)

  private lazy val ordering = TypeUtils.getInterpretedOrdering(dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length <= 1) {
      TypeCheckResult.TypeCheckFailure(
        s"input to function $prettyName requires at least two arguments")
    } else if (!TypeCoercion.haveSameType(inputTypesForMerging)) {
      TypeCheckResult.TypeCheckFailure(
        s"The expressions should all have the same type," +
          s" got GREATEST(${children.map(_.dataType.catalogString).mkString(", ")}).")
    } else {
      TypeUtils.checkForOrderingExpr(dataType, s"function $prettyName")
    }
  }

  override def eval(input: Row): Any = {
    children.foldLeft[Any](null)((r, c) => {
      val evalc = c.eval(input)
      if (evalc != null) {
        if (r == null || ordering.gt(evalc, r)) evalc else r
      } else {
        r
      }
    })
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evalChildren = children.map(_.genCode(ctx))
    ev.isNull = JavaCode.isNullGlobal(ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, ev.isNull))
    val evals = evalChildren.map(eval =>
      s"""
         |${eval.code}
         |${ctx.reassignIfGreater(dataType, ev, eval)}
      """.stripMargin
    )

    val resultType = CodeGenerator.javaType(dataType)
    val codes = ctx.splitExpressionsWithCurrentInputs(
      expressions = evals,
      funcName = "greatest",
      extraArguments = Seq(resultType -> ev.value),
      returnType = resultType,
      makeSplitFunction = body =>
        s"""
           |$body
           |return ${ev.value};
        """.stripMargin,
      foldFunctions = _.map(funcCall => s"${ev.value} = $funcCall;").mkString("\n"))
    ev.copy(code =
      code"""
         |${ev.isNull} = true;
         |$resultType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
         |$codes
      """.stripMargin)
  }
}
