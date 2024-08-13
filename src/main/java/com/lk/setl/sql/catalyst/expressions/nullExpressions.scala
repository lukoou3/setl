package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.Row
import com.lk.setl.sql.catalyst.analysis.TypeCheckResult
import com.lk.setl.sql.catalyst.expressions.codegen.Block.BlockHelper
import com.lk.setl.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, EmptyBlock, ExprCode, FalseLiteral, JavaCode, TrueLiteral}
import com.lk.setl.sql.catalyst.util.TypeUtils
import com.lk.setl.sql.types.{AbstractDataType, BooleanType, DataType, DoubleType, FloatType, TypeCollection}


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

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ev.isNull = JavaCode.isNullGlobal(ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, ev.isNull))

    // all the evals are meant to be in a do { ... } while (false); loop
    val evals = children.map { e =>
      val eval = e.genCode(ctx)
      s"""
         |${eval.code}
         |if (!${eval.isNull}) {
         |  ${ev.isNull} = false;
         |  ${ev.value} = ${eval.value};
         |  continue;
         |}
       """.stripMargin
    }

    val resultType = CodeGenerator.javaType(dataType)
    val codes = ctx.splitExpressionsWithCurrentInputs(
      expressions = evals,
      funcName = "coalesce",
      returnType = resultType,
      makeSplitFunction = func =>
        s"""
           |$resultType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
           |do {
           |  $func
           |} while (false);
           |return ${ev.value};
         """.stripMargin,
      foldFunctions = _.map { funcCall =>
        s"""
           |${ev.value} = $funcCall;
           |if (!${ev.isNull}) {
           |  continue;
           |}
         """.stripMargin
      }.mkString)


    ev.copy(code =
      code"""
            |${ev.isNull} = true;
            |$resultType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
            |do {
            |  $codes
            |} while (false);
       """.stripMargin)
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

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    child.dataType match {
      case DoubleType | FloatType =>
        ev.copy(code = code"""
          ${eval.code}
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
          ${ev.value} = !${eval.isNull} && Double.isNaN(${eval.value});""", isNull = FalseLiteral)
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

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val leftGen = left.genCode(ctx)
    val rightGen = right.genCode(ctx)
    left.dataType match {
      case DoubleType | FloatType =>
        ev.copy(code = code"""
          ${leftGen.code}
          boolean ${ev.isNull} = false;
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
          if (${leftGen.isNull}) {
            ${ev.isNull} = true;
          } else {
            if (!Double.isNaN(${leftGen.value})) {
              ${ev.value} = ${leftGen.value};
            } else {
              ${rightGen.code}
              if (${rightGen.isNull}) {
                ${ev.isNull} = true;
              } else {
                ${ev.value} = ${rightGen.value};
              }
            }
          }""")
    }
  }

}


case class IsNull(child: Expression) extends UnaryExpression with Predicate {

  override def nullable: Boolean = false

  override def eval(input: Row): Any = {
    child.eval(input) == null
  }

  override def sql: String = s"(${child.sql} IS NULL)"

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    ExprCode(code = eval.code, isNull = FalseLiteral, value = eval.isNull)
  }
}


case class IsNotNull(child: Expression) extends UnaryExpression with Predicate {

  override def nullable: Boolean = false

  override def eval(input: Row): Any = {
    child.eval(input) != null
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    val (value, newCode) = eval.isNull match {
      case TrueLiteral => (FalseLiteral, EmptyBlock)
      case FalseLiteral => (TrueLiteral, EmptyBlock)
      case v =>
        val value = ctx.freshName("value")
        (JavaCode.variable(value, BooleanType), code"boolean $value = !$v;")
    }
    ExprCode(code = eval.code + newCode, isNull = FalseLiteral, value = value)
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

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val nonnull = ctx.freshName("nonnull")
    // all evals are meant to be inside a do { ... } while (false); loop
    val evals = children.map { e =>
      val eval = e.genCode(ctx)
      e.dataType match {
        case DoubleType | FloatType =>
          s"""
             |if ($nonnull < $n) {
             |  ${eval.code}
             |  if (!${eval.isNull} && !Double.isNaN(${eval.value})) {
             |    $nonnull += 1;
             |  }
             |} else {
             |  continue;
             |}
           """.stripMargin
        case _ =>
          s"""
             |if ($nonnull < $n) {
             |  ${eval.code}
             |  if (!${eval.isNull}) {
             |    $nonnull += 1;
             |  }
             |} else {
             |  continue;
             |}
           """.stripMargin
      }
    }

    val codes = ctx.splitExpressionsWithCurrentInputs(
      expressions = evals,
      funcName = "atLeastNNonNulls",
      extraArguments = (CodeGenerator.JAVA_INT, nonnull) :: Nil,
      returnType = CodeGenerator.JAVA_INT,
      makeSplitFunction = body =>
        s"""
           |do {
           |  $body
           |} while (false);
           |return $nonnull;
         """.stripMargin,
      foldFunctions = _.map { funcCall =>
        s"""
           |$nonnull = $funcCall;
           |if ($nonnull >= $n) {
           |  continue;
           |}
         """.stripMargin
      }.mkString)

    ev.copy(code =
      code"""
            |${CodeGenerator.JAVA_INT} $nonnull = 0;
            |do {
            |  $codes
            |} while (false);
            |${CodeGenerator.JAVA_BOOLEAN} ${ev.value} = $nonnull >= $n;
       """.stripMargin, isNull = FalseLiteral)
  }
}
