package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.Row
import com.lk.setl.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import com.lk.setl.sql.catalyst.expressions.codegen.Block.BlockHelper
import com.lk.setl.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, JavaCode}
import com.lk.setl.sql.types.{BooleanType, DataType}

case class If(predicate: Expression, trueValue: Expression, falseValue: Expression)
  extends ComplexTypeMergingExpression {

  @transient
  override lazy val inputTypesForMerging: Seq[DataType] = {
    Seq(trueValue.dataType, falseValue.dataType)
  }

  override def children: Seq[Expression] = predicate :: trueValue :: falseValue :: Nil

  override def checkInputDataTypes(): TypeCheckResult = {
    // 检查输入类型, predicate输出类型必须是BooleanType, 输入类型必须相同
    if (predicate.dataType != BooleanType) {
      TypeCheckResult.TypeCheckFailure(
        "type of predicate expression in If should be boolean, " +
          s"not ${predicate.dataType.catalogString}")
    } else if (!TypeCoercion.haveSameType(inputTypesForMerging)) {
      TypeCheckResult.TypeCheckFailure(s"differing types in '$sql' " +
        s"(${trueValue.dataType.catalogString} and ${falseValue.dataType.catalogString}).")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def eval(input: Row): Any = {
    // eval方法中就是直接写代码计算的
    if (java.lang.Boolean.TRUE.equals(predicate.eval(input))) {
      trueValue.eval(input)
    } else {
      falseValue.eval(input)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val condEval = predicate.genCode(ctx)
    val trueEval = trueValue.genCode(ctx)
    val falseEval = falseValue.genCode(ctx)

    // 这代码是怎么生成的?
    val code =
      code"""
            |${condEval.code}
            |boolean ${ev.isNull} = false;
            |${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
            |if (!${condEval.isNull} && ${condEval.value}) {
            |  ${trueEval.code}
            |  ${ev.isNull} = ${trueEval.isNull};
            |  ${ev.value} = ${trueEval.value};
            |} else {
            |  ${falseEval.code}
            |  ${ev.isNull} = ${falseEval.isNull};
            |  ${ev.value} = ${falseEval.value};
            |}
       """.stripMargin
    ev.copy(code = code)
  }

  override def toString: String = s"if ($predicate) $trueValue else $falseValue"

  override def sql: String = s"(IF(${predicate.sql}, ${trueValue.sql}, ${falseValue.sql}))"
}

/**
 * Case statements of the form "CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END".
 * When a = true, returns b; when c = true, returns d; else returns e.
 *
 * @param branches seq of (branch condition, branch value)
 * @param elseValue optional value for the else branch
 */
case class CaseWhen(
  branches: Seq[(Expression, Expression)],
  elseValue: Option[Expression] = None)
  extends ComplexTypeMergingExpression with Serializable {

  override def children: Seq[Expression] = branches.flatMap(b => b._1 :: b._2 :: Nil) ++ elseValue

  // both then and else expressions should be considered.
  @transient
  override lazy val inputTypesForMerging: Seq[DataType] = {
    branches.map(_._2.dataType) ++ elseValue.map(_.dataType)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (TypeCoercion.haveSameType(inputTypesForMerging)) {
      // Make sure all branch conditions are boolean types.
      if (branches.forall(_._1.dataType == BooleanType)) {
        TypeCheckResult.TypeCheckSuccess
      } else {
        val index = branches.indexWhere(_._1.dataType != BooleanType)
        TypeCheckResult.TypeCheckFailure(
          s"WHEN expressions in CaseWhen should all be boolean type, " +
            s"but the ${index + 1}th when expression's type is ${branches(index)._1}")
      }
    } else {
      val branchesStr = branches.map(_._2.dataType).map(dt => s"WHEN ... THEN ${dt.catalogString}")
        .mkString(" ")
      val elseStr = elseValue.map(expr => s" ELSE ${expr.dataType.catalogString}").getOrElse("")
      TypeCheckResult.TypeCheckFailure(
        "THEN and ELSE expressions should all be same type or coercible to a common type," +
          s" got CASE $branchesStr$elseStr END")
    }
  }

  override def eval(input: Row): Any = {
    var i = 0
    val size = branches.size
    while (i < size) {
      if (java.lang.Boolean.TRUE.equals(branches(i)._1.eval(input))) {
        return branches(i)._2.eval(input)
      }
      i += 1
    }
    if (elseValue.isDefined) {
      return elseValue.get.eval(input)
    } else {
      return null
    }
  }

  override def toString: String = {
    val cases = branches.map { case (c, v) => s" WHEN $c THEN $v" }.mkString
    val elseCase = elseValue.map(" ELSE " + _).getOrElse("")
    "CASE" + cases + elseCase + " END"
  }

  override def sql: String = {
    val cases = branches.map { case (c, v) => s" WHEN ${c.sql} THEN ${v.sql}" }.mkString
    val elseCase = elseValue.map(" ELSE " + _.sql).getOrElse("")
    "CASE" + cases + elseCase + " END"
  }

  private def multiBranchesCodegen(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // This variable holds the state of the result:
    // -1 means the condition is not met yet and the result is unknown.
    val NOT_MATCHED = -1
    // 0 means the condition is met and result is not null.
    val HAS_NONNULL = 0
    // 1 means the condition is met and result is null.
    val HAS_NULL = 1
    // It is initialized to `NOT_MATCHED`, and if it's set to `HAS_NULL` or `HAS_NONNULL`,
    // We won't go on anymore on the computation.
    val resultState = ctx.freshName("caseWhenResultState")
    ev.value = JavaCode.global(
      ctx.addMutableState(CodeGenerator.javaType(dataType), ev.value),
      dataType)

    // these blocks are meant to be inside a
    // do {
    //   ...
    // } while (false);
    // loop
    val cases = branches.map { case (condExpr, valueExpr) =>
      val cond = condExpr.genCode(ctx)
      val res = valueExpr.genCode(ctx)
      s"""
         |${cond.code}
         |if (!${cond.isNull} && ${cond.value}) {
         |  ${res.code}
         |  $resultState = (byte)(${res.isNull} ? $HAS_NULL : $HAS_NONNULL);
         |  ${ev.value} = ${res.value};
         |  continue;
         |}
       """.stripMargin
    }

    val elseCode = elseValue.map { elseExpr =>
      val res = elseExpr.genCode(ctx)
      s"""
         |${res.code}
         |$resultState = (byte)(${res.isNull} ? $HAS_NULL : $HAS_NONNULL);
         |${ev.value} = ${res.value};
       """.stripMargin
    }

    val allConditions = cases ++ elseCode

    // This generates code like:
    //   caseWhenResultState = caseWhen_1(i);
    //   if(caseWhenResultState != -1) {
    //     continue;
    //   }
    //   caseWhenResultState = caseWhen_2(i);
    //   if(caseWhenResultState != -1) {
    //     continue;
    //   }
    //   ...
    // and the declared methods are:
    //   private byte caseWhen_1234() {
    //     byte caseWhenResultState = -1;
    //     do {
    //       // here the evaluation of the conditions
    //     } while (false);
    //     return caseWhenResultState;
    //   }
    val codes = ctx.splitExpressionsWithCurrentInputs(
      expressions = allConditions,
      funcName = "caseWhen",
      returnType = CodeGenerator.JAVA_BYTE,
      makeSplitFunction = func =>
        s"""
           |${CodeGenerator.JAVA_BYTE} $resultState = $NOT_MATCHED;
           |do {
           |  $func
           |} while (false);
           |return $resultState;
         """.stripMargin,
      foldFunctions = _.map { funcCall =>
        s"""
           |$resultState = $funcCall;
           |if ($resultState != $NOT_MATCHED) {
           |  continue;
           |}
         """.stripMargin
      }.mkString)

    ev.copy(code =
      code"""
            |${CodeGenerator.JAVA_BYTE} $resultState = $NOT_MATCHED;
            |do {
            |  $codes
            |} while (false);
            |// TRUE if any condition is met and the result is null, or no any condition is met.
            |final boolean ${ev.isNull} = ($resultState != $HAS_NONNULL);
       """.stripMargin)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (branches.length == 1) {
      // If we have only single branch we can use If expression and its codeGen
      If(
        branches(0)._1,
        branches(0)._2,
        elseValue.getOrElse(Literal.create(null, branches(0)._2.dataType))).doGenCode(ctx, ev)
    } else {
      multiBranchesCodegen(ctx, ev)
    }
  }

}

/** Factory methods for CaseWhen. */
object CaseWhen {
  def apply(branches: Seq[(Expression, Expression)], elseValue: Expression): CaseWhen = {
    CaseWhen(branches, Option(elseValue))
  }

  /**
   * A factory method to facilitate the creation of this expression when used in parsers.
   *
   * @param branches Expressions at even position are the branch conditions, and expressions at odd
   *                 position are branch values.
   */
  def createFromParser(branches: Seq[Expression]): CaseWhen = {
    val cases = branches.grouped(2).flatMap {
      case cond :: value :: Nil => Some((cond, value))
      case value :: Nil => None
    }.toArray.toSeq  // force materialization to make the seq serializable
    val elseValue = if (branches.size % 2 != 0) Some(branches.last) else None
    CaseWhen(cases, elseValue)
  }
}

/**
 * Case statements of the form "CASE a WHEN b THEN c [WHEN d THEN e]* [ELSE f] END".
 * When a = b, returns c; when a = d, returns e; else returns f.
 */
object CaseKeyWhen {
  def apply(key: Expression, branches: Seq[Expression]): CaseWhen = {
    val cases = branches.grouped(2).flatMap {
      case Seq(cond, value) => Some((EqualTo(key, cond), value))
      case Seq(value) => None
    }.toArray.toSeq  // force materialization to make the seq serializable
    val elseValue = if (branches.size % 2 != 0) Some(branches.last) else None
    CaseWhen(cases, elseValue)
  }
}