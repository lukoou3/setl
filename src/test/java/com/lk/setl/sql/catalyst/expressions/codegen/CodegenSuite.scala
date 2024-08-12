package com.lk.setl.sql.catalyst.expressions.codegen

import com.lk.setl.sql.catalyst.expressions._
import com.lk.setl.sql.types.{BooleanType, IntegerType}
import org.scalatest.funsuite.AnyFunSuite

class CodegenSuite extends AnyFunSuite {

  test("genCode") {
    val ctx = new CodegenContext
    val expr = BoundReference(0, IntegerType)
    val exprCode:ExprCode = expr.genCode(ctx)
    val code: Block = exprCode.code
    val isNull: ExprValue = exprCode.isNull
    val value: ExprValue = exprCode.value
    println("code:" + code)
    println("isNull:" + isNull)
    println("value:" + value)
  }

  /**
   * ExprCode就代表一段代码逻辑，改代码返回一个值
   *   通过isNull变量引用结果是否为null，对应的java类型固定是boolean
   *   通过value变量引用结果值，对应的java类型是expr返回值的类型
   */
  test("genCode_IsNull") {
    val ctx = new CodegenContext
    val expr = IsNull(BoundReference(0, IntegerType))
    val exprCode: ExprCode = expr.genCode(ctx)
    val code: Block = exprCode.code
    val isNull: ExprValue = exprCode.isNull
    val value: ExprValue = exprCode.value
    println("code:" + code)
    println("isNull:" + isNull)
    println("value:" + value)
  }

  test("genCode_IsNotNull") {
    val ctx = new CodegenContext
    val expr = IsNotNull(BoundReference(0, IntegerType))
    val exprCode: ExprCode = expr.genCode(ctx)
    val code: Block = exprCode.code
    val isNull: ExprValue = exprCode.isNull
    val value: ExprValue = exprCode.value
    println("code:" + code)
    println("isNull:" + isNull)
    println("value:" + value)
  }

  test("genCode_Not") {
    val ctx = new CodegenContext
    val expr = Not(BoundReference(0, BooleanType))
    val exprCode: ExprCode = expr.genCode(ctx)
    val code: Block = exprCode.code
    val isNull: ExprValue = exprCode.isNull
    val value: ExprValue = exprCode.value
    println("code:" + code)
    println("isNull:" + isNull)
    println("value:" + value)
  }

  test("genCode_Add") {
    val ctx = new CodegenContext
    val expr = Add(BoundReference(0, IntegerType), BoundReference(1, IntegerType))
    val exprCode: ExprCode = expr.genCode(ctx)
    val code: Block = exprCode.code
    val isNull: ExprValue = exprCode.isNull
    val value: ExprValue = exprCode.value
    val fmtCode = CodeFormatter.format(CodeFormatter.stripOverlappingComments(new CodeAndComment(code.toString, ctx.getPlaceHolderToComments())))
    println("code:\n" + fmtCode)
    println("isNull:" + isNull)
    println("value:" + value)
  }

  /**
   * 当代码较长时会提取函数，默认代码长度大于1024
   */
  test("genCode_SplitCodeSize") {
    val ctx = new CodegenContext
    val expr = Add(Add(Add(BoundReference(0, IntegerType), Literal(10, IntegerType)),
      Add(BoundReference(1, IntegerType), Literal(20, IntegerType))),
      Add(BoundReference(2, IntegerType), BoundReference(3, IntegerType)))
    val exprCode: ExprCode = expr.genCode(ctx)
    val code: Block = exprCode.code
    val isNull: ExprValue = exprCode.isNull
    val value: ExprValue = exprCode.value
    val fmtCode = CodeFormatter.format(CodeFormatter.stripOverlappingComments(new CodeAndComment(code.toString + "\n" + ctx.declareAddedFunctions(), ctx.getPlaceHolderToComments())))
    println("code:\n" + fmtCode)
    println("isNull:" + isNull)
    println("value:" + value)
  }

  /**
   * 提取函数生成的globalIsNull全局变量在SpecificPredicate类中声明，调用ctx.declareMutableStates()获取声明代码
   * 声明的变量存在inlinedMutableStates中
   */
  test("genCode_SplitCodeSize_InPredicate") {
    val expr = And(And(GreaterThan(BoundReference(0, IntegerType), Literal(10, IntegerType)),
      LessThanOrEqual(BoundReference(1, IntegerType), Literal(20, IntegerType))),
      LessThanOrEqual(BoundReference(0, IntegerType), BoundReference(1, IntegerType)))
    GeneratePredicate.generate(expr)
  }
}
