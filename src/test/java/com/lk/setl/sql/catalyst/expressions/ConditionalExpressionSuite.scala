package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.{GenericRow, Row}
import com.lk.setl.sql.catalyst.expressions.codegen.{CodegenContext, GenerateEval, GenerateSafeProjection}
import com.lk.setl.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class ConditionalExpressionSuite extends AnyFunSuite {


  test("If1") {
    val expression = If(GreaterThanOrEqual(BoundReference(0, IntegerType), Literal(10)),
      Literal(10),
      BoundReference(0, IntegerType))
    val row = new GenericRow(Array[Any](5))
    testByEvalAndCodegen(expression, row, true)
  }

  test("If2") {
    val expression = If(GreaterThanOrEqual(Add(BoundReference(0, IntegerType), BoundReference(1, IntegerType)),  Literal(10)),
      Literal(10),
      Add(BoundReference(0, IntegerType), BoundReference(1, IntegerType)))
    val row = new GenericRow(Array[Any](1, 2))
    testByEvalAndCodegen(expression, row, true)
  }

  test("exprs") {
    // doSubexpressionElimination = true时，多个表达式相同的字表达式会提取
    val ctx = new CodegenContext
    val expressions = Seq(
      GreaterThanOrEqual(Add(BoundReference(0, IntegerType), BoundReference(1, IntegerType)),  Literal(10)),
      Add(BoundReference(0, IntegerType), BoundReference(1, IntegerType))
    )
    val evals = ctx.generateExpressions(expressions, true)
    val evalSubexpr = ctx.subexprFunctionsCode
    println(evalSubexpr)
  }

  test("exprsProjection") {
    // doSubexpressionElimination = true时，多个表达式相同的字表达式会提取
    val ctx = new CodegenContext
    val expressions = Seq(
      GreaterThanOrEqual(Add(BoundReference(0, IntegerType), BoundReference(1, IntegerType)),  Literal(10)),
      Add(BoundReference(0, IntegerType), BoundReference(1, IntegerType))
    )
    val instance = GenerateSafeProjection.generate(expressions, true)
    val row = new GenericRow(Array[Any](1, 2))
    val rst1 = instance.apply(row)
    println(rst1)
    row.update(0, 10)
    row.update(1, 11)
    val rst2 = instance.apply(row)
    println(rst2)
  }

  def testByEvalAndCodegen(expression: Expression, row: Row, useSubexprElimination: Boolean = false): Unit = {
    val evalRst = expression.eval(row)
    val codeEval = GenerateEval.generate(expression, useSubexprElimination)
    val codeRst = codeEval.eval(row)
    println(s"evalRst:$evalRst\ncodeRst:$codeRst")
    assertResult(evalRst)(codeRst)
  }
}
