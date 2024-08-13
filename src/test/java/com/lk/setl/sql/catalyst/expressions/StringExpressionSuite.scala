package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.{GenericRow, Row}
import com.lk.setl.sql.catalyst.expressions.codegen.GenerateEval
import com.lk.setl.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class StringExpressionSuite extends AnyFunSuite {

  test("Substring") {
    val expression = Substring(BoundReference(0, StringType), Literal(1), Literal(4))
    val row = new GenericRow(Array[Any]("2024-01-01"))
    testByEvalAndCodegen(expression, row)
  }

  test("Like") {
    //val expression = new Like(BoundReference(0, StringType), Literal("%aa%"))
    val expression = new Like(BoundReference(0, StringType), Literal("%ab%"))
    val row = new GenericRow(Array[Any]("2024-aa-01"))
    testByEvalAndCodegen(expression, row)
  }

  test("LikeDynamic") {
    val expression = new Like(BoundReference(0, StringType), BoundReference(1, StringType))
    val row = new GenericRow(Array[Any]("2024-aa-01", "%aa%"))
    //val row = new GenericRow(Array[Any]("2024-aa-01", "%ab%"))
    testByEvalAndCodegen(expression, row)
  }

  def testByEvalAndCodegen(expression: Expression, row: Row): Unit = {
    val evalRst = expression.eval(row)
    val codeEval = GenerateEval.generate(expression)
    val codeRst = codeEval.eval(row)
    println(s"evalRst:$evalRst\ncodeRst:$codeRst")
    assertResult(evalRst)(codeRst)
  }
}
