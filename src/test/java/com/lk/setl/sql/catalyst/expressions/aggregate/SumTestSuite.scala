package com.lk.setl.sql.catalyst.expressions.aggregate

import com.lk.setl.sql.AnalysisException
import com.lk.setl.sql.Row
import com.lk.setl.sql.catalyst.expressions.{AttributeReference, CodegenObjectFactoryMode, Literal, SafeProjection}
import com.lk.setl.sql.types.IntegerType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class SumTestSuite extends AnyFunSuite with BeforeAndAfterAll {
  val input = AttributeReference("input", IntegerType)()
  val evaluator = DeclarativeAggregateEvaluator(Sum(input), Seq(input))

  override protected def beforeAll(): Unit = {
    System.setProperty("spark.testing", "true")
    SafeProjection.fallbackMode = CodegenObjectFactoryMode.CODEGEN_ONLY
    //SafeProjection.fallbackMode = CodegenObjectFactoryMode.NO_CODEGEN
  }

  test("empty buffer") {
    println(evaluator.initialize())
    assert(evaluator.initialize() === Row(null))
  }

  test("update") {
    val result = evaluator.update(
      Row(1),
      Row(9),
      Row(null),
      Row(-2))
    println(result)
    assert(result === Row(8))
  }

  test("merge") {
    // Empty merge
    val p0 = evaluator.initialize()
    assert(evaluator.merge(p0) === Row(null))

    // Single merge
    val p1 = evaluator.update(Row(1), Row(9))
    assert(evaluator.merge(p1) === p1)

    // Multiple merges.
    val p2 = evaluator.update(Row(2), Row(10))
    assert(evaluator.merge(p1, p2) === Row(22))

    // Empty partitions (p0 is empty)
    assert(evaluator.merge(p1, p0, p2) === Row(22))
    assert(evaluator.merge(p2, p1, p0) === Row(22))
  }

  test("eval") {
    // Null Eval
    assert(evaluator.eval(Row(null)) === Row(null))

    // Empty Eval
    val p0 = evaluator.initialize()
    assert(evaluator.eval(p0) === Row(null))

    // Update - Eval
    val p1 = evaluator.update(Row(1), Row(9))
    assert(evaluator.eval(p1) === Row(10))

    // Update - Merge - Eval
    val p2 = evaluator.update(Row(2), Row(10))
    val m1 = evaluator.merge(p1, p0, p2)
    assert(evaluator.eval(m1) === Row(22))

    // Update - Merge - Eval (empty partition at the end)
    val m2 = evaluator.merge(p2, p1, p0)
    assert(evaluator.eval(m2) === Row(22))
  }

}
