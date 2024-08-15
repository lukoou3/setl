package com.lk.setl.sql.catalyst.expressions.aggregate

import com.lk.setl.sql.AnalysisException
import com.lk.setl.sql.Row
import com.lk.setl.sql.catalyst.expressions.{AttributeReference, CodegenObjectFactoryMode, Literal, SafeProjection}
import com.lk.setl.sql.types.IntegerType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * 这个不就是聚合函数的测试吗
 */
class FirstLastTestSuite extends AnyFunSuite with BeforeAndAfterAll {
  val input = AttributeReference("input", IntegerType, nullable = true)()
  val evaluator = DeclarativeAggregateEvaluator(Last(input, false), Seq(input))
  val evaluatorIgnoreNulls = DeclarativeAggregateEvaluator(Last(input, true), Seq(input))

  override protected def beforeAll(): Unit = {
    System.setProperty("spark.testing", "true")
    //SafeProjection.fallbackMode = CodegenObjectFactoryMode.CODEGEN_ONLY
    SafeProjection.fallbackMode = CodegenObjectFactoryMode.NO_CODEGEN
  }

  test("empty buffer") {
    println(evaluator.initialize())
    assert(evaluator.initialize() === Row(null, false))
  }

  test("update") {
    val result = evaluator.update(
      Row(1),
      Row(9),
      Row(-1))
    println(result)
    assert(result === Row(-1, true))
  }

  test("update - ignore nulls") {
    val result1 = evaluatorIgnoreNulls.update(
      Row(null),
      Row(9),
      Row(null))
    println(result1)
    assert(result1 === Row(9, true))

    val result2 = evaluatorIgnoreNulls.update(
      Row(null),
      Row(null))
    println(result2)
    assert(result2 === Row(null, false))
  }

  test("merge") {
    // Empty merge
    val p0 = evaluator.initialize()
    assert(evaluator.merge(p0) === Row(null, false))

    // Single merge
    val p1 = evaluator.update(Row(1), Row(-99))
    assert(evaluator.merge(p1) === p1)

    // Multiple merges.
    val p2 = evaluator.update(Row(2), Row(10))
    assert(evaluator.merge(p1, p2) === p2)

    // Empty partitions (p0 is empty)
    assert(evaluator.merge(p1, p0, p2) === p2)
    assert(evaluator.merge(p2, p1, p0) === p1)
  }

  test("merge - ignore nulls") {
    // Multi merges
    val p1 = evaluatorIgnoreNulls.update(Row(1), Row(null))
    val p2 = evaluatorIgnoreNulls.update(Row(null), Row(null))
    assert(evaluatorIgnoreNulls.merge(p1, p2) === p1)
  }

  test("eval") {
    // Null Eval
    assert(evaluator.eval(Row(null, true)) === Row(null))
    assert(evaluator.eval(Row(null, false)) === Row(null))

    // Empty Eval
    val p0 = evaluator.initialize()
    assert(evaluator.eval(p0) === Row(null))

    // Update - Eval
    val p1 = evaluator.update(Row(1), Row(-99))
    assert(evaluator.eval(p1) === Row(-99))

    // Update - Merge - Eval
    val p2 = evaluator.update(Row(2), Row(10))
    val m1 = evaluator.merge(p1, p0, p2)
    assert(evaluator.eval(m1) === Row(10))

    // Update - Merge - Eval (empty partition at the end)
    val m2 = evaluator.merge(p2, p1, p0)
    assert(evaluator.eval(m2) === Row(-99))
  }

  test("eval - ignore nulls") {
    // Update - Merge - Eval
    val p1 = evaluatorIgnoreNulls.update(Row(1), Row(null))
    val p2 = evaluatorIgnoreNulls.update(Row(null), Row(null))
    val m1 = evaluatorIgnoreNulls.merge(p1, p2)
    assert(evaluatorIgnoreNulls.eval(m1) === Row(1))
  }

  test("SPARK-32344: correct error handling for a type mismatch") {
    val msg1 = intercept[AnalysisException] {
      new First(input, Literal(1, IntegerType))
    }.getMessage
    assert(msg1.contains("The second argument in first should be a boolean literal"))
    val msg2 = intercept[AnalysisException] {
      new Last(input, Literal(1, IntegerType))
    }.getMessage
    assert(msg2.contains("The second argument in last should be a boolean literal"))
  }

}
