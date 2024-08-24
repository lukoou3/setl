package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.{ArrayData, Row}
import com.lk.setl.sql.catalyst.analysis.ResolveTimeZone
import com.lk.setl.sql.catalyst.optimizer.Optimizer
import com.lk.setl.sql.catalyst.plans.logical.{OneRowRelation, Project}
import com.lk.setl.sql.types.{ArrayType, DataType, StructType}
import org.scalatest.Assertions.{fail, intercept, withClue}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import scala.reflect.ClassTag

trait ExpressionEvalHelper {

  private def copyBySerialize[T](obj: T) = try {
    val bos = new ByteArrayOutputStream
    val out = new ObjectOutputStream(bos)
    try {
      out.writeObject(obj)
      try {
        val bis = new ByteArrayInputStream(bos.toByteArray)
        val in = new ObjectInputStream(bis)
        try in.readObject.asInstanceOf[T]
        finally {
          if (bis != null) bis.close()
          if (in != null) in.close()
        }
      }
    } catch {
      case e: Exception =>
        throw new RuntimeException(e)
    } finally {
      if (bos != null) bos.close()
      if (out != null) out.close()
    }
  }

  private def prepareEvaluation(expression: Expression) = {
    val resolver = ResolveTimeZone
    val expr = resolver.resolveTimeZones(expression)
    assert(expr.resolved)
    copyBySerialize(expr)
  }

  protected def evaluateWithoutCodegen(
      expression: Expression, inputRow: Row = EmptyRow): Any = {
    expression.foreach {
      case n: Nondeterministic => n.initialize(0)
      case _ =>
    }
    expression.eval(inputRow)
  }

  protected def checkEvaluationWithoutCodegen(
      expression: Expression,
      expected: Any,
      inputRow: Row = EmptyRow): Unit = {

    val actual = try evaluateWithoutCodegen(expression, inputRow) catch {
      case e: Exception => fail(s"Exception evaluating $expression", e)
    }
    if (!checkResult(actual, expected, expression)) {
      val input = if (inputRow == EmptyRow) "" else s", input: $inputRow"
      fail(s"Incorrect evaluation (codegen off): $expression, " +
        s"actual: $actual, " +
        s"expected: $expected$input")
    }
  }

  protected def checkEvaluation(
    expression: => Expression, expected: Any, inputRow: Row = EmptyRow): Unit = {
    // Make it as method to obtain fresh expression everytime.
    def expr = prepareEvaluation(expression)
    val catalystValue = expected
    checkEvaluationWithoutCodegen(expr, catalystValue, inputRow)
    checkEvaluationWithMutableProjection(expr, catalystValue, inputRow)
    checkEvaluationWithOptimization(expr, catalystValue, inputRow)
  }

  protected def checkEvaluationWithOptimization(
    expression: Expression,
    expected: Any,
    inputRow: Row = EmptyRow): Unit = {
    val plan = Project(Alias(expression, s"Optimized($expression)")() :: Nil, OneRowRelation())
    val optimizer = new Optimizer
    val optimizedPlan = optimizer.execute(plan)
    checkEvaluationWithoutCodegen(optimizedPlan.expressions.head, expected, inputRow)
  }

  protected def checkEvaluationWithMutableProjection(
      expression: => Expression,
      expected: Any,
      inputRow: Row = EmptyRow): Unit = {
    val modes = Seq(CodegenObjectFactoryMode.CODEGEN_ONLY, CodegenObjectFactoryMode.NO_CODEGEN)
    for (fallbackMode <- modes) {
      MutableProjection.fallbackMode = fallbackMode
      val actual = evaluateWithMutableProjection(expression, inputRow)
      if (!checkResult(actual, expected, expression)) {
        val input = if (inputRow == EmptyRow) "" else s", input: $inputRow"
        fail(s"Incorrect evaluation (fallback mode = $fallbackMode): $expression, " +
          s"actual: $actual, expected: $expected$input")
      }
    }
  }

  protected def evaluateWithMutableProjection(
      expression: => Expression,
      inputRow: Row = EmptyRow): Any = {
    val plan = MutableProjection.create(Alias(expression, s"Optimized($expression)")() :: Nil)
    plan.initialize(0)

    plan(inputRow).get(0)
  }

  protected def checkExceptionInExpression[T <: Throwable : ClassTag](
      expression: => Expression,
      expectedErrMsg: String): Unit = {
    checkExceptionInExpression[T](expression, Row.empty, expectedErrMsg)
  }

  protected def checkExceptionInExpression[T <: Throwable : ClassTag](
      expression: => Expression,
      inputRow: Row,
      expectedErrMsg: String): Unit = {

    def checkException(eval: => Unit, testMode: String): Unit = {
      val modes = Seq(CodegenObjectFactoryMode.CODEGEN_ONLY, CodegenObjectFactoryMode.NO_CODEGEN)
      withClue(s"($testMode)") {
        val errMsg = intercept[T] {
          for (fallbackMode <- modes) {
            MutableProjection.fallbackMode = fallbackMode
            eval
          }
        }.getMessage
        if (errMsg == null) {
          if (expectedErrMsg != null) {
            fail(s"Expected null error message, but `$errMsg` found")
          }
        } else if (!errMsg.contains(expectedErrMsg)) {
          fail(s"Expected error message is `$expectedErrMsg`, but `$errMsg` found")
        }
      }
    }

    // Make it as method to obtain fresh expression everytime.
    def expr = prepareEvaluation(expression)
    checkException(evaluateWithoutCodegen(expr, inputRow), "non-codegen mode")
    checkException(evaluateWithMutableProjection(expr, inputRow), "codegen mode")
  }

  /**
   * Check the equality between result of expression and expected value, it will handle
   * Array[Byte], Spread[Double], MapData and Row. Also check whether nullable in expression is
   * true if result is null
   */
  protected def checkResult(result: Any, expected: Any, expression: Expression): Boolean = {
    checkResult(result, expected, expression.dataType, expression.nullable)
  }

  protected def checkResult(
      result: Any,
      expected: Any,
      exprDataType: DataType,
      exprNullable: Boolean): Boolean = {
    val dataType = exprDataType

    // The result is null for a non-nullable expression
    assert(result != null || exprNullable, "exprNullable should be true if result is null")
    (result, expected) match {
      case (result: Array[Byte], expected: Array[Byte]) =>
        java.util.Arrays.equals(result, expected)
      case (result: Row, expected: Row) =>
        val st = dataType.asInstanceOf[StructType]
        assert(result.numFields == st.length && expected.numFields == st.length)
        st.zipWithIndex.forall { case (f, i) =>
          checkResult(result.get(i), expected.get(i), f.dataType, true)
        }
      case (result: ArrayData, expected: ArrayData) =>
        result.numElements == expected.numElements && {
          val ArrayType(et, cn) = dataType.asInstanceOf[ArrayType]
          var isSame = true
          var i = 0
          while (isSame && i < result.numElements) {
            isSame = checkResult(result.get(i), expected.get(i), et, cn)
            i += 1
          }
          isSame
        }
      case (result: Double, expected: Double) =>
        if (expected.isNaN) result.isNaN else expected == result
      case (result: Float, expected: Float) =>
        if (expected.isNaN) result.isNaN else expected == result
      case (result: Row, expected: Row) => result == expected
      case _ =>
        (result == null && expected == null) || (result != null && result.equals(expected))
    }
  }
}
