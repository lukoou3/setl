package com.lk.setl.sql.catalyst.expressions.objects

import com.lk.setl.sql.Row
import com.lk.setl.sql.catalyst.expressions.{LeafExpression, NonSQLExpression}
import com.lk.setl.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral, JavaCode}
import com.lk.setl.sql.types.DataType

object LambdaVariable {
  private val curId = new java.util.concurrent.atomic.AtomicLong()

  // Returns the codegen-ed `LambdaVariable` and add it to mutable states, so that it can be
  // accessed anywhere in the generated code.
  /*def prepareLambdaVariable(ctx: CodegenContext, variable: LambdaVariable): ExprCode = {
    val variableCode = variable.genCode(ctx)
    assert(variableCode.code.isEmpty)

    ctx.addMutableState(
      CodeGenerator.javaType(variable.dataType),
      variableCode.value,
      forceInline = true,
      useFreshName = false)

    if (variable.nullable) {
      ctx.addMutableState(
        CodeGenerator.JAVA_BOOLEAN,
        variableCode.isNull,
        forceInline = true,
        useFreshName = false)
    }

    variableCode
  }*/
}

/**
 * A placeholder for the loop variable used in [[MapObjects]]. This should never be constructed
 * manually, but will instead be passed into the provided lambda function.
 */
// TODO: Merge this and `NamedLambdaVariable`.
case class LambdaVariable(
  name: String,
  dataType: DataType,
  id: Long = LambdaVariable.curId.incrementAndGet) extends LeafExpression with NonSQLExpression {

  private val accessor: (Row, Int) => Any = (row, i) => row.get(i)

  // Interpreted execution of `LambdaVariable` always get the 0-index element from input row.
  override def eval(input:Row): Any = {
    assert(input.numFields == 1,
      "The input row of interpreted LambdaVariable should have only 1 field.")
    accessor(input, 0)
  }

  override def genCode(ctx: CodegenContext): ExprCode = {
    // If `LambdaVariable` IDs are reassigned by the `ReassignLambdaVariableID` rule, the IDs will
    // all be negative.
    val suffix = "lambda_variable_" + math.abs(id)
    val isNull = if (nullable) {
      JavaCode.isNullVariable(s"isNull_${name}_$suffix")
    } else {
      FalseLiteral
    }
    val value = JavaCode.variable(s"value_${name}_$suffix", dataType)
    ExprCode(isNull, value)
  }

  // This won't be called as `genCode` is overrided, just overriding it to make
  // `LambdaVariable` non-abstract.
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ev

}

