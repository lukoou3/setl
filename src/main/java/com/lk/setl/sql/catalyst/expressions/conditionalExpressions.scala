package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.Row
import com.lk.setl.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
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

  override def toString: String = s"if ($predicate) $trueValue else $falseValue"

  override def sql: String = s"(IF(${predicate.sql}, ${trueValue.sql}, ${falseValue.sql}))"
}
