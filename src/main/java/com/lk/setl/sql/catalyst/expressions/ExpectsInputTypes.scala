package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.catalyst.analysis.TypeCheckResult
import com.lk.setl.sql.types.AbstractDataType

/**
 * 期望输出输入类型
 * A trait that gets mixin to define the expected input types of an expression.
 *
 * This trait is typically used by operator expressions (e.g. [[Add]], [[Subtract]]) to define
 * expected input types without any implicit casting.
 *
 * Most function expressions (e.g. [[Substring]] should extend [[ImplicitCastInputTypes]]) instead.
 */
trait ExpectsInputTypes extends Expression {

  /**
   * Expected input types from child expressions. The i-th position in the returned seq indicates
   * the type requirement for the i-th child.
   *
   * The possible values at each position are:
   * 1. a specific data type, e.g. LongType, StringType.
   * 2. a non-leaf abstract data type, e.g. NumericType, IntegralType, FractionalType.
   */
  def inputTypes: Seq[AbstractDataType]

  override def checkInputDataTypes(): TypeCheckResult = {
    ExpectsInputTypes.checkInputDataTypes(children, inputTypes)
  }
}

object ExpectsInputTypes {

  def checkInputDataTypes(
      inputs: Seq[Expression],
      inputTypes: Seq[AbstractDataType]): TypeCheckResult = {
    val mismatches = inputs.zip(inputTypes).zipWithIndex.collect {
      case ((input, expected), idx) if !expected.acceptsType(input.dataType) =>
        s"argument ${idx + 1} requires ${expected.simpleString} type, " +
          s"however, '${input.sql}' is of ${input.dataType.catalogString} type."
    }

    if (mismatches.isEmpty) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(mismatches.mkString(" "))
    }
  }
}

/**
 * A mixin for the analyzer to perform implicit type casting using
 * [[ImplicitTypeCasts]].
 */
trait ImplicitCastInputTypes extends ExpectsInputTypes {
  // No other methods
}
