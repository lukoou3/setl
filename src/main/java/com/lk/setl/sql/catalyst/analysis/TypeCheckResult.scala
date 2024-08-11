package com.lk.setl.sql.catalyst.analysis

/**
 * Represents the result of `Expression.checkInputDataTypes`.
 * We will throw `AnalysisException` in `CheckAnalysis` if `isFailure` is true.
 */
trait TypeCheckResult {
  def isFailure: Boolean = !isSuccess
  def isSuccess: Boolean
}

object TypeCheckResult {

  /**
   * Represents the successful result of `Expression.checkInputDataTypes`.
   */
  object TypeCheckSuccess extends TypeCheckResult {
    def isSuccess: Boolean = true
  }

  /**
   * Represents the failing result of `Expression.checkInputDataTypes`,
   * with an error message to show the reason of failure.
   */
  case class TypeCheckFailure(message: String) extends TypeCheckResult {
    def isSuccess: Boolean = false
  }
}
