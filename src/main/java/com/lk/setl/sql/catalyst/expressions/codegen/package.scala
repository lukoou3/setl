package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.catalyst.rules

/**
 * A collection of generators that build custom bytecode at runtime for performing the evaluation
 * of catalyst expression.
 */
package object codegen {

  /** Canonicalizes an expression so those that differ only by names can reuse the same code. */
  object ExpressionCanonicalizer extends rules.RuleExecutor[Expression] {
    val batches =
      Batch("CleanExpressions", FixedPoint(20), CleanExpressions) :: Nil

    object CleanExpressions extends rules.Rule[Expression] {
      def apply(e: Expression): Expression = e transform {
        case Alias(c, _) => c
      }
    }
  }
}
