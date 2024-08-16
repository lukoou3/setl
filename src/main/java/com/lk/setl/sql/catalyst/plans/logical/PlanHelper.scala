package com.lk.setl.sql.catalyst.plans.logical

import com.lk.setl.sql.catalyst.expressions.{Expression, Generator}
import com.lk.setl.sql.catalyst.expressions.aggregate.AggregateFunction

/**
 * PlanHelper包含Analyzer和Optimizer可以使用的实用方法。它也可以是Analyzer和Optimizer中多个规则中通用的方法的容器。
 * [[PlanHelper]] contains utility methods that can be used by Analyzer and Optimizer.
 * It can also be container of methods that are common across multiple rules in Analyzer
 * and Optimizer.
 */
object PlanHelper {
  /**
   * Check if there's any expression in this query plan operator that is
   * - A WindowExpression but the plan is not Window
   * - An AggregateExpresion but the plan is not Aggregate or Window
   * - A Generator but the plan is not Generate
   * Returns the list of invalid expressions that this operator hosts. This can happen when
   * 1. The input query from users contain invalid expressions.
   *    Example : SELECT * FROM tab WHERE max(c1) > 0
   * 2. Query rewrites inadvertently produce plans that are invalid.
   */
  def specialExpressionsInUnsupportedOperator(plan: LogicalPlan): Seq[Expression] = {
    val exprs = plan.expressions
    val invalidExpressions = exprs.flatMap { root =>
      root.collect {
        /*case e: AggregateFunction
          if !(plan.isInstanceOf[Aggregate]) => e*/
        case e: Generator
          if !plan.isInstanceOf[Generate] => e
      }
    }
    invalidExpressions
  }
}
