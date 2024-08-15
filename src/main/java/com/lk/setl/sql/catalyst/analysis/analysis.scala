package com.lk.setl.sql.catalyst.analysis

import com.lk.setl.sql.catalyst.plans.logical.LogicalPlan
import com.lk.setl.sql.catalyst.rules.{Rule, RuleExecutor}

/**
 * Provides a logical query plan analyzer, which translates [[UnresolvedAttribute]]s and
 * [[UnresolvedRelation]]s into fully typed objects using information in a [[SessionCatalog]].
 */
class Analyzer extends RuleExecutor[LogicalPlan]{

  /**
   * If the plan cannot be resolved within maxIterations, analyzer will throw exception to inform
   * user to increase the value of SQLConf.ANALYZER_MAX_ITERATIONS.
   */
  protected def fixedPoint =
    FixedPoint(
      100, //conf.analyzerMaxIterations,
      errorOnExceed = true,
      maxIterationsSetting = "sql.analyzer.maxIterations")

  override protected def batches: Seq[Batch] = Seq(

  )


  /**
   * Replaces [[UnresolvedAttribute]]s with concrete [[AttributeReference]]s from
   * a logical plan node's children.
   */
  object ResolveReferences extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = ???
  }
}


