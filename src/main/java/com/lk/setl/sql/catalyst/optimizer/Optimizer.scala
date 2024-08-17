package com.lk.setl.sql.catalyst.optimizer

import com.lk.setl.sql.catalyst.plans.logical.LogicalPlan
import com.lk.setl.sql.catalyst.rules.RuleExecutor

class Optimizer extends RuleExecutor[LogicalPlan] {

  protected def fixedPoint =
    FixedPoint(
      100, //conf.optimizerMaxIterations,
      maxIterationsSetting = "sql.optimizer.maxIterations")

  def defaultBatches: Seq[Batch] = {
    val operatorOptimizationRuleSet = Seq(
      ConstantFolding,
      LikeSimplification
    )
    val operatorOptimizationBatch: Seq[Batch] = Seq(
      Batch("Operator Optimization", fixedPoint, operatorOptimizationRuleSet: _*)
    )
    operatorOptimizationBatch
  }

  override protected def batches: Seq[Batch] = defaultBatches
}
