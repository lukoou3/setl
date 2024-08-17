package com.lk.setl.sql.catalyst.execution

import com.lk.setl.Logging
import com.lk.setl.sql.catalyst.QueryPlanningTracker
import com.lk.setl.sql.catalyst.analysis.Analyzer
import com.lk.setl.sql.catalyst.optimizer.Optimizer
import com.lk.setl.sql.catalyst.plans.logical.{LogicalPlan, RelationPlaceholder}

import java.util.concurrent.atomic.AtomicLong

/**
 * saprk sql的主要流程在这个类里
 *
 * 使用Spark执行关系查询的主要工作流程。旨在让开发人员轻松访问查询执行的中间阶段。
 * The primary workflow for executing relational queries using Spark.  Designed to allow easy
 * access to the intermediate phases of query execution for developers.
 *
 * 虽然这不是一个公共类，但我们应该避免为了更改而更改函数名，因为很多开发人员使用该功能进行调试。
 * While this is not a public class, we should avoid changing the function names for the sake of
 * changing them, because a lot of developers use the feature for debugging.
 */
class QueryExecution(
    val tempViews: Map[String, RelationPlaceholder],
    val logical: LogicalPlan,
    val tracker: QueryPlanningTracker = new QueryPlanningTracker) extends Logging {
  val id: Long = QueryExecution.nextExecutionId
  val analyzer = new Analyzer(tempViews)


  def assertAnalyzed(): Unit = analyzed

  def assertSupported(): Unit = {}

  //analyzer阶段
  lazy val analyzed: LogicalPlan = executePhase(QueryPlanningTracker.ANALYSIS) {
    // We can't clone `logical` here, which will reset the `_analyzed` flag.
    analyzer.executeAndCheck(logical, tracker)
  }

  //optimizer阶段, 优化器
  lazy val optimizedPlan: LogicalPlan = executePhase(QueryPlanningTracker.OPTIMIZATION) {
    // clone the plan to avoid sharing the plan instance between different stages like analyzing,
    // optimizing and planning.
    val plan = QueryExecution.optimizer.executeAndTrack(analyzed.clone(), tracker)
    // We do not want optimized plans to be re-analyzed as literals that have been constant folded
    // and such can cause issues during analysis. While `clone` should maintain the `analyzed` state
    // of the LogicalPlan, we set the plan as analyzed here as well out of paranoia.
    plan.setAnalyzed()
    plan
  }

  protected def executePhase[T](phase: String)(block: => T): T = {
    tracker.measurePhase(phase)(block)
  }

}

object QueryExecution {
  private val _nextExecutionId = new AtomicLong(0)

  private def nextExecutionId: Long = _nextExecutionId.getAndIncrement

  val optimizer = new Optimizer
}