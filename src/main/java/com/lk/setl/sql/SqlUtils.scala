package com.lk.setl.sql

import com.lk.setl.sql.catalyst.QueryPlanningTracker
import com.lk.setl.sql.catalyst.execution.QueryExecution
import com.lk.setl.sql.catalyst.parser.CatalystSqlParser
import com.lk.setl.sql.catalyst.plans.logical.{LogicalPlan, RelationPlaceholder}
import com.lk.setl.sql.types.StructType

object SqlUtils {
  val sqlParser = new CatalystSqlParser()

  def sqlPlan(sqlText: String, schema: StructType): LogicalPlan =  {
    val tracker = new QueryPlanningTracker
    val plan = tracker.measurePhase(QueryPlanningTracker.PARSING) {
      sqlParser.parsePlan(sqlText)
    }
    val logicalPlan = new QueryExecution(Map("tab" -> RelationPlaceholder(schema.toAttributes)), plan, tracker)
    logicalPlan.assertAnalyzed()
    logicalPlan.analyzed
  }

}
