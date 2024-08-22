package com.lk.setl.sql.catalyst.parser

import com.lk.setl.sql.SqlUtils
import com.lk.setl.sql.catalyst.QueryPlanningTracker
import com.lk.setl.sql.catalyst.execution.QueryExecution
import com.lk.setl.sql.catalyst.planning.PhysicalAggregation
import com.lk.setl.sql.catalyst.plans.logical._
import org.scalatest.funsuite.AnyFunSuite

class TimeWindowAggParserSuite extends AnyFunSuite {

  test("parseTimeWindowAggWithGroupBy") {
    val schema = SqlUtils.parseStructType("id: bigint, name: string, age: int, score: double")
    val tempViews = Map("tab" -> RelationPlaceholder(schema.toAttributes))
    val sql = "select window_start timestamp, id, sum(score) score from tab where id > 10 group by process_time_window(5), id"
    // val sql = "select id, sum(score) score from tab where id > 10 group by id"
    val singleStatement = new CatalystSqlParser().parsePlan(sql)
    println(singleStatement)
    val tracker = new QueryPlanningTracker
    val logicalPlan = new QueryExecution(tempViews, singleStatement, tracker)
    logicalPlan.assertAnalyzed()
    val analyzed = logicalPlan.analyzed
    val optimized = logicalPlan.optimizedPlan
    println(analyzed)
    println(optimized)
    val aggs = PhysicalAggregation.unapply(optimized)
    aggs.get.productIterator.foreach(println(_))
  }

}
