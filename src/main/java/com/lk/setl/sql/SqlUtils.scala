package com.lk.setl.sql

import com.lk.setl.sql.catalyst.QueryPlanningTracker
import com.lk.setl.sql.catalyst.execution.QueryExecution
import com.lk.setl.sql.catalyst.parser.CatalystSqlParser
import com.lk.setl.sql.catalyst.plans.logical.{Filter, LogicalPlan, RelationPlaceholder}
import com.lk.setl.sql.types.{DataType, StructType}

import java.util.regex.{Matcher, Pattern}

object SqlUtils {
  val sqlParser = new CatalystSqlParser()
  val STRUCT_RE = Pattern.compile("""\s*struct\s*<(.+)>\s*""", Pattern.CASE_INSENSITIVE)

  def sqlPlan(sqlText: String, schema: StructType): LogicalPlan =  {
    val tracker = new QueryPlanningTracker
    val plan = tracker.measurePhase(QueryPlanningTracker.PARSING) {
      sqlParser.parsePlan(sqlText)
    }
    val logicalPlan = new QueryExecution(Map("tab" -> RelationPlaceholder(schema.toAttributes)), plan, tracker)
    logicalPlan.assertAnalyzed()
    logicalPlan.optimizedPlan
  }

  def parseFilter(condition: String, schema: StructType): Filter = {
    val tracker = new QueryPlanningTracker
    val expression = tracker.measurePhase(QueryPlanningTracker.PARSING) {
      sqlParser.parseExpression(condition)
    }
    val plan = Filter(expression, RelationPlaceholder(schema.toAttributes))
    val logicalPlan = new QueryExecution(Map.empty, plan, tracker)
    logicalPlan.assertAnalyzed()
    val filter = logicalPlan.optimizedPlan.asInstanceOf[Filter]
    filter
  }

  def parseDataType(sql: String): DataType = sqlParser.parseDataType(sql)

  def parseStructType(sql: String):StructType = {
    try {
      sqlParser.parseDataType(sql).asInstanceOf[StructType]
    } catch {
      case e: Exception if !STRUCT_RE.matcher(sql).matches()=>
        try {
          sqlParser.parseDataType(s"struct<$sql>").asInstanceOf[StructType]
        } catch {
          case _ =>
            throw e
        }
      case e: Exception =>
        throw e
    }
  }

}
