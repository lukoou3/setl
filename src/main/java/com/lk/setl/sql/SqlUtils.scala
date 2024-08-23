package com.lk.setl.sql

import com.lk.setl.Logging

import scala.collection.JavaConverters._
import com.lk.setl.sql.catalyst.QueryPlanningTracker
import com.lk.setl.sql.catalyst.execution.QueryExecution
import com.lk.setl.sql.catalyst.expressions.{Alias, NamedExpression}
import com.lk.setl.sql.catalyst.parser.CatalystSqlParser
import com.lk.setl.sql.catalyst.plans.logical.{Expr, Filter, LogicalPlan, RelationPlaceholder}
import com.lk.setl.sql.types.{DataType, StructType}

import java.util.regex.{Matcher, Pattern}

object SqlUtils extends Logging {
  val sqlParser = new CatalystSqlParser()
  val STRUCT_RE = Pattern.compile("""\s*struct\s*<(.+)>\s*""", Pattern.CASE_INSENSITIVE)

  def sqlPlan(sqlText: String, schemas: java.util.Map[String, StructType] ): LogicalPlan =  {
    sqlPlan(sqlText, schemas.asScala.toSeq)
  }

  def sqlPlan(sqlText: String, schema: StructType): LogicalPlan =  {
    sqlPlan(sqlText, Map("tab" -> RelationPlaceholder(schema.toAttributes)))
  }

  def sqlPlan(sqlText: String, schemas: Seq[(String, StructType)] ): LogicalPlan =  {
    sqlPlan(sqlText, schemas.toMap.mapValues(s => RelationPlaceholder(s.toAttributes)))
  }

  def sqlPlan(sqlText: String, tempViews: Map[String, RelationPlaceholder]): LogicalPlan =  {
    val tracker = new QueryPlanningTracker
    val plan = tracker.measurePhase(QueryPlanningTracker.PARSING) {
      sqlParser.parsePlan(sqlText)
    }
    val logicalPlan = new QueryExecution(tempViews, plan, tracker)
    logicalPlan.assertAnalyzed()
    logInfo(s"sqlPlan for $sqlText :")
    logInfo(s"analyzed plan:\n${logicalPlan.analyzed}")
    logInfo(s"optimized plan:\n${logicalPlan.optimizedPlan}")
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
  def parseExpr(sql: String, schema: StructType): Expr = {
    val tracker = new QueryPlanningTracker
    val expression = tracker.measurePhase(QueryPlanningTracker.PARSING) {
      sqlParser.parseExpression(sql)
    }
    val e = expression match {
      case n: NamedExpression => n
      case _ => Alias(expression, "v")()
    }
    val plan = Expr(e, RelationPlaceholder(schema.toAttributes))
    val logicalPlan = new QueryExecution(Map.empty, plan, tracker)
    val eval = logicalPlan.optimizedPlan.asInstanceOf[Expr]
    eval
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

  def truncateString(str: String, len : Int): String = {
    if(str.length <= len){
      str
    }else{
      str.substring(0, len) + "... "
    }
  }
}
