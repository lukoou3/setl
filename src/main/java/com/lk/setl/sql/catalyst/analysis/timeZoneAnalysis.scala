package com.lk.setl.sql.catalyst.analysis

import com.lk.setl.sql.SQLConf
import com.lk.setl.sql.catalyst.expressions.{Expression, TimeZoneAwareExpression}
import com.lk.setl.sql.catalyst.plans.logical.LogicalPlan
import com.lk.setl.sql.catalyst.rules.Rule

/**
 * 替换TimeZoneAwareExpression中没有timezone配置的值
 * Replace [[TimeZoneAwareExpression]] without timezone id by its copy with session local
 * time zone.
 */
object ResolveTimeZone extends Rule[LogicalPlan] {
  private val transformTimeZoneExprs: PartialFunction[Expression, Expression] = {
    case e: TimeZoneAwareExpression if e.timeZoneId.isEmpty =>
      e.withTimeZone(SQLConf.sessionLocalTimeZone)
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.resolveExpressions(transformTimeZoneExprs)

  def resolveTimeZones(e: Expression): Expression = e.transform(transformTimeZoneExprs)
}

