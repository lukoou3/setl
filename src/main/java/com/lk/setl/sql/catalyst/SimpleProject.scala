package com.lk.setl.sql.catalyst

import com.lk.setl.sql.catalyst.expressions.{Expression, NamedExpression}

case class SimpleProject(
  projectList: Seq[NamedExpression],
  from: Seq[String],
  where: Option[Expression]
)
