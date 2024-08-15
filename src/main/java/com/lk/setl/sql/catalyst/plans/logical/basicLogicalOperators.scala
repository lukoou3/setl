package com.lk.setl.sql.catalyst.plans.logical

import com.lk.setl.sql.catalyst.expressions._

case class Project(projectList: Seq[NamedExpression], child: LogicalPlan)
    extends OrderPreservingUnaryNode {
  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override lazy val resolved: Boolean = {
    val hasSpecialExpressions = false

    !expressions.exists(!_.resolved) && childrenResolved && !hasSpecialExpressions
  }

  override lazy val validConstraints: ExpressionSet =
    getAllValidConstraints(projectList)
}