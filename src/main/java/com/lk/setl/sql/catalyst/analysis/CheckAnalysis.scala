package com.lk.setl.sql.catalyst.analysis

import com.lk.setl.sql.AnalysisException
import com.lk.setl.sql.catalyst.expressions._
import com.lk.setl.sql.catalyst.plans.logical.{Filter, LeafNode, LogicalPlan, PlanHelper, Project}
import com.lk.setl.sql.types.BooleanType

trait CheckAnalysis {
  /**
   * Override to provide additional checks for correct analysis.
   * These rules will be evaluated after our built-in check rules.
   */
  val extendedCheckRules: Seq[LogicalPlan => Unit] = Nil

  protected def failAnalysis(msg: String): Nothing = {
    throw new AnalysisException(msg)
  }

  protected def containsMultipleGenerators(exprs: Seq[Expression]): Boolean = {
    exprs.flatMap(_.collect {
      case e: Generator => e
    }).length > 1
  }

  def checkAnalysis(plan: LogicalPlan): Unit = {
    // We transform up and order the rules so as to catch the first possible failure instead
    // of the result of cascading resolution failures.
    plan.foreachUp {

      case p if p.analyzed => // Skip already analyzed sub-plans

      case u: UnresolvedRelation =>
        u.failAnalysis(s"Table or view not found: ${u.multipartIdentifier.quoted}")

      case operator: LogicalPlan =>

        operator transformExpressionsUp {
          case a: Attribute if !a.resolved =>
            val from = operator.inputSet.toSeq.map(_.qualifiedName).mkString(", ")
            a.failAnalysis(s"cannot resolve '${a.sql}' given input columns: [$from]")

          case e: Expression if e.checkInputDataTypes().isFailure =>
            e.checkInputDataTypes() match {
              case TypeCheckResult.TypeCheckFailure(message) =>
                e.failAnalysis(
                  s"cannot resolve '${e.sql}' due to data type mismatch: $message")
            }

          case c: Cast if !c.resolved =>
            failAnalysis(s"invalid cast from ${c.child.dataType.catalogString} to " +
              c.dataType.catalogString)

        }

        operator match {
          case f: Filter if f.condition.dataType != BooleanType =>
            failAnalysis(
              s"filter expression '${f.condition.sql}' " +
                s"of type ${f.condition.dataType.catalogString} is not a boolean.")

          case _ => // Fallbacks to the following checks
        }

        operator match {
          case o if o.children.nonEmpty && o.missingInput.nonEmpty =>
            val missingAttributes = o.missingInput.mkString(",")
            val input = o.inputSet.mkString(",")
            val msgForMissingAttributes = s"Resolved attribute(s) $missingAttributes missing " +
              s"from $input in operator ${operator.simpleString(25)}."

            val resolver = caseSensitiveResolution
            val attrsWithSameName = o.missingInput.filter { missing =>
              o.inputSet.exists(input => resolver(missing.name, input.name))
            }

            val msg = if (attrsWithSameName.nonEmpty) {
              val sameNames = attrsWithSameName.map(_.name).mkString(",")
              s"$msgForMissingAttributes Attribute(s) with the same name appear in the " +
                s"operation: $sameNames. Please check if the right attribute(s) are used."
            } else {
              msgForMissingAttributes
            }

            failAnalysis(msg)

          case p @ Project(exprs, _) if containsMultipleGenerators(exprs) =>
            failAnalysis(
              s"""Only a single table generating function is allowed in a SELECT clause, found:
                 | ${exprs.map(_.sql).mkString(",")}""".stripMargin)

          case f @ Filter(condition, _)
            if PlanHelper.specialExpressionsInUnsupportedOperator(f).nonEmpty =>
            val invalidExprSqls = PlanHelper.specialExpressionsInUnsupportedOperator(f).map(_.sql)
            failAnalysis(
              s"""
                 |Aggregate/Window/Generate expressions are not valid in where clause of the query.
                 |Expression in where clause: [${condition.sql}]
                 |Invalid expressions: [${invalidExprSqls.mkString(", ")}]""".stripMargin)

          case other if PlanHelper.specialExpressionsInUnsupportedOperator(other).nonEmpty =>
            val invalidExprSqls =
              PlanHelper.specialExpressionsInUnsupportedOperator(other).map(_.sql)
            failAnalysis(
              s"""
                 |The query operator `${other.nodeName}` contains one or more unsupported
                 |expression types Aggregate, Window or Generate.
                 |Invalid expressions: [${invalidExprSqls.mkString(", ")}]""".stripMargin
            )

          case _ => // Analysis successful!
        }
    }
    extendedCheckRules.foreach(_(plan))
    plan.foreachUp {
      case o if !o.resolved =>
        failAnalysis(s"unresolved operator ${o.simpleString(25)}")
      case _ =>
    }

    plan.setAnalyzed()
  }


}
