package com.lk.setl.sql.catalyst.planning

import com.lk.setl.sql.catalyst.expressions._
import com.lk.setl.sql.catalyst.expressions.aggregate.AggregateExpression
import com.lk.setl.sql.catalyst.plans.logical._

trait OperationHelper {
  type ReturnType = (Seq[NamedExpression], Seq[Expression], LogicalPlan)

  protected def collectAliases(fields: Seq[Expression]): AttributeMap[Expression] =
    AttributeMap(fields.collect {
      case a: Alias => (a.toAttribute, a.child)
    })

  protected def substitute(aliases: AttributeMap[Expression])(expr: Expression): Expression = {
    // use transformUp instead of transformDown to avoid dead loop
    // in case of there's Alias whose exprId is the same as its child attribute.
    expr.transformUp {
      case a @ Alias(ref: AttributeReference, name) =>
        aliases.get(ref)
          .map(Alias(_, name)(a.exprId, a.qualifier))
          .getOrElse(a)

      case a: AttributeReference =>
        aliases.get(a)
          .map(Alias(_, a.name)(a.exprId, a.qualifier)).getOrElse(a)
    }
  }
}

/**
 * A pattern that matches any number of project or filter operations on top of another relational
 * operator.  All filter operators are collected and their conditions are broken up and returned
 * together with the top project operator.
 * [[com.lk.setl.sql.catalyst.expressions.Alias Aliases]] are in-lined/substituted if
 * necessary.
 */
object PhysicalOperation extends OperationHelper with PredicateHelper {

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    val (fields, filters, child, _) = collectProjectsAndFilters(plan)
    Some((fields.getOrElse(child.output), filters, child))
  }

  /**
   * Collects all deterministic projects and filters, in-lining/substituting aliases if necessary.
   * Here are two examples for alias in-lining/substitution.
   * Before:
   * {{{
   *   SELECT c1 FROM (SELECT key AS c1 FROM t1) t2 WHERE c1 > 10
   *   SELECT c1 AS c2 FROM (SELECT key AS c1 FROM t1) t2 WHERE c1 > 10
   * }}}
   * After:
   * {{{
   *   SELECT key AS c1 FROM t1 WHERE key > 10
   *   SELECT key AS c2 FROM t1 WHERE key > 10
   * }}}
   */
  private def collectProjectsAndFilters(plan: LogicalPlan):
      (Option[Seq[NamedExpression]], Seq[Expression], LogicalPlan, AttributeMap[Expression]) =
    plan match {
      case Project(fields, child) if fields.forall(_.deterministic) =>
        val (_, filters, other, aliases) = collectProjectsAndFilters(child)
        val substitutedFields = fields.map(substitute(aliases)).asInstanceOf[Seq[NamedExpression]]
        (Some(substitutedFields), filters, other, collectAliases(substitutedFields))

      case Filter(condition, child) if condition.deterministic =>
        val (fields, filters, other, aliases) = collectProjectsAndFilters(child)
        val substitutedCondition = substitute(aliases)(condition)
        (fields, filters ++ splitConjunctivePredicates(substitutedCondition), other, aliases)

      case other =>
        (None, Nil, other, AttributeMap(Seq()))
    }
}


/**
 * 在规划聚合的物理执行时使用的提取器。与逻辑聚合相比，执行以下转换：
 *   未命名的分组表达式被命名，以便在聚合的各个阶段都可以引用它们
 *   出现多次的聚合将被消除重复。
 *   聚合本身的计算与最终结果是分开的。例如，count+1中的计数将被拆分为AggregateExpression和计算count.resultAttribute+1的最终计算。
 * An extractor used when planning the physical execution of an aggregation. Compared with a logical
 * aggregation, the following transformations are performed:
 *  - Unnamed grouping expressions are named so that they can be referred to across phases of
 *    aggregation
 *  - Aggregations that appear multiple times are deduplicated.
 *  - The computation of the aggregations themselves is separated from the final result. For
 *    example, the `count` in `count + 1` will be split into an [[AggregateExpression]] and a final
 *    computation that computes `count.resultAttribute + 1`.
 */
object PhysicalAggregation {
  // groupingExpressions, aggregateExpressions, resultExpressions, child
  type ReturnType = (Seq[NamedExpression], Seq[Expression], Seq[NamedExpression], LogicalPlan)

  def unapply(a: Any): Option[ReturnType] = a match {
    case Aggregate(groupingExpressions, resultExpressions, child) =>
      // 单个聚合表达式可能会在resultExpression中多次出现。
      // 为了避免多次评估单个聚合函数，我们将构建一组语义上不同的聚合表达式并重写表达式，以便它们引用了实际计算的聚合函数的单个副本。
      // 非确定性聚合表达式不会进行重复数据消除。
      // A single aggregate expression might appear multiple times in resultExpressions.
      // In order to avoid evaluating an individual aggregate function multiple times, we'll
      // build a set of semantically distinct aggregate expressions and re-write expressions so
      // that they reference the single copy of the aggregate function which actually gets computed.
      // Non-deterministic aggregate expressions are not deduplicated.
      val equivalentAggregateExpressions = new EquivalentExpressions
      // agg表达式就是个中间结果，提取非重复聚合表达式
      val aggregateExpressions = resultExpressions.flatMap { expr =>
        expr.collect {
          // addExpr() always returns false for non-deterministic expressions and do not add them.
          case agg: AggregateExpression
            if !equivalentAggregateExpressions.addExpr(agg) => agg
        }
      }

      val namedGroupingExpressions = groupingExpressions.map {
        case ne: NamedExpression => ne -> ne
        // If the expression is not a NamedExpressions, we add an alias.
        // So, when we generate the result of the operator, the Aggregate Operator
        // can directly get the Seq of attributes representing the grouping expressions.
        case other =>
          val withAlias = Alias(other, other.toString)()
          other -> withAlias
      }
      val groupExpressionMap = namedGroupingExpressions.toMap

      // 原始的“resultExpression”是一组表达式，可以引用聚合表达式、分组列值和常量。
      // 当聚合运算符最终发送输出行时，我们将使用“resultExpression”生成一个输出投影，该投影将分组列和最终聚合结果缓冲区作为输入。
      // 因此，我们必须重写结果表达式，使其属性与最终结果投影输入行的属性相匹配：
      // The original `resultExpressions` are a set of expressions which may reference
      // aggregate expressions, grouping column values, and constants. When aggregate operator
      // emits output rows, we will use `resultExpressions` to generate an output projection
      // which takes the grouping columns and final aggregate result buffer as input.
      // Thus, we must re-write the result expressions so that their attributes match up with
      // the attributes of the final result projection's input row:
      val rewrittenResultExpressions = resultExpressions.map { expr =>
        expr.transformDown {
          case ae: AggregateExpression =>
            // 最终聚合缓冲区的属性将是“finalAggregationAttributes”，
            // 因此，用集合中的相应属性替换每个聚合表达式：
            // The final aggregation buffer's attributes will be `finalAggregationAttributes`,
            // so replace each aggregate expression by its corresponding attribute in the set:
            equivalentAggregateExpressions.getEquivalentExprs(ae).headOption
              .getOrElse(ae).asInstanceOf[AggregateExpression].resultAttribute
          case expression if !expression.foldable =>
            // Since we're using `namedGroupingAttributes` to extract the grouping key
            // columns, we need to replace grouping key expressions with their corresponding
            // attributes. We do not rely on the equality check at here since attributes may
            // differ cosmetically. Instead, we use semanticEquals.
            groupExpressionMap.collectFirst {
              case (expr, ne) if expr semanticEquals expression => ne.toAttribute
            }.getOrElse(expression)
        }.asInstanceOf[NamedExpression]
      }

      Some((
        namedGroupingExpressions.map(_._2),
        aggregateExpressions,
        rewrittenResultExpressions,
        child))

    case _ => None
  }
}
