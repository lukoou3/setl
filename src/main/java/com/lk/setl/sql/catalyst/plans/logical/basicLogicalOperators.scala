package com.lk.setl.sql.catalyst.plans.logical

import com.lk.setl.sql.catalyst.expressions._
import com.lk.setl.sql.catalyst.expressions.aggregate.AggregateExpression

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

case class Filter(condition: Expression, child: LogicalPlan)
  extends OrderPreservingUnaryNode with PredicateHelper {
  override def output: Seq[Attribute] = child.output

  override protected lazy val validConstraints: ExpressionSet = {
    val predicates = splitConjunctivePredicates(condition)
      //.filterNot(SubqueryExpression.hasCorrelatedSubquery)
    child.constraints.union(ExpressionSet(predicates))
  }
}

case class Expr(expression: NamedExpression, child: LogicalPlan) extends OrderPreservingUnaryNode{
  override def output: Seq[Attribute] = Seq(expression.toAttribute)

  override protected lazy val validConstraints: ExpressionSet = getAllValidConstraints(Seq(expression))
}

/**
 * Applies a [[Generator]] to a stream of input rows, combining the
 * output of each into a new stream of rows.  This operation is similar to a `flatMap` in functional
 * programming with one important additional feature, which allows the input rows to be joined with
 * their output.
 *
 * @param generator the generator expression
 * @param unrequiredChildIndex this parameter starts as Nil and gets filled by the Optimizer.
 *                             It's used as an optimization for omitting data generation that will
 *                             be discarded next by a projection.
 *                             A common use case is when we explode(array(..)) and are interested
 *                             only in the exploded data and not in the original array. before this
 *                             optimization the array got duplicated for each of its elements,
 *                             causing O(n^^2) memory consumption. (see [SPARK-21657])
 * @param outer when true, each input row will be output at least once, even if the output of the
 *              given `generator` is empty.
 * @param qualifier Qualifier for the attributes of generator(UDTF)
 * @param generatorOutput The output schema of the Generator.
 * @param child Children logical plan node
 */
case class Generate(
    generator: Generator,
    unrequiredChildIndex: Seq[Int],
    outer: Boolean,
    qualifier: Option[String],
    generatorOutput: Seq[Attribute],
    child: LogicalPlan)
  extends UnaryNode {
  lazy val requiredChildOutput: Seq[Attribute] = {
    val unrequiredSet = unrequiredChildIndex.toSet
    child.output.zipWithIndex.filterNot(t => unrequiredSet.contains(t._2)).map(_._1)
  }

  override lazy val resolved: Boolean = {
    generator.resolved &&
      childrenResolved &&
      generator.elementSchema.length == generatorOutput.length &&
      generatorOutput.forall(_.resolved)
  }

  override def producedAttributes: AttributeSet = AttributeSet(generatorOutput)

  // 给输出添加表前缀，如果定义table_alias
  def qualifiedGeneratorOutput: Seq[Attribute] = {
    val qualifiedOutput = qualifier.map { q =>
      // 是否定义table_alias
      // prepend the new qualifier to the existed one
      generatorOutput.map(a => a.withQualifier(Seq(q)))
    }.getOrElse(generatorOutput)
    val nullableOutput = qualifiedOutput.map {
      // if outer, make all attributes nullable, otherwise keep existing nullability
      a => a.withNullability(outer || a.nullable)
    }
    nullableOutput
  }

  def output: Seq[Attribute] = requiredChildOutput ++ qualifiedGeneratorOutput
}

/**
 * This is a Group by operator with the aggregate functions and projections.
 *
 * @param groupingExpressions expressions for grouping keys
 * @param aggregateExpressions expressions for a project list, which could contain
 *                             [[AggregateFunction]]s.
 *
 * Note: Currently, aggregateExpressions is the project list of this Group by operator. Before
 * separating projection from grouping and aggregate, we should avoid expression-level optimization
 * on aggregateExpressions, which could reference an expression in groupingExpressions.
 * For example, see the rule [[org.apache.spark.sql.catalyst.optimizer.SimplifyExtractValueOps]]
 */
case class Aggregate(
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: LogicalPlan)
  extends UnaryNode {

  override lazy val resolved: Boolean = {
    val hasWindowExpressions = false
    !expressions.exists(!_.resolved) && childrenResolved && !hasWindowExpressions
  }

  override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute)

  override lazy val validConstraints: ExpressionSet = {
    val nonAgg = aggregateExpressions.filter(_.find(_.isInstanceOf[AggregateExpression]).isEmpty)
    getAllValidConstraints(nonAgg)
  }
}

case class TimeWindow(windowAssigner: TimeWindowAssigner, windowAttributes: Seq[Attribute], child: LogicalPlan) extends UnaryNode {
  override lazy val resolved: Boolean = childrenResolved && windowAttributes.forall(_.resolved)
  override def producedAttributes: AttributeSet = AttributeSet(windowAttributes)
  override def output: Seq[Attribute] = child.output ++ windowAttributes
}

/**
 * A relation with one row. This is used in "SELECT ..." without a from clause.
 */
case class OneRowRelation() extends LeafNode {
  override def output: Seq[Attribute] = Nil

  override def makeCopy(newArgs: Array[AnyRef]): OneRowRelation = {
    val newCopy = OneRowRelation()
    newCopy.copyTagsFrom(this)
    newCopy
  }
}