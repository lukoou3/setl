package com.lk.setl.sql.catalyst.plans.logical

import com.lk.setl.Logging
import com.lk.setl.sql.AnalysisException
import com.lk.setl.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import com.lk.setl.sql.catalyst.expressions._
import com.lk.setl.sql.catalyst.plans.QueryPlan
import com.lk.setl.sql.types.StructType

abstract class LogicalPlan extends QueryPlan[LogicalPlan] with AnalysisHelper with QueryPlanConstraints with Logging {


  /**
   * Returns true if this expression and all its children have been resolved to a specific schema
   * and false if it still contains any unresolved placeholders. Implementations of LogicalPlan
   * can override this (e.g.
   * [[com.lk.setl.sql.catalyst.analysis.UnresolvedRelation UnresolvedRelation]]
   * should return `false`).
   */
  lazy val resolved: Boolean = expressions.forall(_.resolved) && childrenResolved

  override protected def statePrefix = if (!resolved) "'" else super.statePrefix

  /**
   * Returns true if all its children of this query plan have been resolved.
   */
  def childrenResolved: Boolean = children.forall(_.resolved)

  /**
   * Resolves a given schema to concrete [[Attribute]] references in this query plan. This function
   * should only be called on analyzed plans since it will throw [[AnalysisException]] for
   * unresolved [[Attribute]]s.
   */
  def resolve(schema: StructType, resolver: Resolver): Seq[Attribute] = {
    schema.map { field =>
      resolve(field.name :: Nil, resolver).map {
        case a: AttributeReference => a
        case _ => sys.error(s"can not handle nested schema yet...  plan $this")
      }.getOrElse {
        throw new AnalysisException(
          s"Unable to resolve ${field.name} given [${output.map(_.name).mkString(", ")}]")
      }
    }
  }

  private[this] lazy val childAttributes = AttributeSeq(children.flatMap(_.output))

  private[this] lazy val outputAttributes = AttributeSeq(output)

  /**
   * Optionally resolves the given strings to a [[NamedExpression]] using the input from all child
   * nodes of this LogicalPlan. The attribute is expressed as
   * string in the following form: `[scope].AttributeName.[nested].[fields]...`.
   */
  def resolveChildren(
      nameParts: Seq[String],
      resolver: Resolver): Option[NamedExpression] =
    childAttributes.resolve(nameParts, resolver)

  /**
   * Optionally resolves the given strings to a [[NamedExpression]] based on the output of this
   * LogicalPlan. The attribute is expressed as string in the following form:
   * `[scope].AttributeName.[nested].[fields]...`.
   */
  def resolve(
      nameParts: Seq[String],
      resolver: Resolver): Option[NamedExpression] =
    outputAttributes.resolve(nameParts, resolver)

  /**
   * Given an attribute name, split it to name parts by dot, but
   * don't split the name parts quoted by backticks, for example,
   * `ab.cd`.`efg` should be split into two parts "ab.cd" and "efg".
   */
  def resolveQuoted(
      name: String,
      resolver: Resolver): Option[NamedExpression] = {
    resolve(UnresolvedAttribute.parseAttributeName(name), resolver)
  }

  /**
   * Returns the output ordering that this plan generates.
   */
  def outputOrdering: Seq[SortOrder] = Nil

  /**
   * Returns true iff `other`'s output is semantically the same, ie.:
   *  - it contains the same number of `Attribute`s;
   *  - references are the same;
   *  - the order is equal too.
   */
  def sameOutput(other: LogicalPlan): Boolean = {
    val thisOutput = this.output
    val otherOutput = other.output
    thisOutput.length == otherOutput.length && thisOutput.zip(otherOutput).forall {
      case (a1, a2) => a1.semanticEquals(a2)
    }
  }

}

/**
 * A logical plan node with no children.
 */
abstract class LeafNode extends LogicalPlan {
  override final def children: Seq[LogicalPlan] = Nil
  override def producedAttributes: AttributeSet = outputSet

}

/**
 * A logical plan node with single child.
 */
abstract class UnaryNode extends LogicalPlan {
  def child: LogicalPlan

  override final def children: Seq[LogicalPlan] = child :: Nil

  /**
   * Generates all valid constraints including an set of aliased constraints by replacing the
   * original constraint expressions with the corresponding alias
   */
  protected def getAllValidConstraints(projectList: Seq[NamedExpression]): ExpressionSet = {
    var allConstraints = child.constraints
    projectList.foreach {
      case a @ Alias(l: Literal, _) =>
        allConstraints += EqualNullSafe(a.toAttribute, l)
      case a @ Alias(e, _) =>
        // For every alias in `projectList`, replace the reference in constraints by its attribute.
        allConstraints ++= allConstraints.map(_ transform {
          case expr: Expression if expr.semanticEquals(e) =>
            a.toAttribute
        })
        allConstraints += EqualNullSafe(e, a.toAttribute)
      case _ => // Don't change.
    }

    allConstraints
  }

  override protected lazy val validConstraints: ExpressionSet = child.constraints
}

/**
 * A logical plan node with a left and right child.
 */
abstract class BinaryNode extends LogicalPlan {
  def left: LogicalPlan
  def right: LogicalPlan

  override final def children: Seq[LogicalPlan] = Seq(left, right)
}

abstract class OrderPreservingUnaryNode extends UnaryNode {
  override final def outputOrdering: Seq[SortOrder] = child.outputOrdering
}

case class RelationPlaceholder(output: Seq[AttributeReference]) extends LeafNode