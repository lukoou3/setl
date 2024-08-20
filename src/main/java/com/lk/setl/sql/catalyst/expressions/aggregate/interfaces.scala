package com.lk.setl.sql.catalyst.expressions.aggregate

import com.lk.setl.sql.Row
import com.lk.setl.sql.catalyst.analysis.UnresolvedAttribute
import com.lk.setl.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import com.lk.setl.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, Expression, Literal, NamedExpression, Unevaluable}
import com.lk.setl.sql.types._

/** The mode of an [[AggregateFunction]]. */
sealed trait AggregateMode

/**
 * An [[AggregateFunction]] with [[Partial]] mode is used for partial aggregation.
 * This function updates the given aggregation buffer with the original input of this
 * function. When it has processed all input rows, the aggregation buffer is returned.
 */
case object Partial extends AggregateMode

/**
 * An [[AggregateFunction]] with [[PartialMerge]] mode is used to merge aggregation buffers
 * containing intermediate results for this function.
 * This function updates the given aggregation buffer by merging multiple aggregation buffers.
 * When it has processed all input rows, the aggregation buffer is returned.
 */
case object PartialMerge extends AggregateMode

/**
 * An [[AggregateFunction]] with [[Final]] mode is used to merge aggregation buffers
 * containing intermediate results for this function and then generate final result.
 * This function updates the given aggregation buffer by merging multiple aggregation buffers.
 * When it has processed all input rows, the final result of this function is returned.
 */
case object Final extends AggregateMode

/**
 * An [[AggregateFunction]] with [[Complete]] mode is used to evaluate this function directly
 * from original input rows without any partial aggregation.
 * This function updates the given aggregation buffer with the original input of this
 * function. When it has processed all input rows, the final result of this function is returned.
 */
case object Complete extends AggregateMode

/**
 * A place holder expressions used in code-gen, it does not change the corresponding value
 * in the row.
 */
case object NoOp extends Expression with Unevaluable {
  override def nullable: Boolean = true
  override def dataType: DataType = NullType
  override def children: Seq[Expression] = Nil
}

object AggregateExpression {
  def apply(
      aggregateFunction: AggregateFunction,
      mode: AggregateMode,
      isDistinct: Boolean,
      filter: Option[Expression] = None): AggregateExpression = {
    AggregateExpression(
      aggregateFunction,
      mode,
      isDistinct,
      filter,
      NamedExpression.newExprId)
  }
}

/**
 * A container for an [[AggregateFunction]] with its [[AggregateMode]] and a field
 * (`isDistinct`) indicating if DISTINCT keyword is specified for this function and
 * a field (`filter`) indicating if filter clause is specified for this function.
 */
case class AggregateExpression(
    aggregateFunction: AggregateFunction,
    mode: AggregateMode,
    isDistinct: Boolean,
    filter: Option[Expression],
    resultId: Long)
  extends Expression
  with Unevaluable {

  @transient
  lazy val resultAttribute: Attribute = if (aggregateFunction.resolved) {
    AttributeReference(
      aggregateFunction.toString,
      aggregateFunction.dataType,
      aggregateFunction.nullable)(exprId = resultId)
  } else {
    // This is a bit of a hack.  Really we should not be constructing this container and reasoning
    // about datatypes / aggregation mode until after we have finished analysis and made it to
    // planning.
    UnresolvedAttribute(aggregateFunction.toString)
  }

  def filterAttributes: AttributeSet = filter.map(_.references).getOrElse(AttributeSet.empty)

  // We compute the same thing regardless of our final result.
  override lazy val canonicalized: Expression = {
    val normalizedAggFunc = mode match {
      // For PartialMerge or Final mode, the input to the `aggregateFunction` is aggregate buffers,
      // and the actual children of `aggregateFunction` is not used, here we normalize the expr id.
      case PartialMerge | Final => aggregateFunction.transform {
        case a: AttributeReference => a.withExprId(0)
      }
      case Partial | Complete => aggregateFunction
    }

    AggregateExpression(
      normalizedAggFunc.canonicalized.asInstanceOf[AggregateFunction],
      mode,
      isDistinct,
      filter.map(_.canonicalized),
      0)
  }

  override def children: Seq[Expression] = aggregateFunction +: filter.toSeq

  override def dataType: DataType = aggregateFunction.dataType
  override def nullable: Boolean = aggregateFunction.nullable

  @transient
  override lazy val references: AttributeSet = {
    val aggAttributes = mode match {
      case Partial | Complete => aggregateFunction.references
      case PartialMerge | Final => AttributeSet(aggregateFunction.inputAggBufferAttributes)
    }
    aggAttributes ++ filterAttributes
  }

  override def toString: String = {
    val prefix = mode match {
      case Partial => "partial_"
      case PartialMerge => "merge_"
      case Final | Complete => ""
    }
    val aggFuncStr = prefix + aggregateFunction.toAggString(isDistinct)
    filter match {
      case Some(predicate) => s"$aggFuncStr FILTER (WHERE $predicate)"
      case _ => aggFuncStr
    }
  }

  override def sql: String = {
    val aggFuncStr = aggregateFunction.sql(isDistinct)
    filter match {
      case Some(predicate) => s"$aggFuncStr FILTER (WHERE ${predicate.sql})"
      case _ => aggFuncStr
    }
  }
}

/**
 * AggregateFunction is the superclass of two aggregation function interfaces:
 *
 *  - [[ImperativeAggregate]] is for aggregation functions that are specified in terms of
 *    initialize(), update(), and merge() functions that operate on Row-based aggregation buffers.
 *  - [[DeclarativeAggregate]] is for aggregation functions that are specified using
 *    Catalyst expressions.
 *
 * In both interfaces, aggregates must define the schema ([[aggBufferSchema]]) and attributes
 * ([[aggBufferAttributes]]) of an aggregation buffer which is used to hold partial aggregate
 * results. At runtime, multiple aggregate functions are evaluated by the same operator using a
 * combined aggregation buffer which concatenates the aggregation buffers of the individual
 * aggregate functions. Please note that aggregate functions should be stateless.
 *
 * Code which accepts [[AggregateFunction]] instances should be prepared to handle both types of
 * aggregate functions.
 */
abstract class AggregateFunction extends Expression {

  /** An aggregate function is not foldable. */
  final override def foldable: Boolean = false

  /** The schema of the aggregation buffer. */
  def aggBufferSchema: StructType

  /** Attributes of fields in aggBufferSchema. */
  def aggBufferAttributes: Seq[AttributeReference]

  /**
   * Attributes of fields in input aggregation buffers (immutable aggregation buffers that are
   * merged with mutable aggregation buffers in the merge() function or merge expressions).
   * These attributes are created automatically by cloning the [[aggBufferAttributes]].
   */
  def inputAggBufferAttributes: Seq[AttributeReference]

  /**
   * Result of the aggregate function when the input is empty.
   */
  def defaultResult: Option[Literal] = None

  /**
   * Creates [[AggregateExpression]] with `isDistinct` flag disabled.
   *
   * @see `toAggregateExpression(isDistinct: Boolean)` for detailed description
   */
  def toAggregateExpression(): AggregateExpression = toAggregateExpression(isDistinct = false)

  /**
   * Wraps this [[AggregateFunction]] in an [[AggregateExpression]] and sets `isDistinct`
   * flag of the [[AggregateExpression]] to the given value because
   * [[AggregateExpression]] is the container of an [[AggregateFunction]], aggregation mode,
   * and the flag indicating if this aggregation is distinct aggregation or not.
   * An [[AggregateFunction]] should not be used without being wrapped in
   * an [[AggregateExpression]].
   */
  def toAggregateExpression(
      isDistinct: Boolean,
      filter: Option[Expression] = None): AggregateExpression = {
    AggregateExpression(
      aggregateFunction = this,
      mode = Complete,
      isDistinct = isDistinct,
      filter = filter)
  }

  def sql(isDistinct: Boolean): String = {
    val distinct = if (isDistinct) "DISTINCT " else ""
    s"$prettyName($distinct${children.map(_.sql).mkString(", ")})"
  }

  /** String representation used in explain plans. */
  def toAggString(isDistinct: Boolean): String = {
    val start = if (isDistinct) "(distinct " else "("
    prettyName + flatArguments.mkString(start, ", ", ")")
  }
}

/**
 * API for aggregation functions that are expressed in terms of Catalyst expressions.
 *
 * When implementing a new expression-based aggregate function, start by implementing
 * `bufferAttributes`, defining attributes for the fields of the mutable aggregation buffer. You
 * can then use these attributes when defining `updateExpressions`, `mergeExpressions`, and
 * `evaluateExpressions`.
 *
 * Please note that children of an aggregate function can be unresolved (it will happen when
 * we create this function in DataFrame API). So, if there is any fields in
 * the implemented class that need to access fields of its children, please make
 * those fields `lazy val`s.
 */
abstract class DeclarativeAggregate
  extends AggregateFunction
    with Serializable {

  /**
   * Expressions for initializing empty aggregation buffers.
   */
  val initialValues: Seq[Expression]

  /**
   * Expressions for updating the mutable aggregation buffer based on an input row.
   */
  val updateExpressions: Seq[Expression]

  /**
   * A sequence of expressions for merging two aggregation buffers together. When defining these
   * expressions, you can use the syntax `attributeName.left` and `attributeName.right` to refer
   * to the attributes corresponding to each of the buffers being merged (this magic is enabled
   * by the [[RichAttribute]] implicit class).
   */
  val mergeExpressions: Seq[Expression]

  /**
   * An expression which returns the final value for this aggregate function. Its data type should
   * match this expression's [[dataType]].
   */
  val evaluateExpression: Expression

  /** An expression-based aggregate's bufferSchema is derived from bufferAttributes. */
  final override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  final lazy val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  /**
   * A helper class for representing an attribute used in merging two
   * aggregation buffers. When merging two buffers, `bufferLeft` and `bufferRight`,
   * we merge buffer values and then update bufferLeft. A [[RichAttribute]]
   * of an [[AttributeReference]] `a` has two functions `left` and `right`,
   * which represent `a` in `bufferLeft` and `bufferRight`, respectively.
   */
  implicit class RichAttribute(a: AttributeReference) {
    /** Represents this attribute at the mutable buffer side. */
    def left: AttributeReference = a

    /** Represents this attribute at the input buffer side (the data value is read-only). */
    def right: AttributeReference = inputAggBufferAttributes(aggBufferAttributes.indexOf(a))
  }

  final override def eval(input: Row = null): Any =
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")

  final override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new UnsupportedOperationException(s"Cannot generate code for expression: $this")
}