package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.{ArrayData, GenericRow, Row}
import com.lk.setl.sql.catalyst.analysis.TypeCheckResult
import com.lk.setl.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import com.lk.setl.sql.types._

/**
 * An expression that produces zero or more rows given a single input row.
 *
 * Generators produce multiple output rows instead of a single value like other expressions,
 * and thus they must have a schema to associate with the rows that are output.
 *
 * However, unlike row producing relational operators, which are either leaves or determine their
 * output schema functionally from their input, generators can contain other expressions that
 * might result in their modification by rules.  This structure means that they might be copied
 * multiple times after first determining their output schema. If a new output schema is created for
 * each copy references up the tree might be rendered invalid. As a result generators must
 * instead define a function `makeOutput` which is called only once when the schema is first
 * requested.  The attributes produced by this function will be automatically copied anytime rules
 * result in changes to the Generator or its children.
 */
trait Generator extends Expression {

  override def dataType: DataType = ArrayType(elementSchema)

  override def foldable: Boolean = false

  override def nullable: Boolean = false

  /**
   * The output element schema.
   */
  def elementSchema: StructType

  /** Should be implemented by child classes to perform specific Generators. */
  override def eval(input: Row): TraversableOnce[Row]

  /**
   * Notifies that there are no more rows to process, clean up code, and additional
   * rows can be made here.
   */
  def terminate(): TraversableOnce[Row] = Nil

  /**
   * Check if this generator supports code generation.
   */
  def supportCodegen: Boolean = !isInstanceOf[CodegenFallback]
}

/**
 * A collection producing [[Generator]]. This trait provides a different path for code generation,
 * by allowing code generation to return either an [[ArrayData]] or a [[MapData]] object.
 */
trait CollectionGenerator extends Generator {
  /** The position of an element within the collection should also be returned. */
  def position: Boolean

  /** Rows will be inlined during generation. */
  def inline: Boolean

  /** The type of the returned collection object. */
  def collectionType: DataType = dataType
}

/**
 * Wrapper around another generator to specify outer behavior. This is used to implement functions
 * such as explode_outer. This expression gets replaced during analysis.
 */
case class GeneratorOuter(child: Generator) extends UnaryExpression with Generator {
  final override def eval(input: Row = null): TraversableOnce[Row] =
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")

  final override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new UnsupportedOperationException(s"Cannot generate code for expression: $this")

  override def elementSchema: StructType = child.elementSchema

  override lazy val resolved: Boolean = false
}

/**
 * A base class for [[Explode]] and [[PosExplode]].
 */
abstract class ExplodeBase extends UnaryExpression with CollectionGenerator with Serializable {
  override val inline: Boolean = false

  override def checkInputDataTypes(): TypeCheckResult = child.dataType match {
    case _: ArrayType  =>
      TypeCheckResult.TypeCheckSuccess
    case _ =>
      TypeCheckResult.TypeCheckFailure(
        "input to function explode should be array or map type, " +
          s"not ${child.dataType.catalogString}")
  }

  // hive-compatible default alias for explode function ("col" for array, "key", "value" for map)
  override def elementSchema: StructType = child.dataType match {
    case ArrayType(et, containsNull) =>
      if (position) {
        new StructType()
          .add("pos", IntegerType)
          .add("col", et)
      } else {
        new StructType()
          .add("col", et)
      }
  }

  override def eval(input: Row): TraversableOnce[Row] = {
    child.dataType match {
      case ArrayType(et, _) =>
        val inputArray = child.eval(input).asInstanceOf[ArrayData]
        if (inputArray == null) {
          Nil
        } else {
          val rows = new Array[Row](inputArray.numElements())
          var i = 0
          while(i < inputArray.numElements()){
            val e = inputArray.get(i)
            rows(i) = if (position) new GenericRow(Array(i, e)) else new GenericRow(Array(e))
            i += 1
          }
          rows
        }
    }
  }

  override def collectionType: DataType = child.dataType

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    child.genCode(ctx)
  }
}

/**
 * Given an input array produces a sequence of rows for each value in the array.
 *
 * {{{
 *   SELECT explode(array(10,20)) ->
 *   10
 *   20
 * }}}
 */
case class Explode(child: Expression) extends ExplodeBase {
  override val position: Boolean = false
}

/**
 * Given an input array produces a sequence of rows for each position and value in the array.
 *
 * {{{
 *   SELECT posexplode(array(10,20)) ->
 *   0  10
 *   1  20
 * }}}
 */
case class PosExplode(child: Expression) extends ExplodeBase {
  override val position = true
}
