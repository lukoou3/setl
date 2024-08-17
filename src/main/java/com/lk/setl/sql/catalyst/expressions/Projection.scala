package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.{GenericRow, Row}
import com.lk.setl.sql.catalyst.expressions.BindReferences.bindReferences
import com.lk.setl.sql.catalyst.expressions.codegen.GenerateSafeProjection
import com.lk.setl.sql.types.{DataType, StructType}

/**
 * A [[Projection]] that is calculated by calling the `eval` of each of the specified expressions.
 * spark中UserDefinedGenerator中使用
 *
 * @param expressions a sequence of expressions that determine the value of each column of the
 *                    output row.
 */
class InterpretedProjection(expressions: Seq[Expression]) extends Projection {
  def this(expressions: Seq[Expression], inputSchema: Seq[Attribute]) =
    this(bindReferences(expressions, inputSchema))

  override def initialize(partitionIndex: Int): Unit = {
    expressions.foreach(_.foreach {
      case n: Nondeterministic => n.initialize(partitionIndex)
      case _ =>
    })
  }

  // null check is required for when Kryo invokes the no-arg constructor.
  protected val exprArray = if (expressions != null) expressions.toArray else null

  def apply(input: Row): Row = {
    val outputArray = new Array[Any](exprArray.length)
    var i = 0
    while (i < exprArray.length) {
      outputArray(i) = exprArray(i).eval(input)
      i += 1
    }
    new GenericRow(outputArray)
  }

  override def toString(): String = s"Row => [${exprArray.mkString(",")}]"
}

/**
 * A projection that could turn UnsafeRow into GenericInternalRow
 */
object SafeProjection extends CodeGeneratorWithInterpretedFallback[Seq[Expression], Projection] {

  override protected def createCodeGeneratedObject(in: Seq[Expression]): Projection = {
    GenerateSafeProjection.generate(in)
  }

  override protected def createInterpretedObject(in: Seq[Expression]): Projection = {
    InterpretedSafeProjection.createProjection(in)
  }

  /**
   * Returns a SafeProjection for given StructType.
   */
  def create(schema: StructType): Projection = create(schema.fields.map(_.dataType))

  /**
   * Returns a SafeProjection for given Array of DataTypes.
   */
  def create(fields: Array[DataType]): Projection = {
    createObject(fields.zipWithIndex.map(x => new BoundReference(x._2, x._1, true)))
  }

  /**
   * Returns a SafeProjection for given sequence of Expressions (bounded).
   */
  def create(exprs: Seq[Expression]): Projection = {
    createObject(exprs)
  }

  /**
   * Returns a SafeProjection for given sequence of Expressions, which will be bound to
   * `inputSchema`.
   */
  def create(exprs: Seq[Expression], inputSchema: Seq[Attribute]): Projection = {
    create(bindReferences(exprs, inputSchema))
  }
}