package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.Row
import com.lk.setl.sql.catalyst.expressions.codegen.CodegenFallback
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
