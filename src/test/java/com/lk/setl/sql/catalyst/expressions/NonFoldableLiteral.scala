package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.Row
import com.lk.setl.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import com.lk.setl.sql.types.DataType


/**
 * A literal value that is not foldable. Used in expression codegen testing to test code path
 * that behave differently based on foldable values.
 */
case class NonFoldableLiteral(value: Any, dataType: DataType) extends LeafExpression {

  override def foldable: Boolean = false
  override def nullable: Boolean = true

  override def toString: String = if (value != null) value.toString else "null"

  override def eval(input: Row): Any = value

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    Literal.create(value, dataType).doGenCode(ctx, ev)
  }
}


object NonFoldableLiteral {
  def apply(value: Any): NonFoldableLiteral = {
    val lit = Literal(value)
    NonFoldableLiteral(lit.value, lit.dataType)
  }
  def create(value: Any, dataType: DataType): NonFoldableLiteral = {
    val lit = Literal.create(value, dataType)
    NonFoldableLiteral(lit.value, lit.dataType)
  }
}
