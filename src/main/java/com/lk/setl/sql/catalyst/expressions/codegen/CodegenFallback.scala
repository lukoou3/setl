package com.lk.setl.sql.catalyst.expressions.codegen

import com.lk.setl.sql.catalyst.expressions.codegen.Block.BlockHelper
import com.lk.setl.sql.catalyst.expressions.{Expression, LeafExpression, Nondeterministic}

/**
 * A trait that can be used to provide a fallback mode for expression code generation.
 */
trait CodegenFallback extends Expression {

  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // LeafNode does not need `input`
    val input = if (this.isInstanceOf[LeafExpression]) "null" else ctx.INPUT_ROW
    val idx = ctx.references.length
    ctx.references += this
    var childIndex = idx
    this.foreach {
      case n: Nondeterministic =>
        // 这可能会将当前表达式添加两次，但不会造成伤害。
        // This might add the current expression twice, but it won't hurt.
        ctx.references += n
        childIndex += 1
        ctx.addPartitionInitializationStatement(
          s"""
             |((Nondeterministic) references[$childIndex])
             |  .initialize(partitionIndex);
          """.stripMargin)
      case _ =>
    }
    val objectTerm = ctx.freshName("obj")
    val placeHolder = ctx.registerComment(this.toString)
    val javaType = CodeGenerator.javaType(this.dataType)
    ev.copy(code = code"""
        $placeHolder
        Object $objectTerm = ((Expression) references[$idx]).eval($input);
        boolean ${ev.isNull} = $objectTerm == null;
        $javaType ${ev.value} = ${CodeGenerator.defaultValue(this.dataType)};
        if (!${ev.isNull}) {
          ${ev.value} = (${CodeGenerator.boxedType(this.dataType)}) $objectTerm;
        }""")
  }
}
