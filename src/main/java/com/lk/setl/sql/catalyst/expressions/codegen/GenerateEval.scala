package com.lk.setl.sql.catalyst.expressions.codegen

import com.lk.setl.sql.Row
import com.lk.setl.sql.catalyst.expressions.{Attribute, BindReferences, Expression}

abstract class BaseEval {
  def eval(r: Row): Object

  def initialize(partitionIndex: Int): Unit = {}
}

object GenerateEval extends CodeGenerator[Expression, BaseEval] {
  protected def canonicalize(in: Expression): Expression =
    ExpressionCanonicalizer.execute(in)

  protected def bind(in: Expression, inputSchema: Seq[Attribute]): Expression =
    BindReferences.bindReference(in, inputSchema)

  override protected def create(e: Expression): BaseEval = {
    val ctx = newCodeGenContext()
    val eval = e.genCode(ctx)

    val codeBody = s"""
      public SpecificEval generate(Object[] references) {
        return new SpecificEval(references);
      }

      class SpecificEval extends ${classOf[BaseEval].getName} {
        private final Object[] references;
        ${ctx.declareMutableStates()}

        public SpecificEval(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
        }

        public void initialize(int partitionIndex) {
          ${ctx.initPartition()}
        }

        public Object eval(Row ${ctx.INPUT_ROW}) {
          ${eval.code}
          return ${eval.isNull}? null : ${eval.value};
        }

        ${ctx.declareAddedFunctions()}
      }"""

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"Generated e '$e':\n${CodeFormatter.format(code)}")

    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(ctx.references.toArray).asInstanceOf[BaseEval]
  }

}
