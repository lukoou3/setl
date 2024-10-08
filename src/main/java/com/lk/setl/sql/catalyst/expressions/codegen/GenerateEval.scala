package com.lk.setl.sql.catalyst.expressions.codegen

import com.lk.setl.sql.catalyst.expressions.{Attribute, BaseEval, BindReferences, Expression}

object GenerateEval extends CodeGenerator[Expression, BaseEval] {
  protected def canonicalize(in: Expression): Expression =
    ExpressionCanonicalizer.execute(in)

  protected def bind(in: Expression, inputSchema: Seq[Attribute]): Expression =
    BindReferences.bindReference(in, inputSchema)

  def generate(expression: Expression, useSubexprElimination: Boolean): BaseEval =
    create(canonicalize(expression), useSubexprElimination)

  protected def create(predicate: Expression): BaseEval = create(predicate, false)

  protected def create(e: Expression, useSubexprElimination: Boolean): BaseEval = {
    val ctx = newCodeGenContext()
    // Do sub-expression elimination for predicates.
    val eval = ctx.generateExpressions(Seq(e), useSubexprElimination).head
    val evalSubexpr = ctx.subexprFunctionsCode

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
          $evalSubexpr
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
