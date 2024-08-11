package com.lk.setl.sql.catalyst.expressions.codegen

import com.lk.setl.sql.catalyst.expressions._

/**
 * Generates bytecode that evaluates a boolean [[Expression]] on a given input [[Row]].
 */
object GeneratePredicate extends CodeGenerator[Expression, BasePredicate] {

  protected def canonicalize(in: Expression): Expression =
    //ExpressionCanonicalizer.execute(in)
    in

  protected def bind(in: Expression, inputSchema: Seq[Attribute]): Expression =
    BindReferences.bindReference(in, inputSchema)

  def generate(expressions: Expression, useSubexprElimination: Boolean): BasePredicate =
    create(canonicalize(expressions), useSubexprElimination)

  protected def create(predicate: Expression): BasePredicate = create(predicate, false)

  protected def create(predicate: Expression, useSubexprElimination: Boolean): BasePredicate = {
    val ctx = newCodeGenContext()

    // Do sub-expression elimination for predicates.
    val eval = ctx.generateExpressions(Seq(predicate), useSubexprElimination).head
    val evalSubexpr = ctx.subexprFunctionsCode

    val codeBody = s"""
      public SpecificPredicate generate(Object[] references) {
        return new SpecificPredicate(references);
      }

      class SpecificPredicate extends ${classOf[BasePredicate].getName} {
        private final Object[] references;
        ${ctx.declareMutableStates()}

        public SpecificPredicate(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
        }

        public void initialize(int partitionIndex) {
          ${ctx.initPartition()}
        }

        public boolean eval(Row ${ctx.INPUT_ROW}) {
          $evalSubexpr
          ${eval.code}
          return !${eval.isNull} && ${eval.value};
        }

        ${ctx.declareAddedFunctions()}
      }"""

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"Generated predicate '$predicate':\n${CodeFormatter.format(code)}")

    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(ctx.references.toArray).asInstanceOf[BasePredicate]
  }
}
