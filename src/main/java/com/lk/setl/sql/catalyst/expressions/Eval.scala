package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.Row
import com.lk.setl.sql.catalyst.expressions.BindReferences.bindReference
import com.lk.setl.sql.catalyst.expressions.codegen.GenerateEval

abstract class BaseEval {
  def eval(r: Row): Object

  def initialize(partitionIndex: Int): Unit = {}
}

case class InterpretedEval(expression: Expression) extends BaseEval {
  private[this] val subExprEliminationEnabled = false
  private[this] lazy val runtime = new SubExprEvaluationRuntime(100)
  private[this] val expr = if (subExprEliminationEnabled) {
    runtime.proxyExpressions(Seq(expression)).head
  } else {
    expression
  }

  override def eval(r: Row): Object = {
    if (subExprEliminationEnabled) {
      runtime.setInput(r)
    }

    expr.eval(r).asInstanceOf[Object]
  }

  override def initialize(partitionIndex: Int): Unit = {
    super.initialize(partitionIndex)
    expr.foreach {
      case n: Nondeterministic => n.initialize(partitionIndex)
      case _ =>
    }
  }
}

object Eval extends CodeGeneratorWithInterpretedFallback[Expression, BaseEval] {

  override protected def createCodeGeneratedObject(in: Expression): BaseEval = {
    GenerateEval.generate(in)
  }

  override protected def createInterpretedObject(in: Expression): BaseEval = {
    InterpretedEval(in)
  }

  def createInterpreted(e: Expression): InterpretedPredicate = InterpretedPredicate(e)

  /**
   * Returns a BaseEval for an Expression, which will be bound to `inputSchema`.
   */
  def create(e: Expression, inputSchema: Seq[Attribute]): BaseEval = {
    createObject(bindReference(e, inputSchema))
  }

  /**
   * Returns a BaseEval for a given bound Expression.
   */
  def create(e: Expression): BaseEval = {
    createObject(e)
  }
}
