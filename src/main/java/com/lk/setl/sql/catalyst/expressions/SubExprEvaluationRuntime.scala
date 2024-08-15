package com.lk.setl.sql.catalyst.expressions

import java.util.IdentityHashMap

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.common.util.concurrent.{ExecutionError, UncheckedExecutionException}

import com.lk.setl.sql.Row
import com.lk.setl.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import com.lk.setl.sql.types.DataType

/**
 * This class helps subexpression elimination for interpreted evaluation
 * such as `InterpretedUnsafeProjection`. It maintains an evaluation cache.
 * This class wraps `ExpressionProxy` around given expressions. The `ExpressionProxy`
 * intercepts expression evaluation and loads from the cache first.
 */
class SubExprEvaluationRuntime(cacheMaxEntries: Int) {
  // The id assigned to `ExpressionProxy`. `SubExprEvaluationRuntime` will use assigned ids of
  // `ExpressionProxy` to decide the equality when loading from cache. `SubExprEvaluationRuntime`
  // won't be use by multi-threads so we don't need to consider concurrency here.
  private var proxyExpressionCurrentId = 0

  private[sql] val cache: LoadingCache[ExpressionProxy, ResultProxy] = CacheBuilder.newBuilder()
    .maximumSize(cacheMaxEntries)
    .build(
      new CacheLoader[ExpressionProxy, ResultProxy]() {
        override def load(expr: ExpressionProxy): ResultProxy = {
          ResultProxy(expr.proxyEval(currentInput))
        }
      })

  private var currentInput: Row = null

  def getEval(proxy: ExpressionProxy): Any = try {
    cache.get(proxy).result
  } catch {
    // Cache.get() may wrap the original exception. See the following URL
    // http://google.github.io/guava/releases/14.0/api/docs/com/google/common/cache/
    //   Cache.html#get(K,%20java.util.concurrent.Callable)
    case e @ (_: UncheckedExecutionException | _: ExecutionError) =>
      throw e.getCause
  }

  /**
   * Sets given input row as current row for evaluating expressions. This cleans up the cache
   * too as new input comes.
   */
  def setInput(input: Row = null): Unit = {
    currentInput = input
    cache.invalidateAll()
  }

  /**
   * Recursively replaces expression with its proxy expression in `proxyMap`.
   */
  private def replaceWithProxy(
      expr: Expression,
      proxyMap: IdentityHashMap[Expression, ExpressionProxy]): Expression = {
    if (proxyMap.containsKey(expr)) {
      proxyMap.get(expr)
    } else {
      expr.mapChildren(replaceWithProxy(_, proxyMap))
    }
  }

  /**
   * Finds subexpressions and wraps them with `ExpressionProxy`.
   */
  def proxyExpressions(expressions: Seq[Expression]): Seq[Expression] = {
    val equivalentExpressions: EquivalentExpressions = new EquivalentExpressions

    expressions.foreach(equivalentExpressions.addExprTree(_))

    val proxyMap = new IdentityHashMap[Expression, ExpressionProxy]

    val commonExprs = equivalentExpressions.getAllEquivalentExprs.filter(_.size > 1)
    commonExprs.foreach { e =>
      val expr = e.head
      val proxy = ExpressionProxy(expr, proxyExpressionCurrentId, this)
      proxyExpressionCurrentId += 1

      // We leverage `IdentityHashMap` so we compare expression keys by reference here.
      // So for example if there are one group of common exprs like Seq(common expr 1,
      // common expr2, ..., common expr n), we will insert into `proxyMap` some key/value
      // pairs like Map(common expr 1 -> proxy(common expr 1), ...,
      // common expr n -> proxy(common expr 1)).
      e.map(proxyMap.put(_, proxy))
    }

    // Only adding proxy if we find subexpressions.
    if (!proxyMap.isEmpty) {
      expressions.map(replaceWithProxy(_, proxyMap))
    } else {
      expressions
    }
  }
}

/**
 * A proxy for an catalyst `Expression`. Given a runtime object `SubExprEvaluationRuntime`,
 * when this is asked to evaluate, it will load from the evaluation cache in the runtime first.
 */
case class ExpressionProxy(
    child: Expression,
    id: Int,
    runtime: SubExprEvaluationRuntime) extends Expression {

  final override def dataType: DataType = child.dataType
  final override def nullable: Boolean = child.nullable
  final override def children: Seq[Expression] = child :: Nil

  // `ExpressionProxy` is for interpreted expression evaluation only. So cannot `doGenCode`.
  final override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new UnsupportedOperationException(s"Cannot generate code for expression: $this")

  def proxyEval(input: Row = null): Any = child.eval(input)

  override def eval(input: Row = null): Any = runtime.getEval(this)

  override def equals(obj: Any): Boolean = obj match {
    case other: ExpressionProxy => this.id == other.id
    case _ => false
  }

  override def hashCode(): Int = this.id.hashCode()
}

/**
 * A simple wrapper for holding `Any` in the cache of `SubExprEvaluationRuntime`.
 */
case class ResultProxy(result: Any)
