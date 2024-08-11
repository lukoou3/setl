package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.catalyst.expressions.codegen.CodegenFallback
import com.lk.setl.sql.catalyst.expressions.objects.LambdaVariable

import scala.collection.mutable

/**
 * 此类用于计算（子）表达式树的等式。表达式可以添加到此类中，随后它们会查询表达式相等性。如果对于相同的输入产生相同的结果，则认为表达式树是相等的。
 * This class is used to compute equality of (sub)expression trees. Expressions can be added
 * to this class and they subsequently query for expression equality. Expression trees are
 * considered equal if for the same input(s), the same result is produced.
 */
class EquivalentExpressions {
  /**
   * Wrapper around an Expression that provides semantic equality.
   */
  case class Expr(e: Expression) {
    override def equals(o: Any): Boolean = o match {
      case other: Expr => e.semanticEquals(other.e)
      case _ => false
    }

    override def hashCode: Int = e.semanticHash()
  }

  // For each expression, the set of equivalent expressions.
  private val equivalenceMap = mutable.HashMap.empty[Expr, mutable.ArrayBuffer[Expression]]

  /**
   * Adds each expression to this data structure, grouping them with existing equivalent
   * expressions. Non-recursive.
   * Returns true if there was already a matching expression.
   */
  def addExpr(expr: Expression): Boolean = {
    if (expr.deterministic) {
      val e: Expr = Expr(expr)
      val f = equivalenceMap.get(e)
      if (f.isDefined) {
        f.get += expr
        true
      } else {
        equivalenceMap.put(e, mutable.ArrayBuffer(expr))
        false
      }
    } else {
      false
    }
  }

  private def addExprToSet(expr: Expression, set: mutable.Set[Expr]): Boolean = {
    if (expr.deterministic) {
      val e = Expr(expr)
      if (set.contains(e)) {
        true
      } else {
        set.add(e)
        false
      }
    } else {
      false
    }
  }

  /**
   * Adds only expressions which are common in each of given expressions, in a recursive way.
   * For example, given two expressions `(a + (b + (c + 1)))` and `(d + (e + (c + 1)))`,
   * the common expression `(c + 1)` will be added into `equivalenceMap`.
   */
  private def addCommonExprs(
      exprs: Seq[Expression],
      addFunc: Expression => Boolean = addExpr): Unit = {
    val exprSetForAll = mutable.Set[Expr]()
    addExprTree(exprs.head, addExprToSet(_, exprSetForAll))

    val commonExprSet = exprs.tail.foldLeft(exprSetForAll) { (exprSet, expr) =>
      val otherExprSet = mutable.Set[Expr]()
      addExprTree(expr, addExprToSet(_, otherExprSet))
      exprSet.intersect(otherExprSet)
    }

    commonExprSet.foreach(expr => addFunc(expr.e))
  }

  // There are some special expressions that we should not recurse into all of its children.
  //   1. CodegenFallback: it's children will not be used to generate code (call eval() instead)
  //   2. If: common subexpressions will always be evaluated at the beginning, but the true and
  //          false expressions in `If` may not get accessed, according to the predicate
  //          expression. We should only recurse into the predicate expression.
  //   3. CaseWhen: like `If`, the children of `CaseWhen` only get accessed in a certain
  //                condition. We should only recurse into the first condition expression as it
  //                will always get accessed.
  //   4. Coalesce: it's also a conditional expression, we should only recurse into the first
  //                children, because others may not get accessed.
  private def childrenToRecurse(expr: Expression): Seq[Expression] = expr match {
    case _: CodegenFallback => Nil
    case i: If => i.predicate :: Nil
    case c: CaseWhen => c.children.head :: Nil
    case c: Coalesce => c.children.head :: Nil
    case other => other.children
  }

  // For some special expressions we cannot just recurse into all of its children, but we can
  // recursively add the common expressions shared between all of its children.
  private def commonChildrenToRecurse(expr: Expression): Seq[Seq[Expression]] = expr match {
    case i: If => Seq(Seq(i.trueValue, i.falseValue))
    case c: CaseWhen =>
      // We look at subexpressions in conditions and values of `CaseWhen` separately. It is
      // because a subexpression in conditions will be run no matter which condition is matched
      // if it is shared among conditions, but it doesn't need to be shared in values. Similarly,
      // a subexpression among values doesn't need to be in conditions because no matter which
      // condition is true, it will be evaluated.
      val conditions = c.branches.tail.map(_._1)
      val values = c.branches.map(_._2) ++ c.elseValue
      Seq(conditions, values)
    case c: Coalesce => Seq(c.children.tail)
    case _ => Nil
  }

  /**
   * 递归地将表达式添加到此数据结构中。如果找到匹配的表达式，则停止。也就是说，如果已经添加了expr，则不会添加其子项。
   * Adds the expression to this data structure recursively. Stops if a matching expression
   * is found. That is, if `expr` has already been added, its children are not added.
   */
  def addExprTree(
      expr: Expression,
      addFunc: Expression => Boolean = addExpr): Unit = {
    val skip = expr.isInstanceOf[LeafExpression] ||
      // `LambdaVariable通常用作循环变量，不能在循环前对其进行求值。因此，我们无法计算开头包含“LambdaVariable”的子表达式。
      // `LambdaVariable` is usually used as a loop variable, which can't be evaluated ahead of the
      // loop. So we can't evaluate sub-expressions containing `LambdaVariable` at the beginning.
      expr.find(_.isInstanceOf[LambdaVariable]).isDefined // ||
      // `PlanExpression` wraps query plan. To compare query plans of `PlanExpression` on executor,
      // can cause error like NPE.
      //(expr.isInstanceOf[PlanExpression[_]] && TaskContext.get != null)
    if (!skip && !addFunc(expr)) {
      childrenToRecurse(expr).foreach(addExprTree(_, addFunc))
      commonChildrenToRecurse(expr).filter(_.nonEmpty).foreach(addCommonExprs(_, addFunc))
    }
  }

  /**
   * Returns all of the expression trees that are equivalent to `e`. Returns
   * an empty collection if there are none.
   */
  def getEquivalentExprs(e: Expression): Seq[Expression] = {
    equivalenceMap.getOrElse(Expr(e), Seq.empty).toSeq
  }

  /**
   * Returns all the equivalent sets of expressions.
   */
  def getAllEquivalentExprs: Seq[Seq[Expression]] = {
    equivalenceMap.values.map(_.toSeq).toSeq
  }

  /**
   * Returns the state of the data structure as a string. If `all` is false, skips sets of
   * equivalent expressions with cardinality 1.
   */
  def debugString(all: Boolean = false): String = {
    val sb: mutable.StringBuilder = new StringBuilder()
    sb.append("Equivalent expressions:\n")
    equivalenceMap.foreach { case (k, v) =>
      if (all || v.length > 1) {
        sb.append("  " + v.mkString(", ")).append("\n")
      }
    }
    sb.toString()
  }
}
