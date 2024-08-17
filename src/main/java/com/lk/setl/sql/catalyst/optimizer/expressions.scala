package com.lk.setl.sql.catalyst.optimizer

import com.lk.setl.sql.catalyst.expressions._
import com.lk.setl.sql.catalyst.plans.logical.LogicalPlan
import com.lk.setl.sql.catalyst.rules.Rule
import com.lk.setl.sql.types.{BooleanType, StringType}

/*
 * Optimization rules defined in this file should not affect the structure of the logical plan.
 */

/**
 * 用等效的Literal值替换可以静态计算的表达式。
 * Replaces [[Expression Expressions]] that can be statically evaluated with
 * equivalent [[Literal]] values.
 */
object ConstantFolding extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsDown {
      // Skip redundant folding of literals. This rule is technically not necessary. Placing this
      // here avoids running the next rule for Literal values, which would create a new Literal
      // object and running eval unnecessarily.
      case l: Literal => l

      // foldable直接替换为Literal
      // Fold expressions that are foldable.
      case e if e.foldable => Literal.create(e.eval(EmptyRow), e.dataType)
    }
  }
}

/**
 * 简化不需要完整正则表达式来计算条件的LIKE表达式。例如，当表达式只是检查字符串是否以给定模式开头时。
 * Simplifies LIKE expressions that do not need full regular expressions to evaluate the condition.
 * For example, when the expression is just checking to see if a string starts with a given
 * pattern.
 */
object LikeSimplification extends Rule[LogicalPlan] {
  // if guards below protect from escapes on trailing %.
  // Cases like "something\%" are not optimized, but this does not affect correctness.
  private val startsWith = "([^_%]+)%".r
  private val endsWith = "%([^_%]+)".r
  private val startsAndEndsWith = "([^_%]+)%([^_%]+)".r
  private val contains = "%([^_%]+)%".r
  private val equalTo = "([^_%]*)".r

  private def simplifyLike(
      input: Expression, pattern: String, escapeChar: Char = '\\'): Option[Expression] = {
    if (pattern.contains(escapeChar)) {
      // There are three different situations when pattern containing escapeChar:
      // 1. pattern contains invalid escape sequence, e.g. 'm\aca'
      // 2. pattern contains escaped wildcard character, e.g. 'ma\%ca'
      // 3. pattern contains escaped escape character, e.g. 'ma\\ca'
      // Although there are patterns can be optimized if we handle the escape first, we just
      // skip this rule if pattern contains any escapeChar for simplicity.
      None
    } else {
      pattern match {
        case startsWith(prefix) =>
          Some(StartsWith(input, Literal(prefix)))
        case endsWith(postfix) =>
          Some(EndsWith(input, Literal(postfix)))
        // 'a%a' pattern is basically same with 'a%' && '%a'.
        // However, the additional `Length` condition is required to prevent 'a' match 'a%a'.
        case startsAndEndsWith(prefix, postfix) =>
          Some(And(GreaterThanOrEqual(Length(input), Literal(prefix.length + postfix.length)),
            And(StartsWith(input, Literal(prefix)), EndsWith(input, Literal(postfix)))))
        case contains(infix) =>
          Some(Contains(input, Literal(infix)))
        case equalTo(str) =>
          Some(EqualTo(input, Literal(str)))
        case _ => None
      }
    }
  }

  /*private def simplifyMultiLike(
      child: Expression, patterns: Seq[String], multi: MultiLikeBase): Expression = {
    val (remainPatternMap, replacementMap) =
      patterns.map { p =>
        p -> Option(p).flatMap(p => simplifyLike(child, p.toString))
      }.partition(_._2.isEmpty)
    val remainPatterns = remainPatternMap.map(_._1)
    val replacements = replacementMap.map(_._2.get)
    if (replacements.isEmpty) {
      multi
    } else {
      multi match {
        case l: LikeAll => And(replacements.reduceLeft(And), l.copy(patterns = remainPatterns))
        case l: NotLikeAll =>
          And(replacements.map(Not(_)).reduceLeft(And), l.copy(patterns = remainPatterns))
        case l: LikeAny => Or(replacements.reduceLeft(Or), l.copy(patterns = remainPatterns))
        case l: NotLikeAny =>
          Or(replacements.map(Not(_)).reduceLeft(Or), l.copy(patterns = remainPatterns))
      }
    }
  }*/

  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case l @ Like(input, Literal(pattern, StringType), escapeChar) =>
      if (pattern == null) {
        // If pattern is null, return null value directly, since "col like null" == null.
        Literal(null, BooleanType)
      } else {
        simplifyLike(input, pattern.asInstanceOf[String], escapeChar).getOrElse(l)
      }
    /*case l @ LikeAll(child, patterns) => simplifyMultiLike(child, patterns, l)
    case l @ NotLikeAll(child, patterns) => simplifyMultiLike(child, patterns, l)
    case l @ LikeAny(child, patterns) => simplifyMultiLike(child, patterns, l)
    case l @ NotLikeAny(child, patterns) => simplifyMultiLike(child, patterns, l)*/
  }
}
