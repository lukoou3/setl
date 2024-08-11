package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.catalyst.expressions.codegen.Block.BlockHelper
import com.lk.setl.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import com.lk.setl.sql.catalyst.util.StringUtils
import com.lk.setl.sql.types._

import org.apache.commons.text.StringEscapeUtils

import java.util.Locale
import java.util.regex.Pattern

abstract class StringRegexExpression extends BinaryExpression
  with ImplicitCastInputTypes with NullIntolerant {

  def escape(v: String): String
  def matches(regex: Pattern, str: String): Boolean

  override def dataType: DataType = BooleanType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)

  // try cache foldable pattern
  private lazy val cache: Pattern = right match {
    case p: Expression if p.foldable =>
      compile(p.eval().asInstanceOf[String])
    case _ => null
  }

  protected def compile(str: String): Pattern = if (str == null) {
    null
  } else {
    // Let it raise exception if couldn't compile the regex string
    Pattern.compile(escape(str))
  }

  protected def pattern(str: String) = if (cache == null) compile(str) else cache

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    val regex = pattern(input2.asInstanceOf[String])
    if(regex == null) {
      null
    } else {
      matches(regex, input1.asInstanceOf[String])
    }
  }

  override def sql: String = s"${left.sql} ${prettyName.toUpperCase(Locale.ROOT)} ${right.sql}"
}

case class Like(left: Expression, right: Expression, escapeChar: Char)
  extends StringRegexExpression {

  def this(left: Expression, right: Expression) = this(left, right, '\\')

  override def escape(v: String): String = StringUtils.escapeLikeRegex(v, escapeChar)

  override def matches(regex: Pattern, str: String): Boolean = regex.matcher(str).matches()

  override def toString: String = escapeChar match {
    case '\\' => s"$left LIKE $right"
    case c => s"$left LIKE $right ESCAPE '$c'"
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val patternClass = classOf[Pattern].getName
    val escapeFunc = StringUtils.getClass.getName.stripSuffix("$") + ".escapeLikeRegex"

    if (right.foldable) {
      val rVal = right.eval()
      if (rVal != null) {
        val regexStr =
          StringEscapeUtils.escapeJava(escape(rVal.toString()))
        val pattern = ctx.addMutableState(patternClass, "patternLike",
          v => s"""$v = $patternClass.compile("$regexStr");""")

        // We don't use nullSafeCodeGen here because we don't want to re-evaluate right again.
        val eval = left.genCode(ctx)
        ev.copy(code = code"""
          ${eval.code}
          boolean ${ev.isNull} = ${eval.isNull};
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = $pattern.matcher(${eval.value}.toString()).matches();
          }
        """)
      } else {
        ev.copy(code = code"""
          boolean ${ev.isNull} = true;
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        """)
      }
    } else {
      val pattern = ctx.freshName("pattern")
      val rightStr = ctx.freshName("rightStr")
      // We need to escape the escapeChar to make sure the generated code is valid.
      // Otherwise we'll hit org.codehaus.commons.compiler.CompileException.
      val escapedEscapeChar = StringEscapeUtils.escapeJava(escapeChar.toString)
      nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        s"""
          String $rightStr = $eval2.toString();
          $patternClass $pattern = $patternClass.compile(
            $escapeFunc($rightStr, '$escapedEscapeChar'));
          ${ev.value} = $pattern.matcher($eval1.toString()).matches();
        """
      })
    }
  }
}

case class RLike(left: Expression, right: Expression) extends StringRegexExpression {

  override def escape(v: String): String = v
  override def matches(regex: Pattern, str: String): Boolean = regex.matcher(str).find(0)
  override def toString: String = s"$left RLIKE $right"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val patternClass = classOf[Pattern].getName

    if (right.foldable) {
      val rVal = right.eval()
      if (rVal != null) {
        val regexStr =
          StringEscapeUtils.escapeJava(rVal.toString())
        val pattern = ctx.addMutableState(patternClass, "patternRLike",
          v => s"""$v = $patternClass.compile("$regexStr");""")

        // We don't use nullSafeCodeGen here because we don't want to re-evaluate right again.
        val eval = left.genCode(ctx)
        ev.copy(code = code"""
          ${eval.code}
          boolean ${ev.isNull} = ${eval.isNull};
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = $pattern.matcher(${eval.value}.toString()).find(0);
          }
        """)
      } else {
        ev.copy(code = code"""
          boolean ${ev.isNull} = true;
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        """)
      }
    } else {
      val rightStr = ctx.freshName("rightStr")
      val pattern = ctx.freshName("pattern")
      nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        s"""
          String $rightStr = $eval2.toString();
          $patternClass $pattern = $patternClass.compile($rightStr);
          ${ev.value} = $pattern.matcher($eval1.toString()).find(0);
        """
      })
    }
  }
}

