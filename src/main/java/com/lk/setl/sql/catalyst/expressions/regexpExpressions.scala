package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.GenericArrayData
import com.lk.setl.sql.catalyst.analysis.TypeCheckResult
import com.lk.setl.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import com.lk.setl.sql.catalyst.expressions.codegen.Block.BlockHelper
import com.lk.setl.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import com.lk.setl.sql.catalyst.util.StringUtils
import com.lk.setl.sql.types._
import org.apache.commons.text.StringEscapeUtils

import java.util.Locale
import java.util.regex.{MatchResult, Matcher, Pattern}
import scala.collection.mutable.ArrayBuffer

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

/**
 * Splits str around matches of the given regex.
 */
case class StringSplit(str: Expression, regex: Expression, limit: Expression)
  extends TernaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def dataType: DataType = ArrayType(StringType)
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, IntegerType)
  override def children: Seq[Expression] = str :: regex :: limit :: Nil

  def this(exp: Expression, regex: Expression) = this(exp, regex, Literal(-1));

  override def nullSafeEval(string: Any, regex: Any, limit: Any): Any = {
    val strings = string.asInstanceOf[String].split(regex.asInstanceOf[String], limit.asInstanceOf[Int])
    new GenericArrayData(strings.asInstanceOf[Array[Any]])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val arrayClass = classOf[GenericArrayData].getName
    nullSafeCodeGen(ctx, ev, (str, regex, limit) => {
      // Array in java is covariant, so we don't need to cast String[] to Object[].
      s"""${ev.value} = new $arrayClass($str.split($regex,$limit));""".stripMargin
    })
  }

  override def prettyName: String = "split"
}

/**
 * Replace all substrings of str that match regexp with rep.
 *
 * NOTE: this expression is not THREAD-SAFE, as it has some internal mutable status.
 */
case class RegExpReplace(subject: Expression, regexp: Expression, rep: Expression, pos: Expression)
  extends QuaternaryExpression with ImplicitCastInputTypes with NullIntolerant {

  def this(subject: Expression, regexp: Expression, rep: Expression) =
    this(subject, regexp, rep, Literal(1))

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!pos.foldable) {
      return TypeCheckFailure(s"Position expression must be foldable, but got $pos")
    }

    val posEval = pos.eval()
    if (posEval == null || posEval.asInstanceOf[Int] > 0) {
      TypeCheckSuccess
    } else {
      TypeCheckFailure(s"Position expression must be positive, but got: $posEval")
    }
  }

  // last regex in string, we will update the pattern iff regexp value changed.
  @transient private var lastRegex: String = _
  // last regex pattern, we cache it for performance concern
  @transient private var pattern: Pattern = _
  // last replacement string, we don't want to convert a UTF8String => java.langString every time.
  @transient private var lastReplacement: String = _
  @transient private var lastReplacementInUTF8: String = _
  // result buffer write by Matcher
  @transient private lazy val result: StringBuffer = new StringBuffer

  override def nullSafeEval(s: Any, p: Any, r: Any, i: Any): Any = {
    if (!p.equals(lastRegex)) {
      // regex value changed
      lastRegex = p.asInstanceOf[String]
      pattern = Pattern.compile(lastRegex)
    }
    if (!r.equals(lastReplacementInUTF8)) {
      // replacement string changed
      lastReplacementInUTF8 = r.asInstanceOf[String]
      lastReplacement = lastReplacementInUTF8
    }
    val source = s.asInstanceOf[String]
    val position = i.asInstanceOf[Int] - 1
    if (position < source.length) {
      val m = pattern.matcher(source)
      m.region(position, source.length)
      result.delete(0, result.length())
      while (m.find) {
        m.appendReplacement(result, lastReplacement)
      }
      m.appendTail(result)
      result.toString
    } else {
      s
    }
  }

  override def dataType: DataType = StringType
  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringType, StringType, StringType, IntegerType)
  override def children: Seq[Expression] = subject :: regexp :: rep :: pos :: Nil
  override def prettyName: String = "regexp_replace"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val termResult = ctx.freshName("termResult")

    val classNamePattern = classOf[Pattern].getCanonicalName
    val classNameStringBuffer = classOf[java.lang.StringBuffer].getCanonicalName

    val matcher = ctx.freshName("matcher")
    val source = ctx.freshName("source")
    val position = ctx.freshName("position")

    val termLastRegex = ctx.addMutableState("String", "lastRegex")
    val termPattern = ctx.addMutableState(classNamePattern, "pattern")
    val termLastReplacement = ctx.addMutableState("String", "lastReplacement")
    val termLastReplacementInUTF8 = ctx.addMutableState("String", "lastReplacementInUTF8")

    val setEvNotNull = if (nullable) {
      s"${ev.isNull} = false;"
    } else {
      ""
    }

    nullSafeCodeGen(ctx, ev, (subject, regexp, rep, pos) => {
      s"""
      if (!$regexp.equals($termLastRegex)) {
        // regex value changed
        $termLastRegex = $regexp;
        $termPattern = $classNamePattern.compile($termLastRegex);
      }
      if (!$rep.equals($termLastReplacementInUTF8)) {
        // replacement string changed
        $termLastReplacementInUTF8 = $rep;
        $termLastReplacement = $termLastReplacementInUTF8;
      }
      String $source = $subject;
      int $position = $pos - 1;
      if ($position < $source.length()) {
        $classNameStringBuffer $termResult = new $classNameStringBuffer();
        java.util.regex.Matcher $matcher = $termPattern.matcher($source);
        $matcher.region($position, $source.length());

        while ($matcher.find()) {
          $matcher.appendReplacement($termResult, $termLastReplacement);
        }
        $matcher.appendTail($termResult);
        ${ev.value} = $termResult.toString();
        $termResult = null;
      } else {
        ${ev.value} = $subject;
      }
      $setEvNotNull
    """
    })
  }
}

object RegExpReplace {
  def apply(subject: Expression, regexp: Expression, rep: Expression): RegExpReplace =
    new RegExpReplace(subject, regexp, rep)
}


object RegExpExtractBase {
  def checkGroupIndex(groupCount: Int, groupIndex: Int): Unit = {
    if (groupIndex < 0) {
      throw new IllegalArgumentException("The specified group index cannot be less than zero")
    } else if (groupCount < groupIndex) {
      throw new IllegalArgumentException(
        s"Regex group count is $groupCount, but the specified group index is $groupIndex")
    }
  }
}

/**
 * 正则表达式提取字符串的抽象基类, 就两个子类: RegExpExtract 和 RegExpExtractAll
 * 这里就定义了获取Matcher的逻辑, 我们写函数时都可以借鉴这样的实现, 复用值标记为transient
 * 其他情况我们可能把属性标记为transient lazy val, 这里pattern没用lazy标记是因为输入的正则可能不是字面量, 可以变化, 还有就是要根据参数生成pattern
 */
abstract class RegExpExtractBase
  extends TernaryExpression with ImplicitCastInputTypes with NullIntolerant {
  def subject: Expression
  def regexp: Expression
  def idx: Expression

  // last regex in string, we will update the pattern iff regexp value changed.
  @transient private var lastRegex: String = _
  // last regex pattern, we cache it for performance concern
  @transient private var pattern: Pattern = _

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, IntegerType)
  override def children: Seq[Expression] = subject :: regexp :: idx :: Nil

  protected def getLastMatcher(s: Any, p: Any): Matcher = {
    // 正常情况下我们输入的正则字符串都是字面量, 这个每个task的每个类只会执行一次初始化pattern
    if (p != lastRegex) {
      // regex value changed
      lastRegex = p.asInstanceOf[String]
      pattern = Pattern.compile(lastRegex)
    }
    pattern.matcher(s.toString)
  }
}

/**
 * Extract a specific(idx) group identified by a Java regex.
 * spark sql 默认是会转义我们输入的\的, spark.sql.parser.escapedStringLiterals默认为false
 * 正则表达式需要写的和java中的字面量一样
 *
 * NOTE: this expression is not THREAD-SAFE, as it has some internal mutable status.
 */
case class RegExpExtract(subject: Expression, regexp: Expression, idx: Expression)
  extends RegExpExtractBase {
  def this(s: Expression, r: Expression) = this(s, r, Literal(1))

  override def nullSafeEval(s: Any, p: Any, r: Any): Any = {
    val m = getLastMatcher(s, p)
    // 只要参数不为null, 匹配不到返回的是空字符串
    if (m.find) {
      val mr: MatchResult = m.toMatchResult
      val index = r.asInstanceOf[Int]
      RegExpExtractBase.checkGroupIndex(mr.groupCount, index)
      val group = mr.group(index)
      if (group == null) { // Pattern matched, but it's an optional group
        ""
      } else {
        group
      }
    } else {
      ""
    }
  }

  override def dataType: DataType = StringType
  override def prettyName: String = "regexp_extract"

  // 这个生成java代码的实现太麻烦了, 我还是使用给用户提供的注册udf的实现吧
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val classNamePattern = classOf[Pattern].getCanonicalName
    val classNameRegExpExtractBase = classOf[RegExpExtractBase].getCanonicalName
    val matcher = ctx.freshName("matcher")
    val matchResult = ctx.freshName("matchResult")

    val termLastRegex = ctx.addMutableState("String", "lastRegex")
    val termPattern = ctx.addMutableState(classNamePattern, "pattern")

    val setEvNotNull = if (nullable) {
      s"${ev.isNull} = false;"
    } else {
      ""
    }

    nullSafeCodeGen(ctx, ev, (subject, regexp, idx) => {
      s"""
      if (!$regexp.equals($termLastRegex)) {
        // regex value changed
        $termLastRegex = $regexp;
        $termPattern = $classNamePattern.compile($termLastRegex);
      }
      java.util.regex.Matcher $matcher = $termPattern.matcher($subject);
      if ($matcher.find()) {
        java.util.regex.MatchResult $matchResult = $matcher.toMatchResult();
        $classNameRegExpExtractBase.checkGroupIndex($matchResult.groupCount(), $idx);
        if ($matchResult.group($idx) == null) {
          ${ev.value} = "";
        } else {
          ${ev.value} = $matchResult.group($idx);
        }
        $setEvNotNull
      } else {
        ${ev.value} = "";
        $setEvNotNull
      }"""
    })
  }
}

/**
 * Extract all specific(idx) groups identified by a Java regex.
 *
 * NOTE: this expression is not THREAD-SAFE, as it has some internal mutable status.
 */
case class RegExpExtractAll(subject: Expression, regexp: Expression, idx: Expression)
  extends RegExpExtractBase {
  def this(s: Expression, r: Expression) = this(s, r, Literal(1))

  override def nullSafeEval(s: Any, p: Any, r: Any): Any = {
    val m = getLastMatcher(s, p)
    val matchResults = new ArrayBuffer[String]()
    while(m.find) {
      val mr: MatchResult = m.toMatchResult
      val index = r.asInstanceOf[Int]
      RegExpExtractBase.checkGroupIndex(mr.groupCount, index)
      val group = mr.group(index)
      if (group == null) { // Pattern matched, but it's an optional group
        matchResults += ""
      } else {
        matchResults += group
      }
    }

    new GenericArrayData(matchResults.toArray.asInstanceOf[Array[Any]])
  }

  override def dataType: DataType = ArrayType(StringType)
  override def prettyName: String = "regexp_extract_all"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val classNamePattern = classOf[Pattern].getCanonicalName
    val classNameRegExpExtractBase = classOf[RegExpExtractBase].getCanonicalName
    val arrayClass = classOf[GenericArrayData].getName
    val matcher = ctx.freshName("matcher")
    val matchResult = ctx.freshName("matchResult")
    val matchResults = ctx.freshName("matchResults")

    val termLastRegex = ctx.addMutableState("String", "lastRegex")
    val termPattern = ctx.addMutableState(classNamePattern, "pattern")

    val setEvNotNull = if (nullable) {
      s"${ev.isNull} = false;"
    } else {
      ""
    }
    nullSafeCodeGen(ctx, ev, (subject, regexp, idx) => {
      s"""
         | if (!$regexp.equals($termLastRegex)) {
         |   // regex value changed
         |   $termLastRegex = $regexp;
         |   $termPattern = $classNamePattern.compile($termLastRegex);
         | }
         | java.util.regex.Matcher $matcher = $termPattern.matcher($subject);
         | java.util.ArrayList $matchResults = new java.util.ArrayList<String>();
         | while ($matcher.find()) {
         |   java.util.regex.MatchResult $matchResult = $matcher.toMatchResult();
         |   $classNameRegExpExtractBase.checkGroupIndex($matchResult.groupCount(), $idx);
         |   if ($matchResult.group($idx) == null) {
         |     $matchResults.add("");
         |   } else {
         |     $matchResults.add($matchResult.group($idx));
         |   }
         | }
         | ${ev.value} =
         |   new $arrayClass($matchResults.toArray(new String[$matchResults.size()]));
         | $setEvNotNull
         """
    })
  }
}
