package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.Row
import com.lk.setl.sql.catalyst.analysis.FunctionRegistry
import com.lk.setl.sql.catalyst.expressions.codegen._
import com.lk.setl.sql.catalyst.expressions.codegen.Block._
import com.lk.setl.sql.types._
import com.lk.setl.util.StringUtils

import java.util.Locale

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines expressions for string operations.
////////////////////////////////////////////////////////////////////////////////////////////////////


trait String2StringExpression extends ImplicitCastInputTypes {
  self: UnaryExpression =>

  def convert(v: String):String

  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType)

  protected override def nullSafeEval(input: Any): Any =
    convert(input.asInstanceOf[String])
}

/**
 * A function that converts the characters of a string to uppercase.
 */
case class Upper(child: Expression)
  extends UnaryExpression with String2StringExpression with NullIntolerant {

  // scalastyle:off caselocale
  override def convert(v: String): String = v.toUpperCase()
  // scalastyle:on caselocale

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"($c).toUpperCase()")
  }
}

/**
 * A function that converts the characters of a string to lowercase.
 */
case class Lower(child: Expression)
  extends UnaryExpression with String2StringExpression with NullIntolerant {

  // scalastyle:off caselocale
  override def convert(v: String): String = v.toLowerCase()
  // scalastyle:on caselocale

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"($c).toLowerCase()")
  }

  override def prettyName: String =
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("lower")
}

/** A base trait for functions that compare two strings, returning a boolean. */
abstract class StringPredicate extends BinaryExpression
  with Predicate with ImplicitCastInputTypes with NullIntolerant {

  def compare(l: String, r: String): Boolean

  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any =
    compare(input1.asInstanceOf[String], input2.asInstanceOf[String])

  override def toString: String = s"$nodeName($left, $right)"
}

/**
 * A function that returns true if the string `left` contains the string `right`.
 */
case class Contains(left: Expression, right: Expression) extends StringPredicate {
  override def compare(l: String, r: String): Boolean = l.contains(r)
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (c1, c2) => s"($c1).contains($c2)")
  }
}

/**
 * A function that returns true if the string `left` starts with the string `right`.
 */
case class StartsWith(left: Expression, right: Expression) extends StringPredicate {
  override def compare(l: String, r: String): Boolean = l.startsWith(r)
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (c1, c2) => s"($c1).startsWith($c2)")
  }
}

/**
 * A function that returns true if the string `left` ends with the string `right`.
 */
case class EndsWith(left: Expression, right: Expression) extends StringPredicate {
  override def compare(l: String, r: String): Boolean = l.endsWith(r)
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (c1, c2) => s"($c1).endsWith($c2)")
  }
}

/**
 * Replace all occurrences with string.
 */
case class StringReplace(srcExpr: Expression, searchExpr: Expression, replaceExpr: Expression)
  extends TernaryExpression with ImplicitCastInputTypes with NullIntolerant {

  def this(srcExpr: Expression, searchExpr: Expression) = {
    this(srcExpr, searchExpr, Literal(""))
  }

  override def nullSafeEval(srcEval: Any, searchEval: Any, replaceEval: Any): Any = {
    srcEval.asInstanceOf[String].replace(
      searchEval.asInstanceOf[String], replaceEval.asInstanceOf[String])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (src, search, replace) => {
      s"""${ev.value} = $src.replace($search, $replace);"""
    })
  }

  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, StringType)
  override def children: Seq[Expression] = srcExpr :: searchExpr :: replaceExpr :: Nil
  override def prettyName: String = "replace"
}

trait String2TrimExpression extends Expression with ImplicitCastInputTypes {

  protected def srcStr: Expression
  protected def trimStr: Option[Expression]
  protected def direction: String

  override def children: Seq[Expression] = srcStr +: trimStr.toSeq
  override def dataType: DataType = StringType
  override def inputTypes: Seq[AbstractDataType] = Seq.fill(children.size)(StringType)

  override def nullable: Boolean = children.exists(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)

  override def sql: String = if (trimStr.isDefined) {
    s"TRIM($direction ${trimStr.get.sql} FROM ${srcStr.sql})"
  } else {
    super.sql
  }
}

object StringTrim {
  def apply(str: Expression, trimStr: Expression) : StringTrim = StringTrim(str, Some(trimStr))
  def apply(str: Expression) : StringTrim = StringTrim(str, None)
}

/**
 * A function that takes a character string, removes the leading and trailing characters matching
 * with any character in the trim string, returns the new string.
 * If BOTH and trimStr keywords are not specified, it defaults to remove space character from both
 * ends. The trim function will have one argument, which contains the source string.
 * If BOTH and trimStr keywords are specified, it trims the characters from both ends, and the trim
 * function will have two arguments, the first argument contains trimStr, the second argument
 * contains the source string.
 * trimStr: A character string to be trimmed from the source string, if it has multiple characters,
 * the function searches for each character in the source string, removes the characters from the
 * source string until it encounters the first non-match character.
 * BOTH: removes any character from both ends of the source string that matches characters in the
 * trim string.
 */
case class StringTrim(
    srcStr: Expression,
    trimStr: Option[Expression] = None)
  extends String2TrimExpression {

  def this(trimStr: Expression, srcStr: Expression) = this(srcStr, Option(trimStr))

  def this(srcStr: Expression) = this(srcStr, None)

  override def prettyName: String = "trim"

  override protected def direction: String = "BOTH"

  override def eval(input: Row): Any = {
    val srcString = srcStr.eval(input).asInstanceOf[String]
    if (srcString == null) {
      null
    } else {
      if (trimStr.isDefined) {
        StringUtils.trim(srcString, true, true, trimStr.get.eval(input).asInstanceOf[String])
      } else {
        srcString.trim()
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evals = children.map(_.genCode(ctx))
    val srcString = evals(0)

    if (evals.length == 1) {
      ev.copy(evals.map(_.code) :+ code"""
        boolean ${ev.isNull} = false;
        UTF8String ${ev.value} = null;
        if (${srcString.isNull}) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = ${srcString.value}.trim();
        }""")
    } else {
      val trimString = evals(1)
      val getTrimFunction =
        s"""
        if (${trimString.isNull}) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = ${classOf[StringUtils].getName}.trim(${srcString.value}, true, true, ${trimString.value});
        }"""
      ev.copy(evals.map(_.code) :+ code"""
        boolean ${ev.isNull} = false;
        UTF8String ${ev.value} = null;
        if (${srcString.isNull}) {
          ${ev.isNull} = true;
        } else {
          $getTrimFunction
        }""")
    }
  }
}

object StringTrimLeft {
  def apply(str: Expression, trimStr: Expression): StringTrimLeft =
    StringTrimLeft(str, Some(trimStr))
  def apply(str: Expression): StringTrimLeft = StringTrimLeft(str, None)
}

/**
 * A function that trims the characters from left end for a given string.
 * If LEADING and trimStr keywords are not specified, it defaults to remove space character from
 * the left end. The ltrim function will have one argument, which contains the source string.
 * If LEADING and trimStr keywords are not specified, it trims the characters from left end. The
 * ltrim function will have two arguments, the first argument contains trimStr, the second argument
 * contains the source string.
 * trimStr: the function removes any character from the left end of the source string which matches
 * with the characters from trimStr, it stops at the first non-match character.
 * LEADING: removes any character from the left end of the source string that matches characters in
 * the trim string.
 */
case class StringTrimLeft(
    srcStr: Expression,
    trimStr: Option[Expression] = None)
  extends String2TrimExpression {

  def this(trimStr: Expression, srcStr: Expression) = this(srcStr, Option(trimStr))

  def this(srcStr: Expression) = this(srcStr, None)

  override def prettyName: String = "ltrim"

  override protected def direction: String = "LEADING"

  override def eval(input: Row): Any = {
    val srcString = srcStr.eval(input).asInstanceOf[String]
    if (srcString == null) {
      null
    } else {
      if (trimStr.isDefined) {
        //srcString.trimLeft(trimStr.get.eval(input).asInstanceOf[String])
        StringUtils.trim(srcString, true, false, trimStr.get.eval(input).asInstanceOf[String])
      } else {
        //srcString.trimLeft()
        StringUtils.trim(srcString, true, false, null)
      }
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evals = children.map(_.genCode(ctx))
    val srcString = evals(0)

    if (evals.length == 1) {
      ev.copy(evals.map(_.code) :+ code"""
        boolean ${ev.isNull} = false;
        UTF8String ${ev.value} = null;
        if (${srcString.isNull}) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = ${classOf[StringUtils].getName}.trim(${srcString.value}, true, false, null);
        }""")
    } else {
      val trimString = evals(1)
      val getTrimLeftFunction =
        s"""
        if (${trimString.isNull}) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = ${classOf[StringUtils].getName}.trim(${srcString.value}, true, false, ${trimString.value});
        }"""
      ev.copy(evals.map(_.code) :+ code"""
        boolean ${ev.isNull} = false;
        UTF8String ${ev.value} = null;
        if (${srcString.isNull}) {
          ${ev.isNull} = true;
        } else {
          $getTrimLeftFunction
        }""")
    }
  }
}

object StringTrimRight {
  def apply(str: Expression, trimStr: Expression): StringTrimRight =
    StringTrimRight(str, Some(trimStr))
  def apply(str: Expression) : StringTrimRight = StringTrimRight(str, None)
}

/**
 * A function that trims the characters from right end for a given string.
 * If TRAILING and trimStr keywords are not specified, it defaults to remove space character
 * from the right end. The rtrim function will have one argument, which contains the source string.
 * If TRAILING and trimStr keywords are specified, it trims the characters from right end. The
 * rtrim function will have two arguments, the first argument contains trimStr, the second argument
 * contains the source string.
 * trimStr: the function removes any character from the right end of source string which matches
 * with the characters from trimStr, it stops at the first non-match character.
 * TRAILING: removes any character from the right end of the source string that matches characters
 * in the trim string.
 */
case class StringTrimRight(
    srcStr: Expression,
    trimStr: Option[Expression] = None)
  extends String2TrimExpression {

  def this(trimStr: Expression, srcStr: Expression) = this(srcStr, Option(trimStr))

  def this(srcStr: Expression) = this(srcStr, None)

  override def prettyName: String = "rtrim"

  override protected def direction: String = "TRAILING"

  override def eval(input: Row): Any = {
    val srcString = srcStr.eval(input).asInstanceOf[String]
    if (srcString == null) {
      null
    } else {
      if (trimStr.isDefined) {
        StringUtils.trim(srcString, false, true, trimStr.get.eval(input).asInstanceOf[String])
      } else {
        StringUtils.trim(srcString, false, true, null)
      }
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evals = children.map(_.genCode(ctx))
    val srcString = evals(0)

    if (evals.length == 1) {
      ev.copy(evals.map(_.code) :+ code"""
        boolean ${ev.isNull} = false;
        UTF8String ${ev.value} = null;
        if (${srcString.isNull}) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = ${classOf[StringUtils].getName}.trim(${srcString.value}, false, true, null);
        }""")
    } else {
      val trimString = evals(1)
      val getTrimRightFunction =
        s"""
        if (${trimString.isNull}) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = ${classOf[StringUtils].getName}.trim(${srcString.value}, false, true, ${trimString.value});
        }"""
      ev.copy(evals.map(_.code) :+ code"""
        boolean ${ev.isNull} = false;
        UTF8String ${ev.value} = null;
        if (${srcString.isNull}) {
          ${ev.isNull} = true;
        } else {
          $getTrimRightFunction
        }""")
    }
  }
}

/**
 * A function that returns the position of the first occurrence of substr in the given string.
 * Returns null if either of the arguments are null and
 * returns 0 if substr could not be found in str.
 *
 * NOTE: that this is not zero based, but 1-based index. The first character in str has index 1.
 */
case class StringInstr(str: Expression, substr: Expression)
  extends BinaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def left: Expression = str
  override def right: Expression = substr
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)

  override def nullSafeEval(string: Any, sub: Any): Any = {
    string.asInstanceOf[String].indexOf(sub.asInstanceOf[String]) + 1
  }

  override def prettyName: String = "instr"

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (l, r) =>
      s"($l).indexOf($r) + 1")
  }
}

/**
 * Returns the input formatted according do printf-style format strings
 */
case class FormatString(children: Expression*) extends Expression with ImplicitCastInputTypes {

  require(children.nonEmpty, s"$prettyName() should take at least 1 argument")

  override def foldable: Boolean = children.forall(_.foldable)
  override def nullable: Boolean = children(0).nullable
  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] =
    StringType :: List.fill(children.size - 1)(AnyDataType)

  override def eval(input: Row): Any = {
    val pattern = children(0).eval(input)
    if (pattern == null) {
      null
    } else {
      val sb = new StringBuffer()
      val formatter = new java.util.Formatter(sb, Locale.US)

      val arglist = children.tail.map(_.eval(input).asInstanceOf[AnyRef])
      formatter.format(pattern.asInstanceOf[String].toString, arglist: _*)

      sb.toString
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val pattern = children.head.genCode(ctx)

    val argListGen = children.tail.map(x => (x.dataType, x.genCode(ctx)))
    val argList = ctx.freshName("argLists")
    val numArgLists = argListGen.length
    val argListCode = argListGen.zipWithIndex.map { case(v, index) =>
      val value =
        if (CodeGenerator.boxedType(v._1) != CodeGenerator.javaType(v._1)) {
          // Java primitives get boxed in order to allow null values.
          s"(${v._2.isNull}) ? (${CodeGenerator.boxedType(v._1)}) null : " +
            s"new ${CodeGenerator.boxedType(v._1)}(${v._2.value})"
        } else {
          s"(${v._2.isNull}) ? null : ${v._2.value}"
        }
      s"""
         ${v._2.code}
         $argList[$index] = $value;
       """
    }
    val argListCodes = ctx.splitExpressionsWithCurrentInputs(
      expressions = argListCode,
      funcName = "valueFormatString",
      extraArguments = ("Object[]", argList) :: Nil)

    val form = ctx.freshName("formatter")
    val formatter = classOf[java.util.Formatter].getName
    val sb = ctx.freshName("sb")
    val stringBuffer = classOf[StringBuffer].getName
    ev.copy(code = code"""
      ${pattern.code}
      boolean ${ev.isNull} = ${pattern.isNull};
      ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
      if (!${ev.isNull}) {
        $stringBuffer $sb = new $stringBuffer();
        $formatter $form = new $formatter($sb, ${classOf[Locale].getName}.US);
        Object[] $argList = new Object[$numArgLists];
        $argListCodes
        $form.format(${pattern.value}.toString(), $argList);
        ${ev.value} = $sb.toString();
      }""")
  }

  override def prettyName: String = getTagValue(
    FunctionRegistry.FUNC_ALIAS).getOrElse("format_string")
}


/**
 * A function that takes a substring of its first argument starting at a given position.
 * Defined for String and Binary types.
 *
 * NOTE: that this is not zero based, but 1-based index. The first character in str has index 1.
 */
case class Substring(str: Expression, pos: Expression, len: Expression)
  extends TernaryExpression with ImplicitCastInputTypes with NullIntolerant {

  def this(str: Expression, pos: Expression) = {
    this(str, pos, Literal(Integer.MAX_VALUE))
  }

  override def dataType: DataType = str.dataType

  // 输入类型, 第一个参数为TypeCollection(StringType, BinaryType)
  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringType, IntegerType, IntegerType)

  override def children: Seq[Expression] = str :: pos :: len :: Nil

  // TernaryExpression中eval实现了null判断
  override def nullSafeEval(string: Any, pos: Any, len: Any): Any = {
    str.dataType match {
      case StringType => StringUtils.substringSQL(string.asInstanceOf[String], pos.asInstanceOf[Int], len.asInstanceOf[Int])
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

    defineCodeGen(ctx, ev, (string, pos, len) => {
      str.dataType match {
        case StringType => s"${classOf[StringUtils].getName}.substringSQL($string, $pos, $len)"
      }
    })
  }
}

/**
 * A function that returns the char length of the given string expression or
 * number of bytes of the given binary expression.
 */
case class Length(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(StringType, BinaryType))

  protected override def nullSafeEval(value: Any): Any = child.dataType match {
    case StringType => value.asInstanceOf[String].length
    case BinaryType => value.asInstanceOf[Array[Byte]].length
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    child.dataType match {
      case StringType => defineCodeGen(ctx, ev, c => s"($c).length()")
      case BinaryType => defineCodeGen(ctx, ev, c => s"($c).length")
    }
  }
}

/**
 * Returns the ASCII character having the binary equivalent to n.
 * If n is larger than 256 the result is equivalent to chr(n % 256)
 */
case class Chr(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(LongType)

  protected override def nullSafeEval(lon: Any): Any = {
    val longVal = lon.asInstanceOf[Long]
    if (longVal < 0) {
      ""
    } else if ((longVal & 0xFF) == 0) {
      Character.MIN_VALUE.toString
    } else {
      (longVal & 0xFF).toChar.toString
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, lon => {
      s"""
        if ($lon < 0) {
          ${ev.value} = "";
        } else if (($lon & 0xFF) == 0) {
          ${ev.value} = String.valueOf(Character.MIN_VALUE);
        } else {
          char c = (char)($lon & 0xFF);
          ${ev.value} = String.valueOf(c);
        }
      """
    })
  }
}
