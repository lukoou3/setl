package com.lk.setl.sql.catalyst

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

import scala.language.implicitConversions

import com.lk.setl.sql.catalyst.analysis._
import com.lk.setl.sql.catalyst.expressions._
import com.lk.setl.sql.types._

/**
 * A collection of implicit conversions that create a DSL for constructing catalyst data structures.
 *
 * {{{
 *  scala> import com.lk.setl.sql.catalyst.dsl.expressions._
 *
 *  // Standard operators are added to expressions.
 *  scala> import com.lk.setl.sql.catalyst.expressions.Literal
 *  scala> Literal(1) + Literal(1)
 *  res0: com.lk.setl.sql.catalyst.expressions.Add = (1 + 1)
 *
 *  // There is a conversion from 'symbols to unresolved attributes.
 *  scala> 'a.attr
 *  res1: com.lk.setl.sql.catalyst.analysis.UnresolvedAttribute = 'a
 *
 *  // These unresolved attributes can be used to create more complicated expressions.
 *  scala> 'a === 'b
 *  res2: com.lk.setl.sql.catalyst.expressions.EqualTo = ('a = 'b)
 *
 *  // SQL verbs can be used to construct logical query plans.
 *  scala> import com.lk.setl.sql.catalyst.plans.logical._
 *  scala> import com.lk.setl.sql.catalyst.dsl.plans._
 *  scala> LocalRelation('key.int, 'value.string).where('key === 1).select('value).analyze
 *  res3: com.lk.setl.sql.catalyst.plans.logical.LogicalPlan =
 *  Project [value#3]
 *   Filter (key#2 = 1)
 *    LocalRelation [key#2,value#3], []
 * }}}
 */
package object dsl {
  trait ImplicitOperators {
    def expr: Expression

    def unary_+ : Expression = UnaryPositive(expr)
    def unary_- : Expression = UnaryMinus(expr)
    def unary_! : Predicate = Not(expr)

    def + (other: Expression): Expression = Add(expr, other)
    def - (other: Expression): Expression = Subtract(expr, other)
    def * (other: Expression): Expression = Multiply(expr, other)
    def / (other: Expression): Expression = Divide(expr, other)
    def div (other: Expression): Expression = IntegralDivide(expr, other)
    def % (other: Expression): Expression = Remainder(expr, other)

    def && (other: Expression): Predicate = And(expr, other)
    def || (other: Expression): Predicate = Or(expr, other)

    def < (other: Expression): Predicate = LessThan(expr, other)
    def <= (other: Expression): Predicate = LessThanOrEqual(expr, other)
    def > (other: Expression): Predicate = GreaterThan(expr, other)
    def >= (other: Expression): Predicate = GreaterThanOrEqual(expr, other)
    def === (other: Expression): Predicate = EqualTo(expr, other)
    def <=> (other: Expression): Predicate = EqualNullSafe(expr, other)
    def =!= (other: Expression): Predicate = Not(EqualTo(expr, other))

    def in(list: Expression*): Expression = list match {
      case _ => In(expr, list)
    }

    def like(other: Expression, escapeChar: Char = '\\'): Expression =
      Like(expr, other, escapeChar)
    def rlike(other: Expression): Expression = RLike(expr, other)
    def contains(other: Expression): Expression = Contains(expr, other)
    def startsWith(other: Expression): Expression = StartsWith(expr, other)
    def endsWith(other: Expression): Expression = EndsWith(expr, other)
    def substr(pos: Expression, len: Expression = Literal(Int.MaxValue)): Expression =
      Substring(expr, pos, len)
    def substring(pos: Expression, len: Expression = Literal(Int.MaxValue)): Expression =
      Substring(expr, pos, len)

    def isNull: Predicate = IsNull(expr)
    def isNotNull: Predicate = IsNotNull(expr)

    def getItem(ordinal: Expression): UnresolvedExtractValue = UnresolvedExtractValue(expr, ordinal)
    def getField(fieldName: String): UnresolvedExtractValue =
      UnresolvedExtractValue(expr, Literal(fieldName))

    def cast(to: DataType): Expression = {
      if (expr.resolved && expr.dataType.sameType(to)) {
        expr
      } else {
        Cast(expr, to)
      }
    }

    def asc: SortOrder = SortOrder(expr, Ascending)
    def asc_nullsLast: SortOrder = SortOrder(expr, Ascending, NullsLast, Seq.empty)
    def desc: SortOrder = SortOrder(expr, Descending)
    def desc_nullsFirst: SortOrder = SortOrder(expr, Descending, NullsFirst, Seq.empty)
    def as(alias: String): NamedExpression = Alias(expr, alias)()
    def as(alias: Symbol): NamedExpression = Alias(expr, alias.name)()
  }

  trait ExpressionConversions {
    implicit class DslExpression(e: Expression) extends ImplicitOperators {
      def expr: Expression = e
    }

    implicit def booleanToLiteral(b: Boolean): Literal = Literal(b)
    implicit def byteToLiteral(b: Byte): Literal = Literal(b)
    implicit def shortToLiteral(s: Short): Literal = Literal(s)
    implicit def intToLiteral(i: Int): Literal = Literal(i)
    implicit def longToLiteral(l: Long): Literal = Literal(l)
    implicit def floatToLiteral(f: Float): Literal = Literal(f)
    implicit def doubleToLiteral(d: Double): Literal = Literal(d)
    implicit def stringToLiteral(s: String): Literal = Literal.create(s, StringType)
    implicit def dateToLiteral(d: Date): Literal = Literal(d)
    implicit def localDateToLiteral(d: LocalDate): Literal = Literal(d)
    implicit def bigDecimalToLiteral(d: BigDecimal): Literal = Literal(d.underlying())
    implicit def bigDecimalToLiteral(d: java.math.BigDecimal): Literal = Literal(d)
    implicit def timestampToLiteral(t: Timestamp): Literal = Literal(t)
    implicit def instantToLiteral(i: Instant): Literal = Literal(i)
    implicit def binaryToLiteral(a: Array[Byte]): Literal = Literal(a)

    implicit def symbolToUnresolvedAttribute(s: Symbol): analysis.UnresolvedAttribute =
      analysis.UnresolvedAttribute(s.name)

    /** Converts $"col name" into an [[analysis.UnresolvedAttribute]]. */
    implicit class StringToAttributeConversionHelper(val sc: StringContext) {
      // Note that if we make ExpressionConversions an object rather than a trait, we can
      // then make this a value class to avoid the small penalty of runtime instantiation.
      def $(args: Any*): analysis.UnresolvedAttribute = {
        analysis.UnresolvedAttribute(sc.s(args : _*))
      }
    }


    def upper(e: Expression): Expression = Upper(e)
    def lower(e: Expression): Expression = Lower(e)
    def coalesce(args: Expression*): Expression = Coalesce(args)
    def greatest(args: Expression*): Expression = Greatest(args)
    def least(args: Expression*): Expression = Least(args)
    /*def sqrt(e: Expression): Expression = Sqrt(e)
    def abs(e: Expression): Expression = Abs(e)*/
    def star(names: String*): Expression = names match {
      case Seq() => UnresolvedStar(None)
      case target => UnresolvedStar(Option(target))
    }


    implicit class DslSymbol(sym: Symbol) extends ImplicitAttribute { def s: String = sym.name }
    // TODO more implicit class for literal?
    implicit class DslString(val s: String) extends ImplicitOperators {
      override def expr: Expression = Literal(s)
      def attr: UnresolvedAttribute = analysis.UnresolvedAttribute(s)
    }

    abstract class ImplicitAttribute extends ImplicitOperators {
      def s: String
      def expr: UnresolvedAttribute = attr
      def attr: UnresolvedAttribute = analysis.UnresolvedAttribute(s)

      /** Creates a new AttributeReference of type boolean */
      def boolean: AttributeReference = AttributeReference(s, BooleanType)()

      /** Creates a new AttributeReference of type int */
      def int: AttributeReference = AttributeReference(s, IntegerType)()

      /** Creates a new AttributeReference of type long */
      def long: AttributeReference = AttributeReference(s, LongType)()

      /** Creates a new AttributeReference of type float */
      def float: AttributeReference = AttributeReference(s, FloatType)()

      /** Creates a new AttributeReference of type double */
      def double: AttributeReference = AttributeReference(s, DoubleType)()

      /** Creates a new AttributeReference of type string */
      def string: AttributeReference = AttributeReference(s, StringType)()

      /** Creates a new AttributeReference of type binary */
      def binary: AttributeReference = AttributeReference(s, BinaryType)()

      /** Creates a new AttributeReference of type array */
      def array(dataType: DataType): AttributeReference =
        AttributeReference(s, ArrayType(dataType))()

      def array(arrayType: ArrayType): AttributeReference =
        AttributeReference(s, arrayType)()

      /** Creates a new AttributeReference of type struct */
      def struct(structType: StructType): AttributeReference =
        AttributeReference(s, structType)()
      def struct(attrs: AttributeReference*): AttributeReference =
        struct(StructType.fromAttributes(attrs))

      /** Create a function. */
      def function(exprs: Expression*): UnresolvedFunction =
        UnresolvedFunction(s, exprs)
    }

    implicit class DslAttribute(a: AttributeReference) {
      /*def notNull: AttributeReference = a.withNullability(false)
      def canBeNull: AttributeReference = a.withNullability(true)*/
      def at(ordinal: Int): BoundReference = BoundReference(ordinal, a.dataType)
    }
  }

  object expressions extends ExpressionConversions  // scalastyle:ignore

}
