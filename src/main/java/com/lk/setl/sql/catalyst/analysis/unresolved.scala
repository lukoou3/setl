package com.lk.setl.sql.catalyst.analysis

import com.lk.setl.sql.{AnalysisException, Row}
import com.lk.setl.sql.catalyst.errors.TreeNodeException
import com.lk.setl.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import com.lk.setl.sql.catalyst.expressions.{Alias, Attribute, BinaryExpression, Expression, Generator, GetStructField, LeafExpression, NamedExpression, UnaryExpression, Unevaluable}
import com.lk.setl.sql.catalyst.parser.ParserUtils
import com.lk.setl.sql.catalyst.plans.logical.LeafNode
import com.lk.setl.sql.catalyst.trees.TreeNode
import com.lk.setl.sql.catalyst.util.quoteIdentifier
import com.lk.setl.sql.types.{DataType, StructType}


/**
 * Thrown when an invalid attempt is made to access a property of a tree that has yet to be fully
 * resolved.
 */
class UnresolvedException[TreeType <: TreeNode[_]](tree: TreeType, function: String)
  extends TreeNodeException(tree, s"Invalid call to $function on unresolved object", null)

/**
 * Holds the name of a relation that has yet to be looked up in a catalog.
 *
 * @param multipartIdentifier table name
 * @param options options to scan this relation. Only applicable to v2 table scan.
 */
case class UnresolvedRelation(
    multipartIdentifier: Seq[String])
  extends LeafNode with NamedRelation {

  /** Returns a `.` separated name for this relation. */
  def tableName: String = multipartIdentifier.map(quoteIfNeeded).mkString(".")

  override def name: String = tableName

  override def output: Seq[Attribute] = Nil

  override lazy val resolved = false

  def quoteIfNeeded(part: String): String = {
    if (part.contains(".") || part.contains("`")) {
      s"`${part.replace("`", "``")}`"
    } else {
      part
    }
  }
}

/**
 * Represents an unresolved generator, which will be created by the parser for
 * the [[com.lk.setl.sql.catalyst.plans.logical.Generate]] operator.
 * The analyzer will resolve this generator.
 */
case class UnresolvedGenerator(name: String, children: Seq[Expression])
  extends Generator {

  override def elementSchema: StructType = throw new UnresolvedException(this, "elementTypes")
  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def foldable: Boolean = throw new UnresolvedException(this, "foldable")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override lazy val resolved = false

  override def prettyName: String = name
  override def toString: String = s"'$name(${children.mkString(", ")})"

  override def eval(input: Row = null): TraversableOnce[Row] =
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new UnsupportedOperationException(s"Cannot generate code for expression: $this")

  override def terminate(): TraversableOnce[Row] =
    throw new UnsupportedOperationException(s"Cannot terminate expression: $this")
}

case class UnresolvedFunction(
  name: String,
  arguments: Seq[Expression],
  filter: Option[Expression] = None)
  extends Expression with Unevaluable {

  override def children: Seq[Expression] = arguments ++ filter.toSeq

  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override lazy val resolved = false

  override def prettyName: String = name
  override def toString: String = {
    val distinct = if (false) "distinct " else ""
    s"'$name($distinct${children.mkString(", ")})"
  }
}


/**
 * Represents all of the input attributes to a given relational operator, for example in
 * "SELECT * FROM ...". A [[Star]] gets automatically expanded during analysis.
 */
abstract class Star extends LeafExpression with NamedExpression {

  override def name: String = throw new UnresolvedException(this, "name")
  override def exprId: Long = throw new UnresolvedException(this, "exprId")
  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def qualifier: Seq[String] = throw new UnresolvedException(this, "qualifier")
  override def toAttribute: Attribute = throw new UnresolvedException(this, "toAttribute")
  override def newInstance(): NamedExpression = throw new UnresolvedException(this, "newInstance")
  override lazy val resolved = false

  def expand(input: Seq[Attribute], resolver: Resolver): Seq[NamedExpression]
}

/**
 * Represents all of the input attributes to a given relational operator, for example in
 * "SELECT * FROM ...".
 *
 * This is also used to expand structs. For example:
 * "SELECT record.* from (SELECT struct(a,b,c) as record ...)
 *
 * @param target an optional name that should be the target of the expansion.  If omitted all
 *              targets' columns are produced. This can either be a table name or struct name. This
 *              is a list of identifiers that is the path of the expansion.
 */
case class UnresolvedStar(target: Option[Seq[String]]) extends Star with Unevaluable {

  /**
   * Returns true if the nameParts is a subset of the last elements of qualifier of the attribute.
   *
   * For example, the following should all return true:
   *   - `SELECT ns1.ns2.t.* FROM ns1.n2.t` where nameParts is Seq("ns1", "ns2", "t") and
   *     qualifier is Seq("ns1", "ns2", "t").
   *   - `SELECT ns2.t.* FROM ns1.n2.t` where nameParts is Seq("ns2", "t") and
   *     qualifier is Seq("ns1", "ns2", "t").
   *   - `SELECT t.* FROM ns1.n2.t` where nameParts is Seq("t") and
   *     qualifier is Seq("ns1", "ns2", "t").
   */
  private def matchedQualifier(
    attribute: Attribute,
    nameParts: Seq[String],
    resolver: Resolver): Boolean = {
    val qualifierList = if (nameParts.length == attribute.qualifier.length) {
      attribute.qualifier
    } else {
      attribute.qualifier.takeRight(nameParts.length)
    }
    nameParts.corresponds(qualifierList)(resolver)
  }

  override def expand(
    input: Seq[Attribute],
    resolver: Resolver): Seq[NamedExpression] = {
    // If there is no table specified, use all input attributes.
    if (target.isEmpty) return input

    val expandedAttributes = input.filter(matchedQualifier(_, target.get, resolver))

    if (expandedAttributes.nonEmpty) return expandedAttributes

    // Try to resolve it as a struct expansion. If there is a conflict and both are possible,
    // (i.e. [name].* is both a table and a struct), the struct path can always be qualified.
    val attribute = input.resolve(target.get, resolver)
    if (attribute.isDefined) {
      // This target resolved to an attribute in child. It must be a struct. Expand it.
      attribute.get.dataType match {
        case s: StructType => s.zipWithIndex.map {
          case (f, i) =>
            val extract = GetStructField(attribute.get, i)
            Alias(extract, f.name)()
        }

        case _ =>
          throw new AnalysisException("Can only star expand struct data types. Attribute: `" +
            target.get + "`")
      }
    } else {
      val from = input.map(_.name).mkString(", ")
      val targetString = target.get.mkString(".")
      throw new AnalysisException(s"cannot resolve '$targetString.*' given input columns '$from'")
    }
  }

  override def toString: String = target.map(_ + ".").getOrElse("") + "*"
}

case class UnresolvedAlias(
  child: Expression,
  aliasFunc: Option[Expression => String] = None)
  extends UnaryExpression with NamedExpression with Unevaluable {

  override def toAttribute: Attribute = throw new UnresolvedException(this, "toAttribute")
  override def qualifier: Seq[String] = throw new UnresolvedException(this, "qualifier")
  override def exprId: Long = throw new UnresolvedException(this, "exprId")
  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def name: String = throw new UnresolvedException(this, "name")
  override def newInstance(): NamedExpression = throw new UnresolvedException(this, "newInstance")

  override lazy val resolved = false
}

/**
 * Holds the name of an attribute that has yet to be resolved.
 */
case class UnresolvedAttribute(nameParts: Seq[String]) extends Attribute with Unevaluable {

  def name: String =
    nameParts.map(n => if (n.contains(".")) s"`$n`" else n).mkString(".")

  override def exprId: Long = throw new UnresolvedException(this, "exprId")
  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def qualifier: Seq[String] = throw new UnresolvedException(this, "qualifier")
  override lazy val resolved = false

  override def newInstance(): UnresolvedAttribute = this
  override def withNullability(newNullability: Boolean): UnresolvedAttribute = this
  override def withQualifier(newQualifier: Seq[String]): UnresolvedAttribute = this
  override def withName(newName: String): UnresolvedAttribute = UnresolvedAttribute.quoted(newName)
  override def withExprId(newExprId: Long): UnresolvedAttribute = this

  override def toString: String = s"'$name"

  override def sql: String = name match {
    case ParserUtils.escapedIdentifier(_) | ParserUtils.qualifiedEscapedIdentifier(_, _) => name
    case _ => quoteIdentifier(name)
  }
}

object UnresolvedAttribute {
  /**
   * Creates an [[UnresolvedAttribute]], parsing segments separated by dots ('.').
   */
  def apply(name: String): UnresolvedAttribute = new UnresolvedAttribute(name.split("\\."))

  /**
   * Creates an [[UnresolvedAttribute]], from a single quoted string (for example using backticks in
   * HiveQL.  Since the string is consider quoted, no processing is done on the name.
   */
  def quoted(name: String): UnresolvedAttribute = new UnresolvedAttribute(Seq(name))

  /**
   * Creates an [[UnresolvedAttribute]] from a string in an embedded language.  In this case
   * we treat it as a quoted identifier, except for '.', which must be further quoted using
   * backticks if it is part of a column name.
   */
  def quotedString(name: String): UnresolvedAttribute =
    new UnresolvedAttribute(parseAttributeName(name))

  /**
   * Used to split attribute name by dot with backticks rule.
   * Backticks must appear in pairs, and the quoted string must be a complete name part,
   * which means `ab..c`e.f is not allowed.
   * We can use backtick only inside quoted name parts.
   */
  def parseAttributeName(name: String): Seq[String] = {
    def e = new AnalysisException(s"syntax error in attribute name: $name")
    val nameParts = scala.collection.mutable.ArrayBuffer.empty[String]
    val tmp = scala.collection.mutable.ArrayBuffer.empty[Char]
    var inBacktick = false
    var i = 0
    while (i < name.length) {
      val char = name(i)
      if (inBacktick) {
        if (char == '`') {
          if (i + 1 < name.length && name(i + 1) == '`') {
            tmp += '`'
            i += 1
          } else {
            inBacktick = false
            if (i + 1 < name.length && name(i + 1) != '.') throw e
          }
        } else {
          tmp += char
        }
      } else {
        if (char == '`') {
          if (tmp.nonEmpty) throw e
          inBacktick = true
        } else if (char == '.') {
          if (name(i - 1) == '.' || i == name.length - 1) throw e
          nameParts += tmp.mkString
          tmp.clear()
        } else {
          tmp += char
        }
      }
      i += 1
    }
    if (inBacktick) throw e
    nameParts += tmp.mkString
    nameParts.toSeq
  }
}

/**
 * Extracts a value or values from an Expression
 *
 * @param child The expression to extract value from,
 *              can be Map, Array, Struct or array of Structs.
 * @param extraction The expression to describe the extraction,
 *                   can be key of Map, index of Array, field name of Struct.
 */
case class UnresolvedExtractValue(child: Expression, extraction: Expression)
  extends BinaryExpression with Unevaluable {

  override def left: Expression = child
  override def right: Expression = extraction

  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override lazy val resolved = false

  override def toString: String = s"$child[$extraction]"
  override def sql: String = s"${child.sql}[${extraction.sql}]"
}
