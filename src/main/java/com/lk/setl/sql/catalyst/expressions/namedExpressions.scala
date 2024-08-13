package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.Row
import com.lk.setl.sql.catalyst.analysis.UnresolvedAttribute
import com.lk.setl.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import com.lk.setl.sql.catalyst.util.quoteIdentifier
import com.lk.setl.sql.types.{DataType, LongType, NullType}

import java.util.{Objects, UUID}

object NamedExpression {
  private val curId = new java.util.concurrent.atomic.AtomicLong()
  private[expressions] val jvmId = UUID.randomUUID()

  def newExprId: Long = curId.getAndIncrement()
  def unapply(expr: NamedExpression): Option[(String, DataType)] = Some((expr.name, expr.dataType))
}

/**
 * An [[Expression]] that is named.
 */
trait NamedExpression extends Expression {

  /** We should never fold named expressions in order to not remove the alias. */
  override def foldable: Boolean = false

  def name: String
  def exprId: Long

  /**
   * Returns a dot separated fully qualified name for this attribute.  Given that there can be
   * multiple qualifiers, it is possible that there are other possible way to refer to this
   * attribute.
   */
  def qualifiedName: String = (qualifier :+ name).mkString(".")

  /**
   * Optional qualifier for the expression.
   * Qualifier can also contain the fully qualified information, for e.g, Sequence of string
   * containing the database and the table name
   *
   * For now, since we do not allow using original table name to qualify a column name once the
   * table is aliased, this can only be:
   *
   * 1. Empty Seq: when an attribute doesn't have a qualifier,
   *    e.g. top level attributes aliased in the SELECT clause, or column from a LocalRelation.
   * 2. Seq with a Single element: either the table name or the alias name of the table.
   * 3. Seq with 2 elements: database name and table name
   * 4. Seq with 3 elements: catalog name, database name and table name
   */
  def qualifier: Seq[String]

  def toAttribute: Attribute

  /** Returns a copy of this expression with a new `exprId`. */
  def newInstance(): NamedExpression

  protected def typeSuffix =
    if (resolved) {
      dataType match {
        case LongType => "L"
        case _ => ""
      }
    } else {
      ""
    }
}

abstract class Attribute extends LeafExpression with NamedExpression with NullIntolerant {

  @transient
  override lazy val references: AttributeSet = AttributeSet(this)

  def withQualifier(newQualifier: Seq[String]): Attribute
  def withName(newName: String): Attribute
  def withExprId(newExprId: Long): Attribute

  override def toAttribute: Attribute = this
  def newInstance(): Attribute

}

case class Alias(child: Expression, name: String)(
  val exprId: Long = NamedExpression.newExprId,
  val qualifier: Seq[String] = Seq.empty)
  extends UnaryExpression with NamedExpression {
  // Alias(Generator, xx) need to be transformed into Generate(generator, ...)
  override lazy val resolved =
    childrenResolved && checkInputDataTypes().isSuccess // && !child.isInstanceOf[Generator]

  override def eval(input: Row): Any = child.eval(input)

  /** Just a simple passthrough for code generation. */
  override def genCode(ctx: CodegenContext): ExprCode = child.genCode(ctx)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    throw new IllegalStateException("Alias.doGenCode should not be called.")
  }

  override def dataType: DataType = child.dataType

  def newInstance(): NamedExpression =
    Alias(child, name)(
      qualifier = qualifier)

  override def toAttribute: Attribute = {
    if (resolved) {
      AttributeReference(name, child.dataType)(exprId, qualifier)
    } else {
      UnresolvedAttribute(name)
    }
  }
  override def toString: String = s"$child AS $name#${exprId}$typeSuffix"

  override protected final def otherCopyArgs: Seq[AnyRef] = {
    exprId.asInstanceOf[java.lang.Long] :: qualifier  :: Nil
  }

  override def hashCode(): Int = {
    val state = Seq(name, exprId, child, qualifier)
    state.map(Objects.hashCode).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def equals(other: Any): Boolean = other match {
    case a: Alias =>
      name == a.name && exprId == a.exprId && child == a.child && qualifier == a.qualifier
    case _ => false
  }
}

/**
 * A reference to an attribute produced by another operator in the tree.
 *
 * @param name The name of this attribute, should only be used during analysis or for debugging.
 * @param dataType The [[DataType]] of this attribute.
 * @param nullable True if null is a valid value for this attribute.
 * @param metadata The metadata of this attribute.
 * @param exprId A globally unique id used to check if different AttributeReferences refer to the
 *               same attribute.
 * @param qualifier An optional string that can be used to referred to this attribute in a fully
 *                  qualified way. Consider the examples tableName.name, subQueryAlias.name.
 *                  tableName and subQueryAlias are possible qualifiers.
 */
case class AttributeReference(
  name: String,
  dataType: DataType,
  override val nullable: Boolean = true)(
  val exprId: Long = NamedExpression.newExprId,
  val qualifier: Seq[String] = Seq.empty[String])
  extends Attribute with Unevaluable {
  var test = 1
  /**
   * Returns true iff the expression id is the same for both attributes.
   */
  def sameRef(other: AttributeReference): Boolean = this.exprId == other.exprId

  override def equals(other: Any): Boolean = other match {
    case ar: AttributeReference =>
      name == ar.name && dataType == ar.dataType && exprId == ar.exprId && qualifier == ar.qualifier
    case _ => false
  }


  override def hashCode: Int = {
    // See http://stackoverflow.com/questions/113511/hash-code-implementation
    var h = 17
    h = h * 37 + name.hashCode()
    h = h * 37 + dataType.hashCode()
    h = h * 37 + exprId.hashCode()
    h = h * 37 + qualifier.hashCode()
    h
  }

  override def newInstance(): AttributeReference = AttributeReference(name, dataType)(qualifier = qualifier)


  override def withName(newName: String): AttributeReference = {
    if (name == newName) {
      this
    } else {
      AttributeReference(newName, dataType)(exprId, qualifier)
    }
  }

  /**
   * Returns a copy of this [[AttributeReference]] with new qualifier.
   */
  override def withQualifier(newQualifier: Seq[String]): AttributeReference = {
    if (newQualifier == qualifier) {
      this
    } else {
      AttributeReference(name, dataType)(exprId, newQualifier)
    }
  }

  override def withExprId(newExprId: Long): AttributeReference = {
    if (exprId == newExprId) {
      this
    } else {
      AttributeReference(name, dataType)(newExprId, qualifier)
    }
  }

  override protected final def otherCopyArgs: Seq[AnyRef] = {
    exprId.asInstanceOf[java.lang.Long] :: qualifier :: Nil
  }

  override def toString: String = s"$name#${exprId}$typeSuffix"

  // Since the expression id is not in the first constructor it is missing from the default
  // tree string.
  override def simpleString(maxFields: Int): String = {
    s"$name#${exprId}: ${dataType.simpleString(maxFields)}"
  }

  override def sql: String = {
    val qualifierPrefix = if (qualifier.nonEmpty) qualifier.mkString(".") + "." else ""
    s"$qualifierPrefix${quoteIdentifier(name)}"
  }
}

/**
 * A place holder used when printing expressions without debugging information such as the
 * expression id or the unresolved indicator.
 */
case class PrettyAttribute(
  name: String,
  dataType: DataType = NullType)
  extends Attribute with Unevaluable {

  def this(attribute: Attribute) = this(attribute.name, attribute match {
    case a: AttributeReference => a.dataType
    case a: PrettyAttribute => a.dataType
    case _ => NullType
  })

  override def toString: String = name
  override def sql: String = toString

  override def newInstance(): Attribute = throw new UnsupportedOperationException
  override def withQualifier(newQualifier: Seq[String]): Attribute =
    throw new UnsupportedOperationException
  override def withName(newName: String): Attribute = throw new UnsupportedOperationException
  override def qualifier: Seq[String] = throw new UnsupportedOperationException
  override def exprId: Long = throw new UnsupportedOperationException
  override def withExprId(newExprId: Long): Attribute =
    throw new UnsupportedOperationException
}


