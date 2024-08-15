package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.{AnalysisException, Row}
import com.lk.setl.sql.catalyst.CatalystTypeConverters
import com.lk.setl.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, JavaCode}
import com.lk.setl.sql.types._
import com.lk.setl.util.Utils

import java.lang.{Boolean => JavaBoolean}
import java.lang.{Character => JavaChar}
import java.lang.{Double => JavaDouble}
import java.lang.{Float => JavaFloat}
import java.lang.{Integer => JavaInteger}
import java.lang.{Long => JavaLong}
import java.util
import java.util.Objects

object Literal {
  val TrueLiteral: Literal = Literal(true, BooleanType)

  val FalseLiteral: Literal = Literal(false, BooleanType)

  def apply(v: Any): Literal = v match {
    case i: Int => Literal(i, IntegerType)
    case l: Long => Literal(l, LongType)
    case d: Double => Literal(d, DoubleType)
    case f: Float => Literal(f, FloatType)
    case s: String => Literal(s, StringType)
    case b: Boolean => Literal(b, BooleanType)
    case null => Literal(null, NullType)
    case v: Literal => v
    case _ =>
      throw new RuntimeException("Unsupported literal type " + v.getClass + " " + v)
  }

  /**
   * Returns the Spark SQL DataType for a given class object. Since this type needs to be resolved
   * in runtime, we use match-case idioms for class objects here. However, there are similar
   * functions in other files (e.g., HiveInspectors), so these functions need to merged into one.
   */
  private[this] def componentTypeToDataType(clz: Class[_]): DataType = clz match {
    // primitive types
    case JavaInteger.TYPE => IntegerType
    case JavaLong.TYPE => LongType
    case JavaDouble.TYPE => DoubleType
    case JavaFloat.TYPE => FloatType
    case JavaBoolean.TYPE => BooleanType
    case JavaChar.TYPE => StringType

    // java classes
    case _ if clz == classOf[JavaInteger] => IntegerType
    case _ if clz == classOf[JavaLong] => LongType
    case _ if clz == classOf[JavaDouble] => DoubleType
    case _ if clz == classOf[JavaFloat] => FloatType
    case _ if clz == classOf[JavaBoolean] => BooleanType

    // other scala classes
    case _ if clz == classOf[String] => StringType

    case _ => throw new AnalysisException(s"Unsupported component type $clz in arrays")
  }

  /**
   * Constructs a [[Literal]] of [[ObjectType]], for example when you need to pass an object
   * into code generation.
   */
  def fromObject(obj: Any, objType: DataType): Literal = new Literal(obj, objType)

  def create(v: Any, dataType: DataType): Literal = {
    Literal(CatalystTypeConverters.convertToCatalyst(v), dataType)
  }

  /**
   * Create a literal with default value for given DataType
   */
  def default(dataType: DataType): Literal = dataType match {
    case NullType => create(null, NullType)
    case BooleanType => Literal(false)
    case IntegerType => Literal(0)
    case LongType => Literal(0L)
    case FloatType => Literal(0.0f)
    case DoubleType => Literal(0.0)
    case StringType => Literal("")
    case other =>
      throw new RuntimeException(s"no default for type $dataType")
  }

  private[expressions] def validateLiteralValue(value: Any, dataType: DataType): Unit = {
    def doValidate(v: Any, dataType: DataType): Boolean = dataType match {
      case _ if v == null => true
      case BooleanType => v.isInstanceOf[Boolean]
      case IntegerType => v.isInstanceOf[Int]
      case LongType => v.isInstanceOf[Long]
      case FloatType => v.isInstanceOf[Float]
      case DoubleType => v.isInstanceOf[Double]
      case StringType => v.isInstanceOf[String]
      case st: StructType =>
        v.isInstanceOf[Row] && {
          val row = v.asInstanceOf[Row]
          st.fields.map(_.dataType).zipWithIndex.forall {
            case (dt, i) => doValidate(row.get(i), dt)
          }
        }
      case _ => false
    }
    require(doValidate(value, dataType),
      s"Literal must have a corresponding value to ${dataType.catalogString}, " +
      s"but class ${Utils.getSimpleName(value.getClass)} found.")
  }
}

/**
 * An extractor that matches non-null literal values
 */
object NonNullLiteral {
  def unapply(literal: Literal): Option[(Any, DataType)] = {
    Option(literal.value).map(_ => (literal.value, literal.dataType))
  }
}

/**
 * Extractor for retrieving Float literals.
 */
object FloatLiteral {
  def unapply(a: Any): Option[Float] = a match {
    case Literal(a: Float, FloatType) => Some(a)
    case _ => None
  }
}

/**
 * Extractor for retrieving Double literals.
 */
object DoubleLiteral {
  def unapply(a: Any): Option[Double] = a match {
    case Literal(a: Double, DoubleType) => Some(a)
    case _ => None
  }
}

/**
 * Extractor for retrieving Int literals.
 */
object IntegerLiteral {
  def unapply(a: Any): Option[Int] = a match {
    case Literal(a: Int, IntegerType) => Some(a)
    case _ => None
  }
}

/**
 * Extractor for retrieving String literals.
 */
object StringLiteral {
  def unapply(a: Any): Option[String] = a match {
    case Literal(s: String, StringType) => Some(s)
    case _ => None
  }
}

/**
 * 字面量类型, 叶子节点, eval方法直接返回value
 * 要做类型检查, 使用Literal.create()代替使用构造器
 * In order to do type checking, use Literal.create() instead of constructor
 */
case class Literal (value: Any, dataType: DataType) extends LeafExpression {

  //类型检查
  Literal.validateLiteralValue(value, dataType)

  override def foldable: Boolean = true

  // 这个还是有必要重写的，代码生成能减少if判断分支
  override def nullable: Boolean = value == null

  override def toString: String = value match {
    case null => "null"
    case other => other.toString
  }

  override def hashCode(): Int = {
    val valueHashCode = value match {
      case null => 0
      case binary: Array[Byte] => util.Arrays.hashCode(binary)
      case other => other.hashCode()
    }
    31 * Objects.hashCode(dataType) + valueHashCode
  }

  override def equals(other: Any): Boolean = other match {
    case o: Literal if !dataType.equals(o.dataType) => false
    case o: Literal =>
      (value, o.value) match {
        case (null, null) => true
        case (a: Array[Byte], b: Array[Byte]) => util.Arrays.equals(a, b)
        case (a: Double, b: Double) if a.isNaN && b.isNaN => true
        case (a: Float, b: Float) if a.isNaN && b.isNaN => true
        case (a, b) => a != null && a == b
      }
    case _ => false
  }

  override def eval(input: Row): Any = value

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // 返回spark sql DataType对应的java类型字符串, DateType,TimestampType底层是用int,long表示的
    val javaType = CodeGenerator.javaType(dataType)
    if (value == null) {
      ExprCode.forNullValue(dataType)
    } else {
      def toExprCode(code: String): ExprCode = {
        ExprCode.forNonNullValue(JavaCode.literal(code, dataType))
      }
      dataType match {
        case BooleanType | IntegerType =>
          toExprCode(value.toString)
        case FloatType =>
          value.asInstanceOf[Float] match {
            case v if v.isNaN =>
              toExprCode("Float.NaN")
            case Float.PositiveInfinity =>
              toExprCode("Float.POSITIVE_INFINITY")
            case Float.NegativeInfinity =>
              toExprCode("Float.NEGATIVE_INFINITY")
            case _ =>
              toExprCode(s"${value}F")
          }
        case DoubleType =>
          value.asInstanceOf[Double] match {
            case v if v.isNaN =>
              toExprCode("Double.NaN")
            case Double.PositiveInfinity =>
              toExprCode("Double.POSITIVE_INFINITY")
            case Double.NegativeInfinity =>
              toExprCode("Double.NEGATIVE_INFINITY")
            case _ =>
              toExprCode(s"${value}D")
          }
        case LongType =>
          toExprCode(s"${value}L")
        case _ =>
          val constRef = ctx.addReferenceObj("literal", value, javaType)
          ExprCode.forNonNullValue(JavaCode.global(constRef, dataType))
      }
    }
  }

  override def sql: String = (value, dataType) match {
    case (_, NullType | _: ArrayType | _: StructType) if value == null => "NULL"
    case _ if value == null => s"CAST(NULL AS ${dataType.sql})"
    case (v: String, StringType) =>
      // Escapes all backslashes and single quotes.
      "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'"
    case (v: Long, LongType) => v + "L"
    // Float type doesn't have a suffix
    case (v: Float, FloatType) =>
      val castedValue = v match {
        case _ if v.isNaN => "'NaN'"
        case Float.PositiveInfinity => "'Infinity'"
        case Float.NegativeInfinity => "'-Infinity'"
        case _ => s"'$v'"
      }
      s"CAST($castedValue AS ${FloatType.sql})"
    case (v: Double, DoubleType) =>
      v match {
        case _ if v.isNaN => s"CAST('NaN' AS ${DoubleType.sql})"
        case Double.PositiveInfinity => s"CAST('Infinity' AS ${DoubleType.sql})"
        case Double.NegativeInfinity => s"CAST('-Infinity' AS ${DoubleType.sql})"
        case _ => v + "D"
      }
    case _ => value.toString
  }
}
