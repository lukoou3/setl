package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.{AnalysisException, ArrayData, Row}
import com.lk.setl.sql.catalyst.analysis.Resolver
import com.lk.setl.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import com.lk.setl.sql.catalyst.util.quoteIdentifier
import com.lk.setl.sql.types._

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines all the expressions to extract values out of complex types.
// For example, getting a field out of an array, map, or struct.
////////////////////////////////////////////////////////////////////////////////////////////////////


object ExtractValue {
  /**
   * Returns the resolved `ExtractValue`. It will return one kind of concrete `ExtractValue`,
   * depend on the type of `child` and `extraction`.
   *
   *   `child`      |    `extraction`    |    concrete `ExtractValue`
   * ----------------------------------------------------------------
   *    Struct      |   Literal String   |        GetStructField
   * Array[Struct]  |   Literal String   |     GetArrayStructFields
   *    Array       |   Integral type    |         GetArrayItem
   *     Map        |   map key type     |         GetMapValue
   */
  def apply(
      child: Expression,
      extraction: Expression,
      resolver: Resolver): Expression = {

    (child.dataType, extraction) match {
      case (StructType(fields), NonNullLiteral(v, StringType)) =>
        val fieldName = v.toString
        val ordinal = findField(fields, fieldName, resolver)
        GetStructField(child, ordinal, Some(fieldName))
      case (_: ArrayType, _) => GetArrayItem(child, extraction)
      case (otherType, _) =>
        val errorMsg = otherType match {
          case StructType(_) =>
            s"Field name should be String Literal, but it's $extraction"
          case other =>
            s"Can't extract value from $child: need struct type but got ${other.catalogString}"
        }
        throw new AnalysisException(errorMsg)
    }
  }

  /**
   * Find the ordinal of StructField, report error if no desired field or over one
   * desired fields are found.
   */
  private def findField(fields: Array[StructField], fieldName: String, resolver: Resolver): Int = {
    val checkField = (f: StructField) => resolver(f.name, fieldName)
    val ordinal = fields.indexWhere(checkField)
    if (ordinal == -1) {
      throw new AnalysisException(
        s"No such struct field $fieldName in ${fields.map(_.name).mkString(", ")}")
    } else if (fields.indexWhere(checkField, ordinal + 1) != -1) {
      throw new AnalysisException(
        s"Ambiguous reference to fields ${fields.filter(checkField).mkString(", ")}")
    } else {
      ordinal
    }
  }
}

trait ExtractValue extends Expression

/**
 * Returns the value of fields in the Struct `child`.
 *
 * No need to do type checking since it is handled by [[ExtractValue]].
 *
 * Note that we can pass in the field name directly to keep case preserving in `toString`.
 * For example, when get field `yEAr` from `<year: int, month: int>`, we should pass in `yEAr`.
 */
case class GetStructField(child: Expression, ordinal: Int, name: Option[String] = None)
  extends UnaryExpression with ExtractValue with NullIntolerant {

  lazy val childSchema = child.dataType.asInstanceOf[StructType]

  override def dataType: DataType = childSchema(ordinal).dataType

  override def toString: String = {
    val fieldName = if (resolved) childSchema(ordinal).name else s"_$ordinal"
    s"$child.${name.getOrElse(fieldName)}"
  }

  def extractFieldName: String = name.getOrElse(childSchema(ordinal).name)

  override def sql: String =
    child.sql + s".${quoteIdentifier(extractFieldName)}"

  protected override def nullSafeEval(input: Any): Any =
    input.asInstanceOf[Row].get(ordinal)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, eval => {
      if (true) {
        s"""
          if ($eval.isNullAt($ordinal)) {
            ${ev.isNull} = true;
          } else {
            ${ev.value} = ${CodeGenerator.getValue(eval, dataType, ordinal.toString)};
          }
        """
      } else {
        s"""
          ${ev.value} = ${CodeGenerator.getValue(eval, dataType, ordinal.toString)};
        """
      }
    })
  }
}



/**
 * Returns the field at `ordinal` in the Array `child`.
 *
 * We need to do type checking here as `ordinal` expression maybe unresolved.
 */
case class GetArrayItem(
    child: Expression,
    ordinal: Expression,
    failOnError: Boolean = false)
  extends BinaryExpression with ExpectsInputTypes with ExtractValue
  with NullIntolerant {

  def this(child: Expression, ordinal: Expression) = this(child, ordinal, false)

  // We have done type checking for child in `ExtractValue`, so only need to check the `ordinal`.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, IntegralType)

  override def toString: String = s"$child[$ordinal]"
  override def sql: String = s"${child.sql}[${ordinal.sql}]"

  override def left: Expression = child
  override def right: Expression = ordinal
  override def dataType: DataType = child.dataType.asInstanceOf[ArrayType].elementType

  protected override def nullSafeEval(value: Any, ordinal: Any): Any = {
    val baseValue = value.asInstanceOf[ArrayData]
    val index = ordinal.asInstanceOf[Number].intValue()
    if (index >= baseValue.numElements() || index < 0) {
      if (failOnError) {
        throw new ArrayIndexOutOfBoundsException(
          s"Invalid index: $index, numElements: ${baseValue.numElements()}")
      } else {
        null
      }
    } else if (baseValue.isNullAt(index)) {
      null
    } else {
      baseValue.get(index)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      val index = ctx.freshName("index")
      val nullCheck = if (child.dataType.asInstanceOf[ArrayType].containsNull) {
        s"""else if ($eval1.isNullAt($index)) {
               ${ev.isNull} = true;
            }
         """
      } else {
        ""
      }

      val indexOutOfBoundBranch = if (failOnError) {
        s"""throw new ArrayIndexOutOfBoundsException(
           |  "Invalid index: " + $index + ", numElements: " + $eval1.numElements()
           |);
         """.stripMargin
      } else {
        s"${ev.isNull} = true;"
      }

      s"""
        final int $index = (int) $eval2;
        if ($index >= $eval1.numElements() || $index < 0) {
          $indexOutOfBoundBranch
        } $nullCheck else {
          ${ev.value} = ${CodeGenerator.getValue(eval1, dataType, index)};
        }
      """
    })
  }
}

