package com.lk.setl.sql.catalyst.expressions

import java.util.Locale
import com.lk.setl.sql.{ArrayData, GenericArrayData, GenericRow, Row}
import com.lk.setl.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import com.lk.setl.sql.catalyst.expressions.codegen._
import com.lk.setl.sql.catalyst.expressions.codegen.Block._
import com.lk.setl.sql.catalyst.util.DateTimeConstants._
import com.lk.setl.sql.catalyst.util.DateTimeUtils._
import com.lk.setl.sql.catalyst.util.DateTimeUtils
import com.lk.setl.sql.catalyst.util._
import com.lk.setl.sql.types._

import java.nio.charset.StandardCharsets
import java.time.ZoneId
import java.util.concurrent.TimeUnit.SECONDS

object Cast {

  /**
   * Returns true iff we can cast `from` type to `to` type.
   */
  def canCast(from: DataType, to: DataType): Boolean = (from, to) match {
    case (fromType, toType) if fromType == toType => true

    case (NullType, _) => true

    case (_, StringType) => true

    case (StringType, BinaryType) => true
    case (_: IntegralType, BinaryType) => true

    case (StringType, BooleanType) => true
    case (DateType, BooleanType) => true
    case (TimestampType, BooleanType) => true
    case (_: NumericType, BooleanType) => true

    case (StringType, TimestampType) => true
    case (BooleanType, TimestampType) => true
    case (DateType, TimestampType) => true
    case (_: NumericType, TimestampType) => true

    case (StringType, DateType) => true
    case (TimestampType, DateType) => true

    case (StringType, CalendarIntervalType) => true

    case (StringType, _: NumericType) => true
    case (BooleanType, _: NumericType) => true
    case (DateType, _: NumericType) => true
    case (TimestampType, _: NumericType) => true
    case (_: NumericType, _: NumericType) => true

    case (ArrayType(fromType, fn), ArrayType(toType, tn)) =>
      canCast(fromType, toType) &&
        resolvableNullability(fn || forceNullable(fromType, toType), tn)

    case (StructType(fromFields), StructType(toFields)) =>
      fromFields.length == toFields.length &&
        fromFields.zip(toFields).forall {
          case (fromField, toField) =>
            canCast(fromField.dataType, toField.dataType)
        }

    case _ => false
  }

  /**
   * Return true if we need to use the `timeZone` information casting `from` type to `to` type.
   * The patterns matched reflect the current implementation in the Cast node.
   * c.f. usage of `timeZone` in:
   * * Cast.castToString
   * * Cast.castToDate
   * * Cast.castToTimestamp
   */
  def needsTimeZone(from: DataType, to: DataType): Boolean = (from, to) match {
    case (StringType, TimestampType | DateType) => true
    case (TimestampType | DateType, StringType) => true
    case (DateType, TimestampType) => true
    case (TimestampType, DateType) => true
    case (ArrayType(fromType, _), ArrayType(toType, _)) => needsTimeZone(fromType, toType)
    case (StructType(fromFields), StructType(toFields)) =>
      fromFields.length == toFields.length &&
        fromFields.zip(toFields).exists {
          case (fromField, toField) =>
            needsTimeZone(fromField.dataType, toField.dataType)
        }
    case _ => false
  }

  /**
   * 如果我们可以安全地将from类型向上转换为to类型，而不会发生任何截断、精度损失或可能的运行时故障，则返回true。
   * 例如，long->int、string->int都不是向上转换的。
   * Returns true iff we can safely up-cast the `from` type to `to` type without any truncating or
   * precision lose or possible runtime failures. For example, long -> int, string -> int are not
   * up-cast.
   */
  def canUpCast(from: DataType, to: DataType): Boolean = (from, to) match {
    case _ if from == to => true
    case (f, t) if legalNumericPrecedence(f, t) => true
    case (DateType, TimestampType) => true
    case (_: AtomicType, StringType) => true
    case (_: CalendarIntervalType, StringType) => true
    case (NullType, _) => true

    // Spark supports casting between long and timestamp, please see `longToTimestamp` and
    // `timestampToLong` for details.
    case (TimestampType, LongType) => true
    case (LongType, TimestampType) => true

    case (ArrayType(fromType, fn), ArrayType(toType, tn)) =>
      resolvableNullability(fn, tn) && canUpCast(fromType, toType)

    case (StructType(fromFields), StructType(toFields)) =>
      fromFields.length == toFields.length &&
        fromFields.zip(toFields).forall {
          case (f1, f2) => canUpCast(f1.dataType, f2.dataType)
        }

    case _ => false
  }

  /**
   * 如果我们可以根据ANSI SQL将from类型转换为to类型，则返回true。在实践中，行为与PostgreSQL基本相同。
   * 它不允许某些不合理的类型转换，例如将字符串转换为int或将double转换为boolean。
   * Returns true iff we can cast the `from` type to `to` type as per the ANSI SQL.
   * In practice, the behavior is mostly the same as PostgreSQL. It disallows certain unreasonable
   * type conversions such as converting `string` to `int` or `double` to `boolean`.
   */
  def canANSIStoreAssign(from: DataType, to: DataType): Boolean = (from, to) match {
    case _ if from == to => true
    case (NullType, _) => true
    case (_: NumericType, _: NumericType) => true
    case (_: AtomicType, StringType) => true
    case (_: CalendarIntervalType, StringType) => true
    case (DateType, TimestampType) => true
    case (TimestampType, DateType) => true

    case (ArrayType(fromType, fn), ArrayType(toType, tn)) =>
      resolvableNullability(fn, tn) && canANSIStoreAssign(fromType, toType)

    case (StructType(fromFields), StructType(toFields)) =>
      fromFields.length == toFields.length &&
        fromFields.zip(toFields).forall {
          case (f1, f2) => canANSIStoreAssign(f1.dataType, f2.dataType)
        }

    case _ => false
  }

  private def legalNumericPrecedence(from: DataType, to: DataType): Boolean = {
    val fromPrecedence = TypeCoercion.numericPrecedence.indexOf(from)
    val toPrecedence = TypeCoercion.numericPrecedence.indexOf(to)
    fromPrecedence >= 0 && fromPrecedence < toPrecedence
  }


  /**
   * 如果从类型到类型强制转换不可为null的值可能返回null，则返回true。
   * 请注意，调用方应首先考虑输入的可空性，只有在输入不可空的情况下才调用此方法。
   * Returns `true` if casting non-nullable values from `from` type to `to` type
   * may return null. Note that the caller side should take care of input nullability
   * first and only call this method if the input is not nullable.
   */
  def forceNullable(from: DataType, to: DataType): Boolean = (from, to) match {
    case (NullType, _) => false // empty array or map case
    case (_, _) if from == to => false

    case (StringType, BinaryType) => false
    case (StringType, _) => true
    case (_, StringType) => false

    case (FloatType | DoubleType, TimestampType) => true
    case (TimestampType, DateType) => false
    case (_, DateType) => true
    case (DateType, TimestampType) => false
    case (DateType, _) => true
    case (_, CalendarIntervalType) => true

    case (_: FractionalType, _: IntegralType) => true  // NaN, infinity
    case _ => false
  }

  def resolvableNullability(from: Boolean, to: Boolean): Boolean = !from || to

  /**
   * We process literals such as 'Infinity', 'Inf', '-Infinity' and 'NaN' etc in case
   * insensitive manner to be compatible with other database systems such as PostgreSQL and DB2.
   */
  def processFloatingPointSpecialLiterals(v: String, isFloat: Boolean): Any = {
    v.trim.toLowerCase(Locale.ROOT) match {
      case "inf" | "+inf" | "infinity" | "+infinity" =>
        if (isFloat) Float.PositiveInfinity else Double.PositiveInfinity
      case "-inf" | "-infinity" =>
        if (isFloat) Float.NegativeInfinity else Double.NegativeInfinity
      case "nan" =>
        if (isFloat) Float.NaN else Double.NaN
      case _ => null
    }
  }
}

abstract class CastBase extends UnaryExpression with TimeZoneAwareExpression with NullIntolerant {

  def child: Expression

  def dataType: DataType

  /**
   * Returns true iff we can cast `from` type to `to` type.
   */
  def canCast(from: DataType, to: DataType): Boolean

  /**
   * Returns the error message if casting from one type to another one is invalid.
   */
  def typeCheckFailureMessage: String

  override def toString: String = {
    val ansi = if (ansiEnabled) "ansi_" else ""
    s"${ansi}cast($child as ${dataType.simpleString})"
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (canCast(child.dataType, dataType)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(typeCheckFailureMessage)
    }
  }

  override def nullable: Boolean = child.nullable || Cast.forceNullable(child.dataType, dataType)

  protected def ansiEnabled: Boolean

  // When this cast involves TimeZone, it's only resolved if the timeZoneId is set;
  // Otherwise behave like Expression.resolved.
  override lazy val resolved: Boolean =
    childrenResolved && checkInputDataTypes().isSuccess && (!needsTimeZone || timeZoneId.isDefined)

  def needsTimeZone: Boolean = Cast.needsTimeZone(child.dataType, dataType)

  // [[func]] assumes the input is no longer null because eval already does the null check.
  @inline private[this] def buildCast[T](a: Any, func: T => Any): Any = func(a.asInstanceOf[T])

  private lazy val dateFormatter = DateFormatter(zoneId)
  private lazy val timestampFormatter = TimestampFormatter.getFractionFormatter(zoneId)

  private val legacyCastToStr = false // SQLConf.get.getConf(SQLConf.LEGACY_COMPLEX_TYPES_TO_STRING)
  // The brackets that are used in casting structs and maps to strings
  private val (leftBracket, rightBracket) = if (legacyCastToStr) ("[", "]") else ("{", "}")

  // UDFToString
  private[this] def castToString(from: DataType): Any => Any = from match {
    case BinaryType => b => new String(b.asInstanceOf[Array[Byte]], StandardCharsets.UTF_8)
    case DateType => buildCast[Int](_, d => dateFormatter.format(d))
    case TimestampType => buildCast[Long](_, t => timestampFormatter.format(t))
    case ArrayType(et, _) =>
      buildCast[ArrayData](_, array => {
        val builder = new java.lang.StringBuilder
        builder.append("[")
        if (array.numElements > 0) {
          val toUTF8String = castToString(et)
          if (array.isNullAt(0)) {
            if (!legacyCastToStr) builder.append("null")
          } else {
            builder.append(toUTF8String(array.get(0)).asInstanceOf[String])
          }
          var i = 1
          while (i < array.numElements) {
            builder.append(",")
            if (array.isNullAt(i)) {
              if (!legacyCastToStr) builder.append(" null")
            } else {
              builder.append(" ")
              builder.append(toUTF8String(array.get(i)).asInstanceOf[String])
            }
            i += 1
          }
        }
        builder.append("]")
        builder.toString()
      })
    case StructType(fields) =>
      buildCast[Row](_, row => {
        val builder = new java.lang.StringBuilder
        builder.append(leftBracket)
        if (row.numFields > 0) {
          val st = fields.map(_.dataType)
          val toUTF8StringFuncs = st.map(castToString)
          if (row.isNullAt(0)) {
            if (!legacyCastToStr) builder.append("null")
          } else {
            builder.append(toUTF8StringFuncs(0)(row.get(0)).asInstanceOf[String])
          }
          var i = 1
          while (i < row.numFields) {
            builder.append(",")
            if (row.isNullAt(i)) {
              if (!legacyCastToStr) builder.append(" null")
            } else {
              builder.append(" ")
              builder.append(toUTF8StringFuncs(i)(row.get(i)).asInstanceOf[String])
            }
            i += 1
          }
        }
        builder.append(rightBracket)
        builder.toString()
      })
    case _ => buildCast[Any](_, o => o.toString)
  }

  // BinaryConverter
  private[this] def castToBinary(from: DataType): Any => Any = from match {
    case StringType => buildCast[String](_, _.getBytes(StandardCharsets.UTF_8))
    case IntegerType => buildCast[Int](_, NumberConverter.toBinary)
    case LongType => buildCast[Long](_, NumberConverter.toBinary)
  }

  // UDFToBoolean
  private[this] def castToBoolean(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[String](_, s => {
        if (StringUtils.isTrueString(s)) {
          true
        } else if (StringUtils.isFalseString(s)) {
          false
        } else {
          null
        }
      })
    case TimestampType =>
      buildCast[Long](_, t => t != 0)
    case DateType =>
      // Hive would return null when cast from date to boolean
      buildCast[Int](_, d => null)
    case LongType =>
      buildCast[Long](_, _ != 0)
    case IntegerType =>
      buildCast[Int](_, _ != 0)
    case DoubleType =>
      buildCast[Double](_, _ != 0)
    case FloatType =>
      buildCast[Float](_, _ != 0)
  }

  // TimestampConverter
  private[this] def castToTimestamp(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[String](_, utfs => {
        if (ansiEnabled) {
          DateTimeUtils.stringToTimestampAnsi(utfs, zoneId)
        } else {
          DateTimeUtils.stringToTimestamp(utfs, zoneId).orNull
        }
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1L else 0)
    case LongType =>
      buildCast[Long](_, l => longToTimestamp(l))
    case IntegerType =>
      buildCast[Int](_, i => longToTimestamp(i.toLong))
    case DateType =>
      buildCast[Int](_, d => daysToMicros(d, zoneId))
    // TimestampWritable.doubleToTimestamp
    case DoubleType =>
      buildCast[Double](_, d => doubleToTimestamp(d))
    // TimestampWritable.floatToTimestamp
    case FloatType =>
      buildCast[Float](_, f => doubleToTimestamp(f.toDouble))
  }

  private[this] def doubleToTimestamp(d: Double): Any = {
    if (d.isNaN || d.isInfinite) null else (d * MICROS_PER_SECOND).toLong
  }

  // converting seconds to us
  private[this] def longToTimestamp(t: Long): Long = SECONDS.toMicros(t)
  // converting us to seconds
  private[this] def timestampToLong(ts: Long): Long = {
    Math.floorDiv(ts, MICROS_PER_SECOND)
  }
  // converting us to seconds in double
  private[this] def timestampToDouble(ts: Long): Double = {
    ts / MICROS_PER_SECOND.toDouble
  }

  // DateConverter
  private[this] def castToDate(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[String](_, s => DateTimeUtils.stringToDate(s, zoneId).orNull)
    case TimestampType =>
      // throw valid precision more than seconds, according to Hive.
      // Timestamp.nanos is in 0 to 999,999,999, no more than a second.
      buildCast[Long](_, t => microsToDays(t, zoneId))
  }

  // IntervalConverter
  private[this] def castToInterval(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[String](_, s => IntervalUtils.safeStringToInterval(s))
  }

  // LongConverter
  private[this] def castToLong(from: DataType): Any => Any = from match {
    case StringType if ansiEnabled =>
      buildCast[String](_, java.lang.Long.parseLong)
    case StringType =>
      buildCast[String](_, java.lang.Long.parseLong)
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1L else 0L)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Long](_, t => timestampToLong(t))
    case x: NumericType if ansiEnabled =>
      b => x.exactNumeric.asInstanceOf[Numeric[Any]].toLong(b)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toLong(b)
  }

  // IntConverter
  private[this] def castToInt(from: DataType): Any => Any = from match {
    case StringType if ansiEnabled =>
      buildCast[String](_, java.lang.Integer.parseInt)
    case StringType =>
      buildCast[String](_, java.lang.Integer.parseInt)
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1 else 0)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Long](_, t => timestampToLong(t).toInt)
    case x: NumericType if ansiEnabled =>
      b => x.exactNumeric.asInstanceOf[Numeric[Any]].toInt(b)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toInt(b)
  }

  // DoubleConverter
  private[this] def castToDouble(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[String](_, s => {
        val doubleStr = s
        try doubleStr.toDouble catch {
          case _: NumberFormatException =>
            val d = Cast.processFloatingPointSpecialLiterals(doubleStr, false)
            if(ansiEnabled && d == null) {
              throw new NumberFormatException(s"invalid input syntax for type numeric: $s")
            } else {
              d
            }
        }
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1d else 0d)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Long](_, t => timestampToDouble(t))
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toDouble(b)
  }

  // FloatConverter
  private[this] def castToFloat(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[String](_, s => {
        val floatStr = s
        try floatStr.toFloat catch {
          case _: NumberFormatException =>
            val f = Cast.processFloatingPointSpecialLiterals(floatStr, true)
            if (ansiEnabled && f == null) {
              throw new NumberFormatException(s"invalid input syntax for type numeric: $s")
            } else {
              f
            }
        }
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1f else 0f)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Long](_, t => timestampToDouble(t).toFloat)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toFloat(b)
  }

  private[this] def castArray(fromType: DataType, toType: DataType): Any => Any = {
    val elementCast = cast(fromType, toType)
    // TODO: Could be faster?
    buildCast[ArrayData](_, array => {
      val values = new Array[Any](array.numElements())
      var i = 0
      while (i < array.length){
        if (array.isNullAt(i)){
          values(i) = null
        } else {
          values(i) = elementCast(array.get(i))
        }
        i += 1
      }
      new GenericArrayData(values)
    })
  }



  private[this] def castStruct(from: StructType, to: StructType): Any => Any = {
    val castFuncs: Array[(Any) => Any] = from.fields.zip(to.fields).map {
      case (fromField, toField) => cast(fromField.dataType, toField.dataType)
    }
    // TODO: Could be faster?
    buildCast[Row](_, row => {
      val newRow = new GenericRow(Array[Any](from.fields.length))
      var i = 0
      while (i < row.numFields) {
        newRow.update(i,
          if (row.isNullAt(i)) null else castFuncs(i)(row.get(i)))
        i += 1
      }
      newRow
    })
  }

  private[this] def cast(from: DataType, to: DataType): Any => Any = {
    // If the cast does not change the structure, then we don't really need to cast anything.
    // We can return what the children return. Same thing should happen in the codegen path.
    if (DataType.equalsStructurally(from, to)) {
      identity
    } else if (from == NullType) {
      // According to `canCast`, NullType can be casted to any type.
      // For primitive types, we don't reach here because the guard of `nullSafeEval`.
      // But for nested types like struct, we might reach here for nested null type field.
      // We won't call the returned function actually, but returns a placeholder.
      _ => throw new RuntimeException(s"should not directly cast from NullType to $to.")
    } else {
      to match {
        case dt if dt == from => identity[Any]
        case StringType => castToString(from)
        case BinaryType => castToBinary(from)
        case DateType => castToDate(from)
        case TimestampType => castToTimestamp(from)
        case CalendarIntervalType => castToInterval(from)
        case BooleanType => castToBoolean(from)
        case IntegerType => castToInt(from)
        case FloatType => castToFloat(from)
        case LongType => castToLong(from)
        case DoubleType => castToDouble(from)
        case array: ArrayType =>
          castArray(from.asInstanceOf[ArrayType].elementType, array.elementType)
        case struct: StructType => castStruct(from.asInstanceOf[StructType], struct)
        case _ =>
          throw new RuntimeException(s"Cannot cast $from to $to.")
      }
    }
  }

  private[this] lazy val cast: Any => Any = cast(child.dataType, dataType)

  protected override def nullSafeEval(input: Any): Any = cast(input)

  override def genCode(ctx: CodegenContext): ExprCode = {
    // If the cast does not change the structure, then we don't really need to cast anything.
    // We can return what the children return. Same thing should happen in the interpreted path.
    if (DataType.equalsStructurally(child.dataType, dataType)) {
      child.genCode(ctx)
    } else {
      super.genCode(ctx)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    val nullSafeCast = nullSafeCastFunction(child.dataType, dataType, ctx)

    ev.copy(code = eval.code +
      castCode(ctx, eval.value, eval.isNull, ev.value, ev.isNull, dataType, nullSafeCast))
  }

  // The function arguments are: `input`, `result` and `resultIsNull`. We don't need `inputIsNull`
  // in parameter list, because the returned code will be put in null safe evaluation region.
  private[this] type CastFunction = (ExprValue, ExprValue, ExprValue) => Block

  private[this] def nullSafeCastFunction(
      from: DataType,
      to: DataType,
      ctx: CodegenContext): CastFunction = to match {

    case _ if from == NullType => (c, evPrim, evNull) => code"$evNull = true;"
    case _ if to == from => (c, evPrim, evNull) => code"$evPrim = $c;"
    case StringType => castToStringCode(from, ctx)
    case BinaryType => castToBinaryCode(from)
    case DateType => castToDateCode(from, ctx)
    case TimestampType => castToTimestampCode(from, ctx)
    case CalendarIntervalType => castToIntervalCode(from)
    case BooleanType => castToBooleanCode(from)
    case IntegerType => castToIntCode(from, ctx)
    case FloatType => castToFloatCode(from, ctx)
    case LongType => castToLongCode(from, ctx)
    case DoubleType => castToDoubleCode(from, ctx)

    case array: ArrayType =>
      castArrayCode(from.asInstanceOf[ArrayType].elementType, array.elementType, ctx)
    case struct: StructType => castStructCode(from.asInstanceOf[StructType], struct, ctx)
    case _ =>
      throw new RuntimeException(s"Cannot cast $from to $to.")
  }

  // Since we need to cast input expressions recursively inside ComplexTypes, such as Map's
  // Key and Value, Struct's field, we need to name out all the variable names involved in a cast.
  private[this] def castCode(ctx: CodegenContext, input: ExprValue, inputIsNull: ExprValue,
    result: ExprValue, resultIsNull: ExprValue, resultType: DataType, cast: CastFunction): Block = {
    val javaType = JavaCode.javaType(resultType)
    code"""
      boolean $resultIsNull = $inputIsNull;
      $javaType $result = ${CodeGenerator.defaultValue(resultType)};
      if (!$inputIsNull) {
        ${cast(input, result, resultIsNull)}
      }
    """
  }

  private def appendIfNotLegacyCastToStr(buffer: ExprValue, s: String): Block = {
    if (!legacyCastToStr) code"""$buffer.append("$s");""" else EmptyBlock
  }

  private def writeArrayToStringBuilder(
      et: DataType,
      array: ExprValue,
      buffer: ExprValue,
      ctx: CodegenContext): Block = {
    val elementToStringCode = castToStringCode(et, ctx)
    val funcName = ctx.freshName("elementToString")
    val element = JavaCode.variable("element", et)
    val elementStr = JavaCode.variable("elementStr", StringType)
    val elementToStringFunc = inline"${ctx.addNewFunction(funcName,
      s"""
         |private UTF8String $funcName(${CodeGenerator.javaType(et)} $element) {
         |  UTF8String $elementStr = null;
         |  ${elementToStringCode(element, elementStr, null /* resultIsNull won't be used */)}
         |  return elementStr;
         |}
       """.stripMargin)}"

    val loopIndex = ctx.freshVariable("loopIndex", IntegerType)
    code"""
       |$buffer.append("[");
       |if ($array.numElements() > 0) {
       |  if ($array.isNullAt(0)) {
       |    ${appendIfNotLegacyCastToStr(buffer, "null")}
       |  } else {
       |    $buffer.append($elementToStringFunc(${CodeGenerator.getValue(array, et, "0")}));
       |  }
       |  for (int $loopIndex = 1; $loopIndex < $array.numElements(); $loopIndex++) {
       |    $buffer.append(",");
       |    if ($array.isNullAt($loopIndex)) {
       |      ${appendIfNotLegacyCastToStr(buffer, " null")}
       |    } else {
       |      $buffer.append(" ");
       |      $buffer.append($elementToStringFunc(${CodeGenerator.getValue(array, et, loopIndex)}));
       |    }
       |  }
       |}
       |$buffer.append("]");
     """.stripMargin
  }

  private def writeMapToStringBuilder(
      kt: DataType,
      vt: DataType,
      map: ExprValue,
      buffer: ExprValue,
      ctx: CodegenContext): Block = {

    def dataToStringFunc(func: String, dataType: DataType) = {
      val funcName = ctx.freshName(func)
      val dataToStringCode = castToStringCode(dataType, ctx)
      val data = JavaCode.variable("data", dataType)
      val dataStr = JavaCode.variable("dataStr", StringType)
      val functionCall = ctx.addNewFunction(funcName,
        s"""
           |private UTF8String $funcName(${CodeGenerator.javaType(dataType)} $data) {
           |  UTF8String $dataStr = null;
           |  ${dataToStringCode(data, dataStr, null /* resultIsNull won't be used */)}
           |  return dataStr;
           |}
         """.stripMargin)
      inline"$functionCall"
    }

    val keyToStringFunc = dataToStringFunc("keyToString", kt)
    val valueToStringFunc = dataToStringFunc("valueToString", vt)
    val loopIndex = ctx.freshVariable("loopIndex", IntegerType)
    val mapKeyArray = JavaCode.expression(s"$map.keyArray()", classOf[ArrayData])
    val mapValueArray = JavaCode.expression(s"$map.valueArray()", classOf[ArrayData])
    val getMapFirstKey = CodeGenerator.getValue(mapKeyArray, kt, JavaCode.literal("0", IntegerType))
    val getMapFirstValue = CodeGenerator.getValue(mapValueArray, vt,
      JavaCode.literal("0", IntegerType))
    val getMapKeyArray = CodeGenerator.getValue(mapKeyArray, kt, loopIndex)
    val getMapValueArray = CodeGenerator.getValue(mapValueArray, vt, loopIndex)
    code"""
       |$buffer.append("$leftBracket");
       |if ($map.numElements() > 0) {
       |  $buffer.append($keyToStringFunc($getMapFirstKey));
       |  $buffer.append(" ->");
       |  if ($map.valueArray().isNullAt(0)) {
       |    ${appendIfNotLegacyCastToStr(buffer, " null")}
       |  } else {
       |    $buffer.append(" ");
       |    $buffer.append($valueToStringFunc($getMapFirstValue));
       |  }
       |  for (int $loopIndex = 1; $loopIndex < $map.numElements(); $loopIndex++) {
       |    $buffer.append(", ");
       |    $buffer.append($keyToStringFunc($getMapKeyArray));
       |    $buffer.append(" ->");
       |    if ($map.valueArray().isNullAt($loopIndex)) {
       |      ${appendIfNotLegacyCastToStr(buffer, " null")}
       |    } else {
       |      $buffer.append(" ");
       |      $buffer.append($valueToStringFunc($getMapValueArray));
       |    }
       |  }
       |}
       |$buffer.append("$rightBracket");
     """.stripMargin
  }

  private def writeStructToStringBuilder(
      st: Seq[DataType],
      row: ExprValue,
      buffer: ExprValue,
      ctx: CodegenContext): Block = {
    val structToStringCode = st.zipWithIndex.map { case (ft, i) =>
      val fieldToStringCode = castToStringCode(ft, ctx)
      val field = ctx.freshVariable("field", ft)
      val fieldStr = ctx.freshVariable("fieldStr", StringType)
      val javaType = JavaCode.javaType(ft)
      code"""
         |${if (i != 0) code"""$buffer.append(",");""" else EmptyBlock}
         |if ($row.isNullAt($i)) {
         |  ${appendIfNotLegacyCastToStr(buffer, if (i == 0) "null" else " null")}
         |} else {
         |  ${if (i != 0) code"""$buffer.append(" ");""" else EmptyBlock}
         |
         |  // Append $i field into the string buffer
         |  $javaType $field = ${CodeGenerator.getValue(row, ft, s"$i")};
         |  UTF8String $fieldStr = null;
         |  ${fieldToStringCode(field, fieldStr, null /* resultIsNull won't be used */)}
         |  $buffer.append($fieldStr);
         |}
       """.stripMargin
    }

    val writeStructCode = ctx.splitExpressions(
      expressions = structToStringCode.map(_.code),
      funcName = "fieldToString",
      arguments = ("Row", row.code) ::
        (classOf[java.lang.StringBuilder].getName, buffer.code) :: Nil)

    code"""
       |$buffer.append("$leftBracket");
       |$writeStructCode
       |$buffer.append("$rightBracket");
     """.stripMargin
  }

  private[this] def castToStringCode(from: DataType, ctx: CodegenContext): CastFunction = {
    from match {
      case BinaryType =>
        (c, evPrim, evNull) => code"$evPrim = new String($c, java.nio.charset.StandardCharsets.UTF_8);"
      case DateType =>
        val df = JavaCode.global(
          ctx.addReferenceObj("dateFormatter", dateFormatter),
          dateFormatter.getClass)
        (c, evPrim, evNull) => code"""$evPrim = $df.format($c);"""
      case TimestampType =>
        val tf = JavaCode.global(
          ctx.addReferenceObj("timestampFormatter", timestampFormatter),
          timestampFormatter.getClass)
        (c, evPrim, evNull) => code"""$evPrim = $tf.format($c);"""
      case CalendarIntervalType =>
        (c, evPrim, _) => code"""$evPrim = $c.toString();"""
      case ArrayType(et, _) =>
        (c, evPrim, evNull) => {
          val buffer = ctx.freshVariable("buffer", classOf[java.lang.StringBuilder])
          val bufferClass = JavaCode.javaType(classOf[java.lang.StringBuilder])
          val writeArrayElemCode = writeArrayToStringBuilder(et, c, buffer, ctx)
          code"""
             |$bufferClass $buffer = new $bufferClass();
             |$writeArrayElemCode;
             |$evPrim = $buffer.toString();
           """.stripMargin
        }
      case StructType(fields) =>
        (c, evPrim, evNull) => {
          val row = ctx.freshVariable("row", classOf[Row])
          val buffer = ctx.freshVariable("buffer", classOf[java.lang.StringBuilder])
          val bufferClass = JavaCode.javaType(classOf[java.lang.StringBuilder])
          val writeStructCode = writeStructToStringBuilder(fields.map(_.dataType), row, buffer, ctx)
          code"""
             |InternalRow $row = $c;
             |$bufferClass $buffer = new $bufferClass();
             |$writeStructCode
             |$evPrim = $buffer.build();
           """.stripMargin
        }
      case _ =>
        (c, evPrim, evNull) => code"$evPrim = String.valueOf($c);"
    }
  }

  private[this] def castToBinaryCode(from: DataType): CastFunction = from match {
    case StringType =>
      (c, evPrim, evNull) =>
        code"$evPrim = $c.getBytes(java.nio.charset.StandardCharsets.UTF_8);"
    case _: IntegralType =>
      (c, evPrim, evNull) =>
        code"$evPrim = ${NumberConverter.getClass.getName.stripSuffix("$")}.toBinary($c);"
  }

  private[this] def castToDateCode(
      from: DataType,
      ctx: CodegenContext): CastFunction = {
    def getZoneId() = {
      val zoneIdClass = classOf[ZoneId]
      JavaCode.global(
        ctx.addReferenceObj("zoneId", zoneId, zoneIdClass.getName),
        zoneIdClass)
    }
    val DateTimeUtilsClass = DateTimeUtils.getClass.getName.stripSuffix("$")
    from match {
      case StringType =>
        val intOpt = ctx.freshVariable("intOpt", classOf[Option[Integer]])
        val zid = getZoneId()
        (c, evPrim, evNull) =>
          code"""
          scala.Option<Integer> $intOpt =
            ${DateTimeUtilsClass}.stringToDate($c, $zid);
          if ($intOpt.isDefined()) {
            $evPrim = ((Integer) $intOpt.get()).intValue();
          } else {
            $evNull = true;
          }
         """
      case TimestampType =>
        val zid = getZoneId()
        (c, evPrim, evNull) =>
          code"""$evPrim =
            ${DateTimeUtilsClass}.microsToDays($c, $zid);"""
      case _ =>
        (c, evPrim, evNull) => code"$evNull = true;"
    }
  }

  private[this] def castToTimestampCode(
      from: DataType,
      ctx: CodegenContext): CastFunction = from match {
    case StringType =>
      val zoneIdClass = classOf[ZoneId]
      val zid = JavaCode.global(
        ctx.addReferenceObj("zoneId", zoneId, zoneIdClass.getName),
        zoneIdClass)
      val DateTimeUtilsClass = DateTimeUtils.getClass.getName.stripSuffix("$")
      val longOpt = ctx.freshVariable("longOpt", classOf[Option[Long]])
      (c, evPrim, evNull) =>
        if (ansiEnabled) {
          code"""
            $evPrim =
              ${DateTimeUtilsClass}.stringToTimestampAnsi($c, $zid);
           """
        } else {
          code"""
            scala.Option<Long> $longOpt =
              ${DateTimeUtilsClass}.stringToTimestamp($c, $zid);
            if ($longOpt.isDefined()) {
              $evPrim = ((Long) $longOpt.get()).longValue();
            } else {
              $evNull = true;
            }
           """
        }
    case BooleanType =>
      (c, evPrim, evNull) => code"$evPrim = $c ? 1L : 0L;"
    case _: IntegralType =>
      (c, evPrim, evNull) => code"$evPrim = ${longToTimeStampCode(c)};"
    case DateType =>
      val zoneIdClass = classOf[ZoneId]
      val DateTimeUtilsClass = DateTimeUtils.getClass.getName.stripSuffix("$")
      val zid = JavaCode.global(
        ctx.addReferenceObj("zoneId", zoneId, zoneIdClass.getName),
        zoneIdClass)
      (c, evPrim, evNull) =>
        code"""$evPrim =
          ${DateTimeUtilsClass}.daysToMicros($c, $zid);"""
    case DoubleType =>
      (c, evPrim, evNull) =>
        code"""
          if (Double.isNaN($c) || Double.isInfinite($c)) {
            $evNull = true;
          } else {
            $evPrim = (long)($c * $MICROS_PER_SECOND);
          }
        """
    case FloatType =>
      (c, evPrim, evNull) =>
        code"""
          if (Float.isNaN($c) || Float.isInfinite($c)) {
            $evNull = true;
          } else {
            $evPrim = (long)((double)$c * $MICROS_PER_SECOND);
          }
        """
  }

  private[this] def castToIntervalCode(from: DataType): CastFunction = from match {
    case StringType =>
      val util = IntervalUtils.getClass.getCanonicalName.stripSuffix("$")
      (c, evPrim, evNull) =>
        code"""$evPrim = $util.safeStringToInterval($c);
           if(${evPrim} == null) {
             ${evNull} = true;
           }
         """.stripMargin

  }

  private[this] def longToTimeStampCode(l: ExprValue): Block = code"$l * (long)$MICROS_PER_SECOND"
  private[this] def timestampToLongCode(ts: ExprValue): Block =
    code"java.lang.Math.floorDiv($ts, $MICROS_PER_SECOND)"
  private[this] def timestampToDoubleCode(ts: ExprValue): Block =
    code"$ts / (double)$MICROS_PER_SECOND"

  private[this] def castToBooleanCode(from: DataType): CastFunction = from match {
    case StringType =>
      val stringUtils = inline"${StringUtils.getClass.getName.stripSuffix("$")}"
      (c, evPrim, evNull) =>
        code"""
          if ($stringUtils.isTrueString($c)) {
            $evPrim = true;
          } else if ($stringUtils.isFalseString($c)) {
            $evPrim = false;
          } else {
            $evNull = true;
          }
        """
    case TimestampType =>
      (c, evPrim, evNull) => code"$evPrim = $c != 0;"
    case DateType =>
      // Hive would return null when cast from date to boolean
      (c, evPrim, evNull) => code"$evNull = true;"
    case n: NumericType =>
      (c, evPrim, evNull) => code"$evPrim = $c != 0;"
  }

  private[this] def castTimestampToIntegralTypeCode(
      ctx: CodegenContext,
      integralType: String): CastFunction = {
    if (ansiEnabled) {
      val longValue = ctx.freshName("longValue")
      (c, evPrim, evNull) =>
        code"""
          long $longValue = ${timestampToLongCode(c)};
          if ($longValue == ($integralType) $longValue) {
            $evPrim = ($integralType) $longValue;
          } else {
            throw new ArithmeticException("Casting " + $c + " to $integralType causes overflow");
          }
        """
    } else {
      (c, evPrim, evNull) => code"$evPrim = ($integralType) ${timestampToLongCode(c)};"
    }
  }

  private[this] def castIntegralTypeToIntegralTypeExactCode(integralType: String): CastFunction = {
    assert(ansiEnabled)
    (c, evPrim, evNull) =>
      code"""
        if ($c == ($integralType) $c) {
          $evPrim = ($integralType) $c;
        } else {
          throw new ArithmeticException("Casting " + $c + " to $integralType causes overflow");
        }
      """
  }

  private[this] def lowerAndUpperBound(integralType: String): (String, String) = {
    val (min, max, typeIndicator) = integralType.toLowerCase(Locale.ROOT) match {
      case "long" => (Long.MinValue, Long.MaxValue, "L")
      case "int" => (Int.MinValue, Int.MaxValue, "")
      case "short" => (Short.MinValue, Short.MaxValue, "")
      case "byte" => (Byte.MinValue, Byte.MaxValue, "")
    }
    (min.toString + typeIndicator, max.toString + typeIndicator)
  }

  private[this] def castFractionToIntegralTypeCode(integralType: String): CastFunction = {
    assert(ansiEnabled)
    val (min, max) = lowerAndUpperBound(integralType)
    val mathClass = classOf[Math].getName
    // When casting floating values to integral types, Spark uses the method `Numeric.toInt`
    // Or `Numeric.toLong` directly. For positive floating values, it is equivalent to `Math.floor`;
    // for negative floating values, it is equivalent to `Math.ceil`.
    // So, we can use the condition `Math.floor(x) <= upperBound && Math.ceil(x) >= lowerBound`
    // to check if the floating value x is in the range of an integral type after rounding.
    (c, evPrim, evNull) =>
      code"""
        if ($mathClass.floor($c) <= $max && $mathClass.ceil($c) >= $min) {
          $evPrim = ($integralType) $c;
        } else {
          throw new ArithmeticException("Casting " + $c + " to $integralType causes overflow");
        }
      """
  }

  private[this] def castToIntCode(from: DataType, ctx: CodegenContext): CastFunction = from match {
    case StringType if ansiEnabled =>
      (c, evPrim, evNull) => code"$evPrim = Integer.parseInt($c);"
    case StringType =>
      (c, evPrim, evNull) => code"$evPrim = Integer.parseInt($c);"
    case BooleanType =>
      (c, evPrim, evNull) => code"$evPrim = $c ? 1 : 0;"
    case DateType =>
      (c, evPrim, evNull) => code"$evNull = true;"
    case TimestampType => castTimestampToIntegralTypeCode(ctx, "int")
    case LongType if ansiEnabled => castIntegralTypeToIntegralTypeExactCode("int")
    case FloatType | DoubleType if ansiEnabled =>
      castFractionToIntegralTypeCode("int")
    case x: NumericType =>
      (c, evPrim, evNull) => code"$evPrim = (int) $c;"
  }

  private[this] def castToLongCode(from: DataType, ctx: CodegenContext): CastFunction = from match {
    case StringType if ansiEnabled =>
      (c, evPrim, evNull) => code"$evPrim = Long.parseLong($c);"
    case StringType =>
      (c, evPrim, evNull) => code"$evPrim = Long.parseLong($c);"
    case BooleanType =>
      (c, evPrim, evNull) => code"$evPrim = $c ? 1L : 0L;"
    case DateType =>
      (c, evPrim, evNull) => code"$evNull = true;"
    case TimestampType =>
      (c, evPrim, evNull) => code"$evPrim = (long) ${timestampToLongCode(c)};"
    case FloatType | DoubleType if ansiEnabled =>
      castFractionToIntegralTypeCode("long")
    case x: NumericType =>
      (c, evPrim, evNull) => code"$evPrim = (long) $c;"
  }

  private[this] def castToFloatCode(from: DataType, ctx: CodegenContext): CastFunction = {
    from match {
      case StringType =>
        val floatStr = ctx.freshVariable("floatStr", StringType)
        (c, evPrim, evNull) =>
          val handleNull = if (ansiEnabled) {
            s"""throw new NumberFormatException("invalid input syntax for type numeric: " + $c);"""
          } else {
            s"$evNull = true;"
          }
          code"""
          final String $floatStr = $c.toString();
          try {
            $evPrim = Float.valueOf($floatStr);
          } catch (java.lang.NumberFormatException e) {
            final Float f = (Float) Cast.processFloatingPointSpecialLiterals($floatStr, true);
            if (f == null) {
              $handleNull
            } else {
              $evPrim = f.floatValue();
            }
          }
        """
      case BooleanType =>
        (c, evPrim, evNull) => code"$evPrim = $c ? 1.0f : 0.0f;"
      case DateType =>
        (c, evPrim, evNull) => code"$evNull = true;"
      case TimestampType =>
        (c, evPrim, evNull) => code"$evPrim = (float) (${timestampToDoubleCode(c)});"
      case x: NumericType =>
        (c, evPrim, evNull) => code"$evPrim = (float) $c;"
    }
  }

  private[this] def castToDoubleCode(from: DataType, ctx: CodegenContext): CastFunction = {
    from match {
      case StringType =>
        val doubleStr = ctx.freshVariable("doubleStr", StringType)
        (c, evPrim, evNull) =>
          val handleNull = if (ansiEnabled) {
            s"""throw new NumberFormatException("invalid input syntax for type numeric: " + $c);"""
          } else {
            s"$evNull = true;"
          }
          code"""
          final String $doubleStr = $c.toString();
          try {
            $evPrim = Double.valueOf($doubleStr);
          } catch (java.lang.NumberFormatException e) {
            final Double d = (Double) Cast.processFloatingPointSpecialLiterals($doubleStr, false);
            if (d == null) {
              $handleNull
            } else {
              $evPrim = d.doubleValue();
            }
          }
        """
      case BooleanType =>
        (c, evPrim, evNull) => code"$evPrim = $c ? 1.0d : 0.0d;"
      case DateType =>
        (c, evPrim, evNull) => code"$evNull = true;"
      case TimestampType =>
        (c, evPrim, evNull) => code"$evPrim = ${timestampToDoubleCode(c)};"
      case x: NumericType =>
        (c, evPrim, evNull) => code"$evPrim = (double) $c;"
    }
  }

  private[this] def castArrayCode(
      fromType: DataType, toType: DataType, ctx: CodegenContext): CastFunction = {
    val elementCast = nullSafeCastFunction(fromType, toType, ctx)
    val arrayClass = JavaCode.javaType(classOf[GenericArrayData])
    val fromElementNull = ctx.freshVariable("feNull", BooleanType)
    val fromElementPrim = ctx.freshVariable("fePrim", fromType)
    val toElementNull = ctx.freshVariable("teNull", BooleanType)
    val toElementPrim = ctx.freshVariable("tePrim", toType)
    val size = ctx.freshVariable("n", IntegerType)
    val j = ctx.freshVariable("j", IntegerType)
    val values = ctx.freshVariable("values", classOf[Array[Object]])
    val javaType = JavaCode.javaType(fromType)

    (c, evPrim, evNull) =>
      code"""
        final int $size = $c.numElements();
        final Object[] $values = new Object[$size];
        for (int $j = 0; $j < $size; $j ++) {
          if ($c.isNullAt($j)) {
            $values[$j] = null;
          } else {
            boolean $fromElementNull = false;
            $javaType $fromElementPrim =
              ${CodeGenerator.getValue(c, fromType, j)};
            ${castCode(ctx, fromElementPrim,
              fromElementNull, toElementPrim, toElementNull, toType, elementCast)}
            if ($toElementNull) {
              $values[$j] = null;
            } else {
              $values[$j] = $toElementPrim;
            }
          }
        }
        $evPrim = new $arrayClass($values);
      """
  }

  private[this] def castStructCode(
      from: StructType, to: StructType, ctx: CodegenContext): CastFunction = {

    val fieldsCasts = from.fields.zip(to.fields).map {
      case (fromField, toField) => nullSafeCastFunction(fromField.dataType, toField.dataType, ctx)
    }
    val tmpResult = ctx.freshVariable("tmpResult", classOf[GenericRow])
    val rowClass = JavaCode.javaType(classOf[GenericRow])
    val tmpInput = ctx.freshVariable("tmpInput", classOf[Row])

    val fieldsEvalCode = fieldsCasts.zipWithIndex.map { case (cast, i) =>
      val fromFieldPrim = ctx.freshVariable("ffp", from.fields(i).dataType)
      val fromFieldNull = ctx.freshVariable("ffn", BooleanType)
      val toFieldPrim = ctx.freshVariable("tfp", to.fields(i).dataType)
      val toFieldNull = ctx.freshVariable("tfn", BooleanType)
      val fromType = JavaCode.javaType(from.fields(i).dataType)
      val setColumn = CodeGenerator.setColumn(tmpResult, to.fields(i).dataType, i, toFieldPrim)
      code"""
        boolean $fromFieldNull = $tmpInput.isNullAt($i);
        if ($fromFieldNull) {
          $tmpResult.setNullAt($i);
        } else {
          $fromType $fromFieldPrim =
            ${CodeGenerator.getValue(tmpInput, from.fields(i).dataType, i.toString)};
          ${castCode(ctx, fromFieldPrim,
            fromFieldNull, toFieldPrim, toFieldNull, to.fields(i).dataType, cast)}
          if ($toFieldNull) {
            $tmpResult.setNullAt($i);
          } else {
            $setColumn;
          }
        }
       """
    }
    val fieldsEvalCodes = ctx.splitExpressions(
      expressions = fieldsEvalCode.map(_.code),
      funcName = "castStruct",
      arguments = ("InternalRow", tmpInput.code) :: (rowClass.code, tmpResult.code) :: Nil)

    (input, result, resultIsNull) =>
      code"""
        final $rowClass $tmpResult = new $rowClass(${fieldsCasts.length});
        final InternalRow $tmpInput = $input;
        $fieldsEvalCodes
        $result = $tmpResult;
      """
  }

  override def sql: String = dataType match {
    // HiveQL doesn't allow casting to complex types. For logical plans translated from HiveQL, this
    // type of casting can only be introduced by the analyzer, and can be omitted when converting
    // back to SQL query string.
    case _: ArrayType | _: StructType => child.sql
    case _ => s"CAST(${child.sql} AS ${dataType.sql})"
  }
}

/**
 * Cast the child expression to the target data type.
 *
 * When cast from/to timezone related types, we need timeZoneId, which will be resolved with
 * session local timezone by an analyzer [[ResolveTimeZone]].
 */
case class Cast(child: Expression, dataType: DataType, timeZoneId: Option[String] = None)
  extends CastBase {
  println("cast", child, " => ", dataType)

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override protected val ansiEnabled: Boolean = false

  override def canCast(from: DataType, to: DataType): Boolean = Cast.canCast(from, to)

  override def typeCheckFailureMessage: String = s"cannot cast ${child.dataType.catalogString} to ${dataType.catalogString}"
}

/**
 * Cast the child expression to the target data type, but will throw error if the cast might
 * truncate, e.g. long -> int, timestamp -> data.
 *
 * Note: `target` is `AbstractDataType`, so that we can put `object DecimalType`, which means
 * we accept `DecimalType` with any valid precision/scale.
 */
case class UpCast(child: Expression, target: AbstractDataType, walkedTypePath: Seq[String] = Nil)
  extends UnaryExpression with Unevaluable {
  override lazy val resolved = false

  def dataType: DataType = target.asInstanceOf[DataType]
}
