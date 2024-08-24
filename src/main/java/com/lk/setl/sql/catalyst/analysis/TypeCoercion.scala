package com.lk.setl.sql.catalyst.analysis

import com.lk.setl.Logging
import com.lk.setl.sql.catalyst.expressions._
import com.lk.setl.sql.catalyst.plans.logical.LogicalPlan
import com.lk.setl.sql.catalyst.rules.Rule
import com.lk.setl.sql.types._

import javax.annotation.Nullable

/**
 * 强制类型转换识别
 *
 * A collection of [[Rule]] that can be used to coerce differing types that participate in
 * operations into compatible ones.
 *
 * Notes about type widening / tightest common types: Broadly, there are two cases when we need
 * to widen data types (e.g. union, binary comparison). In case 1, we are looking for a common
 * data type for two or more data types, and in this case no loss of precision is allowed. Examples
 * include type inference in JSON (e.g. what's the column's data type if one row is an integer
 * while the other row is a long?). In case 2, we are looking for a widened data type with
 * some acceptable loss of precision (e.g. there is no common type for double and decimal because
 * double's range is larger than decimal, and yet decimal is more precise than double, but in
 * union we would cast the decimal into double).
 */
object TypeCoercion {

  def typeCoercionRules: List[Rule[LogicalPlan]] =
    InConversion ::
      IfCoercion ::
      Division ::
      IntegralDivision ::
      ImplicitTypeCasts ::
      Nil

  // See https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types.
  // The conversion for integral and floating point types have a linear widening hierarchy:
  val numericPrecedence =
  IndexedSeq(
    IntegerType,
    LongType,
    FloatType,
    DoubleType)

  /**
   * 案例1类型加宽（有关类型强制，请参阅TypeCoercion上面的classdoc注释）。
   * Case 1 type widening (see the classdoc comment above for TypeCoercion).
   *
   * 查找binary expression可能使用的两种类型中最紧密的常见类型。
   * 这处理了除固定精度小数之外的所有数值类型，这些小数彼此交互或与基本类型交互，因为在这种情况下，结果的精度和规模取决于操作。
   * 这些规则在DecimalPrecision中实现。
   * Find the tightest common type of two types that might be used in a binary expression.
   * This handles all numeric types except fixed-precision decimals interacting with each other or
   * with primitive types, because in that case the precision and scale of the result depends on
   * the operation. Those rules are implemented in [[DecimalPrecision]].
   */
  val findTightestCommonType: (DataType, DataType) => Option[DataType] = {
    case (t1, t2) if t1 == t2 => Some(t1)
    case (NullType, t1) => Some(t1)
    case (t1, NullType) => Some(t1)


    // Promote numeric types to the highest of the two
    case (t1: NumericType, t2: NumericType) /*if !t1.isInstanceOf[DecimalType] && !t2.isInstanceOf[DecimalType]*/ =>
      val index = numericPrecedence.lastIndexWhere(t => t == t1 || t == t2)
      Some(numericPrecedence(index))

    case (t1, t2) => findTypeForComplex(t1, t2, findTightestCommonType)
  }

  private def findTypeForComplex(
      t1: DataType,
      t2: DataType,
      findTypeFunc: (DataType, DataType) => Option[DataType]): Option[DataType] = (t1, t2) match {
    case (ArrayType(et1, containsNull1), ArrayType(et2, containsNull2)) =>
      findTypeFunc(et1, et2).map { et =>
        ArrayType(et, containsNull1 || containsNull2 ||
          Cast.forceNullable(et1, et) || Cast.forceNullable(et2, et))
      }
    case (StructType(fields1), StructType(fields2)) if fields1.length == fields2.length =>
      val resolver = caseInsensitiveResolution
      fields1.zip(fields2).foldLeft(Option(new StructType())) {
        case (Some(struct), (field1, field2)) if resolver(field1.name, field2.name) =>
          findTypeFunc(field1.dataType, field2.dataType).map { dt =>
            struct.add(field1.name, dt)
          }
        case _ => None
      }
    case _ => None
  }

  /**
   * The method finds a common type for data types that differ only in nullable flags, including
   * `nullable`, `containsNull` of [[ArrayType]] and `valueContainsNull` of [[MapType]].
   * If the input types are different besides nullable flags, None is returned.
   */
  def findCommonTypeDifferentOnlyInNullFlags(t1: DataType, t2: DataType): Option[DataType] = {
    if (t1 == t2) {
      Some(t1)
    } else {
      None
    }
  }

  def findCommonTypeDifferentOnlyInNullFlags(types: Seq[DataType]): Option[DataType] = {
    if (types.isEmpty) {
      None
    } else {
      types.tail.foldLeft[Option[DataType]](Some(types.head)) {
        case (Some(t1), t2) => findCommonTypeDifferentOnlyInNullFlags(t1, t2)
        case _ => None
      }
    }
  }

  /**
   * Case 2 type widening (see the classdoc comment above for TypeCoercion).
   *
   * 与findTightestCommonType的主要区别在于，在这里我们允许在扩大decimal和double以及升级为string时损失一些精度。
   * i.e. the main difference with [[findTightestCommonType]] is that here we allow some
   * loss of precision when widening decimal and double, and promotion to string.
   */
  def findWiderTypeForTwo(t1: DataType, t2: DataType): Option[DataType] = {
    findTightestCommonType(t1, t2)
      //.orElse(findWiderTypeForDecimal(t1, t2))
      .orElse(stringPromotion(t1, t2))
      .orElse(findTypeForComplex(t1, t2, findWiderTypeForTwo))
  }

  /** Promotes all the way to StringType. */
  private def stringPromotion(dt1: DataType, dt2: DataType): Option[DataType] = (dt1, dt2) match {
    case (StringType, t2: AtomicType) if t2 != BinaryType && t2 != BooleanType => Some(StringType)
    case (t1: AtomicType, StringType) if t1 != BinaryType && t1 != BooleanType => Some(StringType)
    case _ => None
  }

  /**
   * Whether the data type contains StringType.
   */
  def hasStringType(dt: DataType): Boolean = dt match {
    case StringType => true
    case ArrayType(et, _) => hasStringType(et)
    // Add StructType if we support string promotion for struct fields in the future.
    case _ => false
  }

  private def findWiderCommonType(types: Seq[DataType]): Option[DataType] = {
    // findWiderTypeForTwo doesn't satisfy the associative law, i.e. (a op b) op c may not equal
    // to a op (b op c). This is only a problem for StringType or nested StringType in ArrayType.
    // Excluding these types, findWiderTypeForTwo satisfies the associative law. For instance,
    // (TimestampType, IntegerType, StringType) should have StringType as the wider common type.
    val (stringTypes, nonStringTypes) = types.partition(hasStringType(_))
    (stringTypes.distinct ++ nonStringTypes).foldLeft[Option[DataType]](Some(NullType))((r, c) =>
      r match {
        case Some(d) => findWiderTypeForTwo(d, c)
        case _ => None
      })
  }

  /**
   * Check whether the given types are equal ignoring nullable, containsNull and valueContainsNull.
   */
  def haveSameType(types: Seq[DataType]): Boolean = {
    if (types.size <= 1) {
      true
    } else {
      val head = types.head
      types.tail.forall(_.sameType(head))
    }
  }


  private def castIfNotSameType(expr: Expression, dt: DataType): Expression = {
    if (!expr.dataType.sameType(dt)) {
      Cast(expr, dt)
    } else {
      expr
    }
  }

  /**
   * Handles type coercion for both IN expression with subquery and IN
   * expressions without subquery.
   * 1. In the first case, find the common type by comparing the left hand side (LHS)
   *    expression types against corresponding right hand side (RHS) expression derived
   *    from the subquery expression's plan output. Inject appropriate casts in the
   *    LHS and RHS side of IN expression.
   *
   * 2. In the second case, convert the value and in list expressions to the
   *    common operator type by looking at all the argument types and finding
   *    the closest one that all the arguments can be cast to. When no common
   *    operator type is found the original expression will be returned and an
   *    Analysis Exception will be raised at the type checking phase.
   */
  object InConversion extends TypeCoercionRule {
    override protected def coerceTypes(
        plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case i @ In(a, b) if b.exists(_.dataType != a.dataType) =>
        findWiderCommonType(i.children.map(_.dataType)) match {
          case Some(finalDataType) => i.withNewChildren(i.children.map(Cast(_, finalDataType)))
          case None => i
        }
    }
  }

  /**
   * 将If语句的不同分支的类型强制转换为通用类型。
   * Coerces the type of different branches of If statement to a common type.
   */
  object IfCoercion extends TypeCoercionRule {
    override protected def coerceTypes(
        plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      case e if !e.childrenResolved => e
      // Find tightest common type for If, if the true value and false value have different types.
      case i @ If(pred, left, right) if !haveSameType(i.inputTypesForMerging) =>
        findWiderTypeForTwo(left.dataType, right.dataType).map { widestType =>
          val newLeft = castIfNotSameType(left, widestType)
          val newRight = castIfNotSameType(right, widestType)
          If(pred, newLeft, newRight)
        }.getOrElse(i)  // If there is no applicable conversion, leave expression unchanged.
      case If(Literal(null, NullType), left, right) =>
        If(Literal.create(null, BooleanType), left, right)
      case If(pred, left, right) if pred.dataType == NullType =>
        If(Cast(pred, BooleanType), left, right)
    }
  }

  /**
   * Hive只使用DIV运算符执行整数除法。这里/的参数总是转换为double类型。
   * Hive only performs integral division with the DIV operator. The arguments to / are always
   * converted to fractional types.
   */
  object Division extends TypeCoercionRule {
    override protected def coerceTypes(
        plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      // Skip nodes who has not been resolved yet,
      // as this is an extra rule which should be applied at last.
      case e if !e.childrenResolved => e

      // Decimal and Double remain the same
      case d: Divide if d.dataType == DoubleType => d
      case d @ Divide(left, right, _) if isNumericOrNull(left) && isNumericOrNull(right) =>
        d.withNewChildren(Seq(Cast(left, DoubleType), Cast(right, DoubleType)))
    }

    private def isNumericOrNull(ex: Expression): Boolean = {
      // We need to handle null types in case a query contains null literals.
      ex.dataType.isInstanceOf[NumericType] || ex.dataType == NullType
    }
  }

  /**
   * The DIV operator always returns long-type value.
   * This rule cast the integral inputs to long type, to avoid overflow during calculation.
   */
  object IntegralDivision extends TypeCoercionRule {
    override protected def coerceTypes(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      case e if !e.childrenResolved => e
      case d @ IntegralDivide(left, right, _) =>
        d.withNewChildren(Seq(mayCastToLong(left), mayCastToLong(right)))
    }

    private def mayCastToLong(expr: Expression): Expression = expr.dataType match {
      case _: IntegerType => Cast(expr, LongType)
      case _ => expr
    }
  }

  /**
   * 根据Expression的预期输input types强制转换类型。
   * Casts types according to the expected input types for [[Expression]]s.
   */
  object ImplicitTypeCasts extends TypeCoercionRule {

    private def canHandleTypeCoercion(leftType: DataType, rightType: DataType): Boolean = {
      // If DecimalType operands are involved except for the two cases above,
      // DecimalPrecision will handle it.
      // !leftType.isInstanceOf[DecimalType] && !rightType.isInstanceOf[DecimalType] &&
        leftType != rightType
    }

    override protected def coerceTypes(
        plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case b @ BinaryOperator(left, right)
          if canHandleTypeCoercion(left.dataType, right.dataType) =>
        findTightestCommonType(left.dataType, right.dataType).map { commonType =>
          if (b.inputType.acceptsType(commonType)) {
            // If the expression accepts the tightest common type, cast to that.
            val newLeft = if (left.dataType == commonType) left else Cast(left, commonType)
            val newRight = if (right.dataType == commonType) right else Cast(right, commonType)
            b.withNewChildren(Seq(newLeft, newRight))
          } else {
            // Otherwise, don't do anything with the expression.
            b
          }
        }.getOrElse(b)  // If there is no applicable conversion, leave expression unchanged.

      case e: ImplicitCastInputTypes if e.inputTypes.nonEmpty =>
        val children: Seq[Expression] = e.children.zip(e.inputTypes).map { case (in, expected) =>
          // If we cannot do the implicit cast, just use the original input.
          implicitCast(in, expected).getOrElse(in)
        }
        e.withNewChildren(children)

      case e: ExpectsInputTypes if e.inputTypes.nonEmpty =>
        // Convert NullType into some specific target type for ExpectsInputTypes that don't do
        // general implicit casting.
        val children: Seq[Expression] = e.children.zip(e.inputTypes).map { case (in, expected) =>
          if (in.dataType == NullType && !expected.acceptsType(NullType)) {
            Literal.create(null, expected.defaultConcreteType)
          } else {
            in
          }
        }
        e.withNewChildren(children)

    }

    private def udfInputToCastType(input: DataType, expectedType: DataType): DataType = {
      (input, expectedType) match {
        case (ArrayType(dtIn, _), ArrayType(dtExp, nullableExp)) =>
          ArrayType(udfInputToCastType(dtIn, dtExp), nullableExp)
        case (StructType(fieldsIn), StructType(fieldsExp)) =>
          val fieldTypes =
            fieldsIn.map(_.dataType).zip(fieldsExp.map(_.dataType)).map { case (dtIn, dtExp) =>
              udfInputToCastType(dtIn, dtExp)
            }
          StructType(fieldsExp.zip(fieldTypes).map { case (field, newDt) =>
            field.copy(dataType = newDt)
          })
        case (_, other) => other
      }
    }

    /**
     * Given an expected data type, try to cast the expression and return the cast expression.
     *
     * If the expression already fits the input type, we simply return the expression itself.
     * If the expression has an incompatible type that cannot be implicitly cast, return None.
     */
    def implicitCast(e: Expression, expectedType: AbstractDataType): Option[Expression] = {
      implicitCast(e.dataType, expectedType).map { dt =>
        if (dt == e.dataType) e else Cast(e, dt)
      }
    }

    private def implicitCast(inType: DataType, expectedType: AbstractDataType): Option[DataType] = {
      // Note that ret is nullable to avoid typing a lot of Some(...) in this local scope.
      // We wrap immediately an Option after this.
      @Nullable val ret: DataType = (inType, expectedType) match {
        // If the expected type is already a parent of the input type, no need to cast.
        case _ if expectedType.acceptsType(inType) => inType

        // Cast null type (usually from null literals) into target types
        case (NullType, target) => target.defaultConcreteType

        // If the function accepts any numeric type and the input is a string, we follow the hive
        // convention and cast that input into a double
        case (StringType, NumericType) => NumericType.defaultConcreteType

        // Implicit cast among numeric types. When we reach here, input type is not acceptable.

        // For any other numeric types, implicitly cast to each other, e.g. long -> int, int -> long
        case (_: NumericType, target: NumericType) => target

        // Implicit cast between date time types
        case (DateType, TimestampType) => TimestampType
        case (TimestampType, DateType) => DateType

        // string类型可以隐士转换成这么多类型
        // Implicit cast from/to string
        case (StringType, target: NumericType) => target
        case (StringType, DateType) => DateType
        case (StringType, TimestampType) => TimestampType
        case (StringType, BinaryType) => BinaryType
        // Cast any atomic type to string.
        case (any: AtomicType, StringType) if any != StringType => StringType

        // When we reach here, input type is not acceptable for any types in this type collection,
        // try to find the first one we can implicitly cast.
        case (_, TypeCollection(types)) =>
          types.flatMap(implicitCast(inType, _)).headOption.orNull

        // Implicit cast between array types.
        //
        // Compare the nullabilities of the from type and the to type, check whether the cast of
        // the nullability is resolvable by the following rules:
        // 1. If the nullability of the to type is true, the cast is always allowed;
        // 2. If the nullability of the to type is false, and the nullability of the from type is
        // true, the cast is never allowed;
        // 3. If the nullabilities of both the from type and the to type are false, the cast is
        // allowed only when Cast.forceNullable(fromType, toType) is false.
        case (ArrayType(fromType, fn), ArrayType(toType: DataType, true)) =>
          implicitCast(fromType, toType).map(ArrayType(_, true)).orNull

        case (ArrayType(fromType, true), ArrayType(toType: DataType, false)) => null

        case (ArrayType(fromType, false), ArrayType(toType: DataType, false))
            if !Cast.forceNullable(fromType, toType) =>
          implicitCast(fromType, toType).map(ArrayType(_, false)).orNull

        case _ => null
      }
      Option(ret)
    }
  }

}

trait TypeCoercionRule extends Rule[LogicalPlan] with Logging {
  /**
   * Applies any changes to [[AttributeReference]] data types that are made by the transform method
   * to instances higher in the query tree.
   */
  def apply(plan: LogicalPlan): LogicalPlan = {
    val newPlan = coerceTypes(plan)
    if (plan.fastEquals(newPlan)) {
      plan
    } else {
      propagateTypes(newPlan)
    }
  }

  protected def coerceTypes(plan: LogicalPlan): LogicalPlan

  private def propagateTypes(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp {
    // No propagation required for leaf nodes.
    case q: LogicalPlan if q.children.isEmpty => q

    // Don't propagate types from unresolved children.
    case q: LogicalPlan if !q.childrenResolved => q

    case q: LogicalPlan =>
      val inputMap = q.inputSet.toSeq.map(a => (a.exprId, a)).toMap
      q transformExpressions {
        case a: AttributeReference =>
          inputMap.get(a.exprId) match {
            // This can happen when an Attribute reference is born in a non-leaf node, for
            // example due to a call to an external script like in the Transform operator.
            // TODO: Perhaps those should actually be aliases?
            case None => a
            // Leave the same if the dataTypes match.
            case Some(newType) if a.dataType == newType.dataType => a
            case Some(newType) =>
              logDebug(s"Promoting $a from ${a.dataType} to ${newType.dataType} in " +
                s" ${q.simpleString(25)}")
              newType
          }
      }
  }
}

