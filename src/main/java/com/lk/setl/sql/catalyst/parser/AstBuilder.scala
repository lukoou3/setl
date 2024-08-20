package com.lk.setl.sql.catalyst.parser

import com.lk.setl.Logging
import com.lk.setl.sql.AnalysisException
import com.lk.setl.sql.catalyst.analysis._
import com.lk.setl.sql.catalyst.expressions._
import com.lk.setl.sql.catalyst.expressions.aggregate.{First, Last}
import com.lk.setl.sql.catalyst.parser.ParserUtils.{EnhancedLogicalPlan, string, stringWithoutUnescape, withOrigin}
import com.lk.setl.sql.catalyst.parser.SqlBaseParser._
import com.lk.setl.sql.catalyst.plans.logical.{Filter, Generate, Aggregate, LogicalPlan, Project}
import com.lk.setl.sql.types._
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.tree.{ParseTree, TerminalNode}

import java.util.Locale
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ArrayBuffer

class AstBuilder extends SqlBaseBaseVisitor[AnyRef] with Logging {

  protected def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }

  override def visitSingleDataType(ctx: SingleDataTypeContext): DataType = withOrigin(ctx) {
    typedVisit[DataType](ctx.dataType)
  }

  override def visitSingleExpression(ctx: SingleExpressionContext): Expression = withOrigin(ctx) {
    visitNamedExpression(ctx.namedExpression)
  }

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }

  override def visitRegularQuerySpecification(
    ctx: RegularQuerySpecificationContext): LogicalPlan = withOrigin(ctx) {
    //val where: Option[Expression] = Option(ctx.whereClause).map(x => expression(x.booleanExpression))
    val relation = visitFromClause(ctx.fromClause)
    val lateralView = ctx.lateralView().asScala
    if(lateralView.length > 1){
      throw new ParseException(s"now only supported one generator_function ", ctx)
    }
    // Add lateral views.
    val withLateralView = relation.optionalMap(lateralView.headOption.getOrElse(null))((g, l) => withGenerate(l, g))

    // Add where.
    val withFilter = withLateralView.optionalMap(ctx.whereClause)(withWhereClause)

    val expressions = visitNamedExpressionSeq(ctx.selectClause.namedExpressionSeq)
    // Add aggregation or a project.
    val namedExpressions = expressions.map {
      case e: NamedExpression => e
      case e: Expression => UnresolvedAlias(e)
    }

    val aggregationClause = ctx.aggregationClause
    if (aggregationClause != null) {
      val aggregate = withAggregationClause(aggregationClause, namedExpressions, withFilter)
      aggregate
    } else {
      if (namedExpressions.nonEmpty) {
        Project(namedExpressions, withFilter) // select
      } else {
        withFilter
      }
    }
  }

  override def visitFromClause(ctx: FromClauseContext) : LogicalPlan = withOrigin(ctx) {
    typedVisit[LogicalPlan](ctx.relation.relationPrimary)
  }

  /**
   * Create an aliased table reference. This is typically used in FROM clauses.
   */
  override def visitTableName(ctx: TableNameContext): LogicalPlan = withOrigin(ctx) {
    val tableId = visitMultipartIdentifier(ctx.multipartIdentifier)
    UnresolvedRelation(tableId)
  }

  /**
   * Add an [[Aggregate]] or [[GroupingSets]] to a logical plan.
   */
  private def withAggregationClause(
      ctx: AggregationClauseContext,
      selectExpressions: Seq[NamedExpression],
      query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    val groupByExpressions = expressionList(ctx.groupingExpressions)
    Aggregate(groupByExpressions, selectExpressions, query)
  }

  /**
   * Add a [[Generate]] (Lateral View) to a logical plan.
   */
  private def withGenerate(
      query: LogicalPlan,
      ctx: LateralViewContext): LogicalPlan = withOrigin(ctx) {
    val expressions = expressionList(ctx.expression)
    Generate(
      UnresolvedGenerator(visitFunctionName(ctx.qualifiedName), expressions),
      unrequiredChildIndex = Nil,
      outer = ctx.OUTER != null,
      // scalastyle:off caselocale
      Some(ctx.tblName.getText.toLowerCase),
      // scalastyle:on caselocale
      ctx.colName.asScala.map(_.getText).map(UnresolvedAttribute.apply).toSeq,
      query)
  }

  /**
   * Create a logical plan using a where clause.
   */
  private def withWhereClause(ctx: WhereClauseContext, plan: LogicalPlan): LogicalPlan = {
    Filter(expression(ctx.booleanExpression), plan)
  }

  /**
   * Create a multi-part identifier.
   */
  override def visitMultipartIdentifier(ctx: MultipartIdentifierContext): Seq[String] =
    withOrigin(ctx) {
      ctx.parts.asScala.map(_.getText).toSeq
    }

  /**
   * Create an aliased expression if an alias is specified. Both single and multi-aliases are
   * supported.
   */
  override def visitNamedExpression(ctx: NamedExpressionContext): Expression = withOrigin(ctx) {
    val e = expression(ctx.expression)
    if(e == null){
      println(ctx.expression.getText)
      throw new ParseException(s"can not parse expression '${ctx.expression.getText}'", ctx)
    }
    if (ctx.name != null) {
      Alias(e, ctx.name.getText)()
    } else {
      e
    }
  }

  override def visitNamedExpressionSeq(
    ctx: NamedExpressionSeqContext): Seq[Expression] = {
    Option(ctx).toSeq
      .flatMap(_.namedExpression.asScala)
      .map(typedVisit[Expression])
  }

  /**
   * Create an [[UnresolvedExtractValue]] expression, this is used for subscript access to an array.
   */
  override def visitSubscript(ctx: SubscriptContext): Expression = withOrigin(ctx) {
    UnresolvedExtractValue(expression(ctx.value), expression(ctx.index))
  }

  /* ********************************************************************************************
   * Expression parsing
   * ******************************************************************************************** */
  /**
   * Create an expression from the given context. This method just passes the context on to the
   * visitor and only takes care of typing (We assume that the visitor returns an Expression here).
   */
  protected def expression(ctx: ParserRuleContext): Expression = typedVisit(ctx)

  /**
   * Create sequence of expressions from the given sequence of contexts.
   */
  private def expressionList(trees: java.util.List[ExpressionContext]): Seq[Expression] = {
    trees.asScala.map(expression).toSeq
  }

  /**
   * Invert a boolean expression.
   */
  override def visitLogicalNot(ctx: LogicalNotContext): Expression = withOrigin(ctx) {
    Not(expression(ctx.booleanExpression()))
  }

  /**
   * Create a predicated expression. A predicated expression is a normal expression with a
   * predicate attached to it, for example:
   * {{{
   *    a + 1 IS NULL
   * }}}
   */
  override def visitPredicated(ctx: PredicatedContext): Expression = withOrigin(ctx) {
    val e = expression(ctx.valueExpression)
    if (ctx.predicate != null) {
      withPredicate(e, ctx.predicate)
    } else {
      e
    }
  }

  /**
   * Add a predicate to the given expression. Supported expressions are:
   * - (NOT) BETWEEN
   * - (NOT) IN
   * - (NOT) LIKE (ANY | SOME | ALL)
   * - (NOT) RLIKE
   * - IS (NOT) NULL.
   * - IS (NOT) (TRUE | FALSE | UNKNOWN)
   * - IS (NOT) DISTINCT FROM
   */
  private def withPredicate(e: Expression, ctx: PredicateContext): Expression = withOrigin(ctx) {
    // Invert a predicate if it has a valid NOT clause.
    def invertIfNotDefined(e: Expression): Expression = ctx.NOT match {
      case null => e
      case not => Not(e)
    }

    def getValueExpressions(e: Expression): Seq[Expression] = e match {
      //case c: CreateNamedStruct => c.valExprs
      case other => Seq(other)
    }

    // Create the predicate.
    ctx.kind.getType match {
      case SqlBaseParser.BETWEEN =>
        // BETWEEN is translated to lower <= e && e <= upper
        invertIfNotDefined(And(
          GreaterThanOrEqual(e, expression(ctx.lower)),
          LessThanOrEqual(e, expression(ctx.upper))))
      case SqlBaseParser.IN =>
        invertIfNotDefined(In(e, ctx.expression.asScala.map(expression).toSeq))
      case SqlBaseParser.LIKE =>
        Option(ctx.quantifier).map(_.getType) match {
          case _ =>
            val escapeChar = Option(ctx.escapeChar).map(string).map { str =>
              if (str.length != 1) {
                throw new ParseException("Invalid escape string." +
                  "Escape string must contain only one character.", ctx)
              }
              str.charAt(0)
            }.getOrElse('\\')
            invertIfNotDefined(Like(e, expression(ctx.pattern), escapeChar))
        }
      case SqlBaseParser.RLIKE =>
        invertIfNotDefined(RLike(e, expression(ctx.pattern)))
      case SqlBaseParser.NULL if ctx.NOT != null =>
        IsNotNull(e)
      case SqlBaseParser.NULL =>
        IsNull(e)
      case SqlBaseParser.TRUE => ctx.NOT match {
        case null => EqualNullSafe(e, Literal(true))
        case _ => Not(EqualNullSafe(e, Literal(true)))
      }
      case SqlBaseParser.FALSE => ctx.NOT match {
        case null => EqualNullSafe(e, Literal(false))
        case _ => Not(EqualNullSafe(e, Literal(false)))
      }
    }
  }

  /**
   * Combine a number of boolean expressions into a balanced expression tree. These expressions are
   * either combined by a logical [[And]] or a logical [[Or]].
   *
   * A balanced binary tree is created because regular left recursive trees cause considerable
   * performance degradations and can cause stack overflows.
   */
  override def visitLogicalBinary(ctx: LogicalBinaryContext): Expression = withOrigin(ctx) {
    val expressionType = ctx.operator.getType
    val expressionCombiner = expressionType match {
      case SqlBaseParser.AND => And.apply _
      case SqlBaseParser.OR => Or.apply _
    }

    // Collect all similar left hand contexts.
    val contexts = ArrayBuffer(ctx.right)
    var current = ctx.left
    def collectContexts: Boolean = current match {
      case lbc: LogicalBinaryContext if lbc.operator.getType == expressionType =>
        contexts += lbc.right
        current = lbc.left
        true
      case _ =>
        contexts += current
        false
    }
    while (collectContexts) {
      // No body - all updates take place in the collectContexts.
    }

    // Reverse the contexts to have them in the same sequence as in the SQL statement & turn them
    // into expressions.
    val expressions = contexts.reverseMap(expression)

    // Create a balanced tree.
    def reduceToExpressionTree(low: Int, high: Int): Expression = high - low match {
      case 0 =>
        expressions(low)
      case 1 =>
        expressionCombiner(expressions(low), expressions(high))
      case x =>
        val mid = low + x / 2
        expressionCombiner(
          reduceToExpressionTree(low, mid),
          reduceToExpressionTree(mid + 1, high))
    }
    reduceToExpressionTree(0, expressions.size - 1)
  }

  /**
   * Create a comparison expression. This compares two expressions. The following comparison
   * operators are supported:
   * - Equal: '=' or '=='
   * - Null-safe Equal: '<=>'
   * - Not Equal: '<>' or '!='
   * - Less than: '<'
   * - Less then or Equal: '<='
   * - Greater than: '>'
   * - Greater then or Equal: '>='
   */
  override def visitComparison(ctx: ComparisonContext): Expression = withOrigin(ctx) {
    val left = expression(ctx.left)
    val right = expression(ctx.right)
    val operator = ctx.comparisonOperator().getChild(0).asInstanceOf[TerminalNode]
    operator.getSymbol.getType match {
      case SqlBaseParser.EQ =>
        EqualTo(left, right)
      case SqlBaseParser.NSEQ =>
        EqualNullSafe(left, right)
      case SqlBaseParser.NEQ | SqlBaseParser.NEQJ =>
        Not(EqualTo(left, right))
      case SqlBaseParser.LT =>
        LessThan(left, right)
      case SqlBaseParser.LTE =>
        LessThanOrEqual(left, right)
      case SqlBaseParser.GT =>
        GreaterThan(left, right)
      case SqlBaseParser.GTE =>
        GreaterThanOrEqual(left, right)
    }
  }

  /**
   * Create a binary arithmetic expression. The following arithmetic operators are supported:
   * - Multiplication: '*'
   * - Division: '/'
   * - Hive Long Division: 'DIV'
   * - Modulo: '%'
   * - Addition: '+'
   * - Subtraction: '-'
   * - Binary AND: '&'
   * - Binary XOR
   * - Binary OR: '|'
   */
  override def visitArithmeticBinary(ctx: ArithmeticBinaryContext): Expression = withOrigin(ctx) {
    val left = expression(ctx.left)
    val right = expression(ctx.right)
    ctx.operator.getType match {
      case SqlBaseParser.ASTERISK =>
        Multiply(left, right)
      case SqlBaseParser.SLASH =>
        Divide(left, right)
      case SqlBaseParser.PERCENT =>
        Remainder(left, right)
      case SqlBaseParser.DIV =>
        IntegralDivide(left, right)
      case SqlBaseParser.PLUS =>
        Add(left, right)
      case SqlBaseParser.MINUS =>
        Subtract(left, right)
      /*case SqlBaseParser.CONCAT_PIPE =>
        Concat(left :: right :: Nil)
      case SqlBaseParser.AMPERSAND =>
        BitwiseAnd(left, right)
      case SqlBaseParser.HAT =>
        BitwiseXor(left, right)
      case SqlBaseParser.PIPE =>
        BitwiseOr(left, right)*/
    }
  }

  /**
   * Create a unary arithmetic expression. The following arithmetic operators are supported:
   * - Plus: '+'
   * - Minus: '-'
   * - Bitwise Not: '~'
   */
  override def visitArithmeticUnary(ctx: ArithmeticUnaryContext): Expression = withOrigin(ctx) {
    val value = expression(ctx.valueExpression)
    ctx.operator.getType match {
      case SqlBaseParser.PLUS =>
        UnaryPositive(value)
      case SqlBaseParser.MINUS =>
        UnaryMinus(value)
      /*case SqlBaseParser.TILDE =>
        BitwiseNot(value)*/
    }
  }

  /**
   * Create a [[First]] expression.
   */
  override def visitFirst(ctx: FirstContext): Expression = withOrigin(ctx) {
    val ignoreNullsExpr = ctx.IGNORE != null
    First(expression(ctx.expression), ignoreNullsExpr).toAggregateExpression()
  }

  /**
   * Create a [[Last]] expression.
   */
  override def visitLast(ctx: LastContext): Expression = withOrigin(ctx) {
    val ignoreNullsExpr = ctx.IGNORE != null
    Last(expression(ctx.expression), ignoreNullsExpr).toAggregateExpression()
  }

  /**
   * 没在.g4文件定义的函数应该都是这个入口, udf就是这个入口
   * 对应的处理规则在[[ResolveFunctions]]
   * 通过lookupFunction函数能找到我们注册的函数
   * Create a (windowed) Function expression.
   */
  override def visitFunctionCall(ctx: FunctionCallContext): Expression = withOrigin(ctx) {
    // Create the function call.
    val name = ctx.functionName.getText
    // Call `toSeq`, otherwise `ctx.argument.asScala.map(expression)` is `Buffer` in Scala 2.13
    val arguments = ctx.argument.asScala.map(expression).toSeq match {
      case Seq(UnresolvedStar(None))
        if name.toLowerCase(Locale.ROOT) == "count" =>
        // Transform COUNT(*) into COUNT(1).
        Seq(Literal(1))
      case expressions =>
        expressions
    }
    val filter = Option(ctx.where).map(expression(_))
    val function = UnresolvedFunction(
      getFunctionIdentifier(ctx.functionName), arguments, filter)

    function
  }


  /**
   * Create a function database (optional) and name pair, for multipartIdentifier.
   * This is used in CREATE FUNCTION, DROP FUNCTION, SHOWFUNCTIONS.
   */
  protected def visitFunctionName(ctx: MultipartIdentifierContext): String = {
    visitFunctionName(ctx, ctx.parts.asScala.map(_.getText).toSeq)
  }

  /**
   * Create a function database (optional) and name pair.
   */
  protected def visitFunctionName(ctx: QualifiedNameContext): String = {
    visitFunctionName(ctx, ctx.identifier().asScala.map(_.getText).toSeq)
  }

  /**
   * Create a function database (optional) and name pair.
   */
  private def visitFunctionName(ctx: ParserRuleContext, texts: Seq[String]): String = {
    texts match {
      case Seq(db, fn) => fn
      case Seq(fn) => fn
      case other =>
        throw new ParseException(s"Unsupported function name '${texts.mkString(".")}'", ctx)
    }
  }

  /**
   * Create a [[FunctionIdentifier]] from a 'functionName' or 'databaseName'.'functionName' pattern.
   */
  override def visitFunctionIdentifier(
    ctx: FunctionIdentifierContext): String = withOrigin(ctx) {
    ctx.function.getText
  }

  /**
   * Get a function identifier consist by database (optional) and name.
   */
  protected def getFunctionIdentifier(ctx: FunctionNameContext): String = {
    if (ctx.qualifiedName != null) {
      visitFunctionName(ctx.qualifiedName)
    } else {
      ctx.getText
    }
  }

  /**
   * Create an UnresolvedAttribute expression  if it is a regex
   * quoted in ``
   */
  override def visitColumnReference(ctx: ColumnReferenceContext): Expression = withOrigin(ctx) {
    UnresolvedAttribute.quoted(ctx.getText) //创建列引用
  }


  /**
   * Create a NULL literal expression.
   */
  override def visitNullLiteral(ctx: NullLiteralContext): Literal = withOrigin(ctx) {
    Literal(null)
  }

  /**
   * Create a Boolean literal expression.
   */
  override def visitBooleanLiteral(ctx: BooleanLiteralContext): Literal = withOrigin(ctx) {
    if (ctx.getText.toBoolean) {
      Literal.TrueLiteral
    } else {
      Literal.FalseLiteral
    }
  }

  /**
   * Create an integral literal expression. The code selects the most narrow integral type
   * possible, either a BigDecimal, a Long or an Integer is returned.
   */
  override def visitIntegerLiteral(ctx: IntegerLiteralContext): Literal = withOrigin(ctx) {
    BigDecimal(ctx.getText) match {
      case v if v.isValidInt =>
        Literal(v.intValue)
      case v if v.isValidLong =>
        Literal(v.longValue)
      case v => Literal(v.doubleValue())
    }
  }

  /**
   * Create a decimal literal for a regular decimal number.
   */
  override def visitDecimalLiteral(ctx: DecimalLiteralContext): Literal = withOrigin(ctx) {
    Literal(BigDecimal(ctx.getText).underlying())
  }

  /**
   * Create a decimal literal for a regular decimal number or a scientific decimal number.
   */
  override def visitLegacyDecimalLiteral(
    ctx: LegacyDecimalLiteralContext): Literal = withOrigin(ctx) {
    Literal(BigDecimal(ctx.getText).underlying())
  }

  /**
   * Create a double literal for number with an exponent, e.g. 1E-30
   */
  override def visitExponentLiteral(ctx: ExponentLiteralContext): Literal = {
    numericLiteral(ctx, ctx.getText, /* exponent values don't have a suffix */
      Double.MinValue, Double.MaxValue, DoubleType.simpleString)(_.toDouble)
  }

  /** Create a numeric literal expression. */
  private def numericLiteral(
    ctx: NumberContext,
    rawStrippedQualifier: String,
    minValue: BigDecimal,
    maxValue: BigDecimal,
    typeName: String)(converter: String => Any): Literal = withOrigin(ctx) {
    try {
      val rawBigDecimal = BigDecimal(rawStrippedQualifier)
      if (rawBigDecimal < minValue || rawBigDecimal > maxValue) {
        throw new ParseException(s"Numeric literal ${rawStrippedQualifier} does not " +
          s"fit in range [${minValue}, ${maxValue}] for type ${typeName}", ctx)
      }
      Literal(converter(rawStrippedQualifier))
    } catch {
      case e: NumberFormatException =>
        throw new ParseException(e.getMessage, ctx)
    }
  }

  /**
   * Create a Byte Literal expression.
   */
  override def visitTinyIntLiteral(ctx: TinyIntLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier,
      Int.MinValue, Int.MaxValue, IntegerType.simpleString)(_.toInt)
  }

  /**
   * Create a Short Literal expression.
   */
  override def visitSmallIntLiteral(ctx: SmallIntLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier,
      Int.MinValue, Int.MaxValue, IntegerType.simpleString)(_.toInt)
  }

  /**
   * Create a Long Literal expression.
   */
  override def visitBigIntLiteral(ctx: BigIntLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier,
      Long.MinValue, Long.MaxValue, LongType.simpleString)(_.toLong)
  }

  /**
   * Create a Float Literal expression.
   */
  override def visitFloatLiteral(ctx: FloatLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier,
      Float.MinValue, Float.MaxValue, FloatType.simpleString)(_.toFloat)
  }

  /**
   * Create a Double Literal expression.
   */
  override def visitDoubleLiteral(ctx: DoubleLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier,
      Double.MinValue, Double.MaxValue, DoubleType.simpleString)(_.toDouble)
  }

  /**
   * Create a BigDecimal Literal expression.
   */
  override def visitBigDecimalLiteral(ctx: BigDecimalLiteralContext): Literal = {
    val raw = ctx.getText.substring(0, ctx.getText.length - 2)
    try {
      Literal(BigDecimal(raw).underlying())
    } catch {
      case e: AnalysisException =>
        throw new ParseException(e.message, ctx)
    }
  }

  /**
   * Create a String literal expression.
   */
  override def visitStringLiteral(ctx: StringLiteralContext): Literal = withOrigin(ctx) {
    Literal(createString(ctx))
  }

  /**
   * Create a String from a string literal context. This supports multiple consecutive string
   * literals, these are concatenated, for example this expression "'hello' 'world'" will be
   * converted into "helloworld".
   *
   * Special characters can be escaped by using Hive/C-style escaping.
   */
  private def createString(ctx: StringLiteralContext): String = {
    if (false) {
      ctx.STRING().asScala.map(stringWithoutUnescape).mkString
    } else {
      ctx.STRING().asScala.map(string).mkString
    }
  }

  /* ********************************************************************************************
   * DataType parsing
   * ******************************************************************************************** */

  /**
   * Resolve/create a primitive type.
   */
  override def visitPrimitiveDataType(ctx: PrimitiveDataTypeContext): DataType = withOrigin(ctx) {
    val dataType = ctx.identifier.getText.toLowerCase(Locale.ROOT)
    (dataType, ctx.INTEGER_VALUE().asScala.toList) match {
      case ("boolean", Nil) => BooleanType
      case ("int" | "integer", Nil) => IntegerType
      case ("bigint" | "long", Nil) => LongType
      case ("float" | "real", Nil) => FloatType
      case ("double", Nil) => DoubleType
      case ("string", Nil) => StringType
      case ("binary", Nil) => BinaryType
      case ("void", Nil) => NullType
      case (dt, params) =>
        val dtStr = if (params.nonEmpty) s"$dt(${params.mkString(",")})" else dt
        throw new ParseException(s"DataType $dtStr is not supported.", ctx)
    }
  }

  /**
   * Create a complex DataType. Arrays, Maps and Structures are supported.
   */
  override def visitComplexDataType(ctx: ComplexDataTypeContext): DataType = withOrigin(ctx) {
    ctx.complex.getType match {
      case SqlBaseParser.ARRAY =>
        ArrayType(typedVisit(ctx.dataType(0)))
      case SqlBaseParser.MAP =>
        //MapType(typedVisit(ctx.dataType(0)), typedVisit(ctx.dataType(1)))
        throw new ParseException(s"DataType ${ctx.getText} is not supported.", ctx)
      case SqlBaseParser.STRUCT =>
        StructType(Option(ctx.complexColTypeList).toSeq.flatMap(visitComplexColTypeList))
    }
  }

  /**
   * Create a [[StructType]] from a number of column definitions.
   */
  override def visitComplexColTypeList(
      ctx: ComplexColTypeListContext): Seq[StructField] = withOrigin(ctx) {
    ctx.complexColType().asScala.map(visitComplexColType).toSeq
  }

  /**
   * Create a [[StructField]] from a column definition.
   */
  override def visitComplexColType(ctx: ComplexColTypeContext): StructField = withOrigin(ctx) {
    val structField = StructField(
      name = ctx.identifier.getText,
      dataType = typedVisit(ctx.dataType()))
    // 忽略null和注释解析
    structField
  }

}
