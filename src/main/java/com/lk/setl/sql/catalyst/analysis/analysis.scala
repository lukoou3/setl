package com.lk.setl.sql.catalyst.analysis

import com.lk.setl.sql.AnalysisException
import com.lk.setl.sql.catalyst.QueryPlanningTracker
import com.lk.setl.sql.catalyst.expressions._
import com.lk.setl.sql.catalyst.plans.logical.{AnalysisHelper, Generate, LogicalPlan, Project, RelationPlaceholder}
import com.lk.setl.sql.catalyst.rules.{Rule, RuleExecutor}
import com.lk.setl.sql.catalyst.util.toPrettySQL

import java.util.concurrent.atomic.AtomicBoolean

/**
 * Provides a logical query plan analyzer, which translates [[UnresolvedAttribute]]s and
 * [[UnresolvedRelation]]s into fully typed objects using information in a [[SessionCatalog]].
 */
class Analyzer(val tempViews: Map[String, RelationPlaceholder]) extends RuleExecutor[LogicalPlan] with CheckAnalysis {

  def executeAndCheck(plan: LogicalPlan, tracker: QueryPlanningTracker): LogicalPlan = {
    if (plan.analyzed) return plan
    AnalysisHelper.markInAnalyzer {
      val analyzed = executeAndTrack(plan, tracker)
      try {
        checkAnalysis(analyzed)
        analyzed
      } catch {
        case e: AnalysisException =>
          val ae = new AnalysisException(e.message, e.line, e.startPosition, Option(analyzed))
          ae.setStackTrace(e.getStackTrace)
          throw ae
      }
    }
  }

  override def execute(plan: LogicalPlan): LogicalPlan = {
    // 不需要解析视图
    //AnalysisContext.reset()
    try {
      executeSameContext(plan)
    } finally {
      //AnalysisContext.reset()
    }
  }

  private def executeSameContext(plan: LogicalPlan): LogicalPlan = super.execute(plan)

  def resolver: Resolver = caseInsensitiveResolution

  /**
   * If the plan cannot be resolved within maxIterations, analyzer will throw exception to inform
   * user to increase the value of SQLConf.ANALYZER_MAX_ITERATIONS.
   */
  protected def fixedPoint =
    FixedPoint(
      100, //conf.analyzerMaxIterations,
      errorOnExceed = true,
      maxIterationsSetting = "sql.analyzer.maxIterations")

  override protected def batches: Seq[Batch] = Seq(
    Batch("Resolution", fixedPoint,
      ResolveRelations ::
      ResolveReferences ::
      ResolveGenerate ::
      ResolveFunctions ::
      ResolveAliases ::
      TypeCoercion.typeCoercionRules : _*
    )
  )

  /**
   * Replaces [[UnresolvedRelation]]s with concrete relations from the catalog.
   */
  object ResolveRelations extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case u @ UnresolvedRelation(ident) if ident.size == 1=>
        tempViews.get(ident.head).getOrElse(u)
    }
  }

  /**
   * 解析替换函数
   * Replaces [[UnresolvedFunction]]s with concrete [[Expression]]s.
   */
  object ResolveFunctions extends Rule[LogicalPlan] {
    val trimWarningEnabled = new AtomicBoolean(true)
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case q: LogicalPlan =>
        q transformExpressions {
          case u if !u.childrenResolved => u // Skip until children are resolved.
          /*case u: UnresolvedAttribute if resolver(u.name, VirtualColumn.hiveGroupingIdName) =>
            withPosition(u) {
              Alias(GroupingID(Nil), VirtualColumn.hiveGroupingIdName)()
            }*/
          case u @ UnresolvedGenerator(name, children) =>
            withPosition(u) {
              // 匹配函数
              FunctionRegistry.builtin.lookupFunction(name, children) match {
                case generator: Generator => generator
                case other =>
                  failAnalysis(s"$name is expected to be a generator. However, " +
                    s"its class is ${other.getClass.getCanonicalName}, which is not a generator.")
              }
            }
          case u @ UnresolvedFunction(funcId, arguments, filter) =>
            withPosition(u) {
              // 匹配函数
              FunctionRegistry.builtin.lookupFunction(funcId, arguments) match {
                case other if filter.isDefined =>
                  throw new AnalysisException("DISTINCT or FILTER specified, " +
                    s"but ${other.prettyName} is not an aggregate function")
                case e: String2TrimExpression if arguments.size == 2 =>
                  if (trimWarningEnabled.get) {
                    log.warn("Two-parameter TRIM/LTRIM/RTRIM function signatures are deprecated." +
                      " Use SQL syntax `TRIM((BOTH | LEADING | TRAILING)? trimStr FROM str)`" +
                      " instead.")
                    trimWarningEnabled.set(false)
                  }
                  e
                case other =>
                  other
              }
            }
        }
    }
  }

  /**
   * Resolves the attribute, column value and extract value expressions(s) by traversing the
   * input expression in bottom-up manner. In order to resolve the nested complex type fields
   * correctly, this function makes use of `throws` parameter to control when to raise an
   * AnalysisException.
   *
   * Example :
   * SELECT a.b FROM t ORDER BY b[0].d
   *
   * In the above example, in b needs to be resolved before d can be resolved. Given we are
   * doing a bottom up traversal, it will first attempt to resolve d and fail as b has not
   * been resolved yet. If `throws` is false, this function will handle the exception by
   * returning the original attribute. In this case `d` will be resolved in subsequent passes
   * after `b` is resolved.
   */
  protected[sql] def resolveExpressionBottomUp(
      expr: Expression,
      plan: LogicalPlan,
      throws: Boolean = false): Expression = {
    if (expr.resolved) return expr
    // Resolve expression in one round.
    // If throws == false or the desired attribute doesn't exist
    // (like try to resolve `a.b` but `a` doesn't exist), fail and return the origin one.
    // Else, throw exception.
    try {
      expr transformUp {
        // case GetColumnByOrdinal(ordinal, _) => plan.output(ordinal)
        case u @ UnresolvedAttribute(nameParts) =>
          val result =
            withPosition(u) {
              plan.resolve(nameParts, resolver)
                .getOrElse(u)
            }
          logDebug(s"Resolving $u to $result")
          result
        case UnresolvedExtractValue(child, fieldName) if child.resolved =>
          ExtractValue(child, fieldName, resolver)
      }
    } catch {
      case a: AnalysisException if !throws => expr
    }
  }

  /**
   * Replaces [[UnresolvedAttribute]]s with concrete [[AttributeReference]]s from
   * a logical plan node's children.
   */
  object ResolveReferences extends Rule[LogicalPlan] {

    /**
     * 通过自上而下遍历输入表达式来解析属性并提取值表达式。遍历是以自上而下的方式完成的，因为我们需要跳过未绑定的lambda函数表达式。
     * lambda表达式在不同的规则ResolveLambdaVariables中解析
     * Resolves the attribute and extract value expressions(s) by traversing the
     * input expression in top down manner. The traversal is done in top-down manner as
     * we need to skip over unbound lambda function expression. The lambda expressions are
     * resolved in a different rule [[ResolveLambdaVariables]]
     *
     * Example :
     * SELECT transform(array(1, 2, 3), (x, i) -> x + i)"
     *
     * In the case above, x and i are resolved as lambda variables in [[ResolveLambdaVariables]]
     *
     * 在此例程中，未解决的属性从输入计划的子属性中解析出来。
     * Note : In this routine, the unresolved attributes are resolved from the input plan's
     * children attributes.
     *
     * @param e The expression need to be resolved.
     * @param q The LogicalPlan whose children are used to resolve expression's attribute.
     * @param trimAlias When true, trim unnecessary alias of `GetStructField`. Note that,
     *                  we cannot trim the alias of top-level `GetStructField`, as we should
     *                  resolve `UnresolvedAttribute` to a named expression. The caller side
     *                  can trim the alias of top-level `GetStructField` if it's safe to do so.
     * @return resolved Expression.
     */
    private def resolveExpressionTopDown(
        e: Expression,
        q: LogicalPlan,
        trimAlias: Boolean = false): Expression = {

      def innerResolve(e: Expression, isTopLevel: Boolean): Expression = {
        if (e.resolved) return e
        e match {
          // case f: LambdaFunction if !f.bound => f
          case u @ UnresolvedAttribute(nameParts) =>
            println(s"get UnresolvedAttribute")
            // Leave unchanged if resolution fails. Hopefully will be resolved next round.
            val resolved =
              withPosition(u) {
                q.resolveChildren(nameParts, resolver)
                  .getOrElse(u)
              }
            val result = resolved match {
              // As the comment of method `resolveExpressionTopDown`'s param `trimAlias` said,
              // when trimAlias = true, we will trim unnecessary alias of `GetStructField` and
              // we won't trim the alias of top-level `GetStructField`. Since we will call
              // CleanupAliases later in Analyzer, trim non top-level unnecessary alias of
              // `GetStructField` here is safe.
              case Alias(s: GetStructField, _) if trimAlias && !isTopLevel => s
              case others => others
            }
            logDebug(s"Resolving $u to $result")
            result
          // 获取Array,Struct,Map属性
          case UnresolvedExtractValue(child, fieldExpr) if child.resolved =>
            ExtractValue(child, fieldExpr, resolver)
          case _ => e.mapChildren(innerResolve(_, isTopLevel = false))
        }
      }

      innerResolve(e, isTopLevel = true)
    }

    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case p: LogicalPlan if !p.childrenResolved => p
      // A special case for Generate, because the output of Generate should not be resolved by
      // ResolveReferences. Attributes in the output will be resolved by ResolveGenerate.
      case g @ Generate(generator, _, _, _, _, _) if generator.resolved => g

      case g @ Generate(generator, join, outer, qualifier, output, child) =>
        val newG = resolveExpressionBottomUp(generator, child, throws = true)
        if (newG.fastEquals(generator)) {
          g
        } else {
          Generate(newG.asInstanceOf[Generator], join, outer, qualifier, output, child)
        }
      case q: LogicalPlan =>
        logTrace(s"Attempting to resolve ${q.simpleString(25)}")
        q.mapExpressions(resolveExpressionTopDown(_, q))
    }
  }

  /**
   * Replaces [[UnresolvedAlias]]s with concrete aliases.
   */
  object ResolveAliases extends Rule[LogicalPlan] {
    private def assignAliases(exprs: Seq[NamedExpression]) = {
      exprs.map(_.transformUp { case u @ UnresolvedAlias(child, optGenAliasFunc) =>
          child match {
            case ne: NamedExpression => ne
            //case go @ GeneratorOuter(g: Generator) if g.resolved => MultiAlias(go, Nil)
            case e if !e.resolved => u
            //case g: Generator => MultiAlias(g, Nil)
            case c @ Cast(ne: NamedExpression, _, _) => Alias(c, ne.name)()
            case e: ExtractValue => Alias(e, toPrettySQL(e))()
            case e if optGenAliasFunc.isDefined =>
              Alias(child, optGenAliasFunc.get.apply(e))()
            case e => Alias(e, toPrettySQL(e))()
          }
        }
      ).asInstanceOf[Seq[NamedExpression]]
    }

    private def hasUnresolvedAlias(exprs: Seq[NamedExpression]) =
      exprs.exists(_.find(_.isInstanceOf[UnresolvedAlias]).isDefined)

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case Project(projectList, child) if child.resolved && hasUnresolvedAlias(projectList) =>
        Project(assignAliases(projectList), child)
    }
  }

  /**
   * Rewrites table generating expressions that either need one or more of the following in order
   * to be resolved:
   *  - concrete attribute references for their output.
   *  - to be relocated from a SELECT clause (i.e. from  a [[Project]]) into a [[Generate]]).
   *
   * Names for the output [[Attribute]]s are extracted from [[Alias]] or [[MultiAlias]] expressions
   * that wrap the [[Generator]].
   */
  object ResolveGenerate extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case g: Generate if !g.child.resolved || !g.generator.resolved => g
      case g: Generate if !g.resolved =>
        g.copy(generatorOutput = makeGeneratorOutput(g.generator, g.generatorOutput.map(_.name)))
    }

    /**
     * Construct the output attributes for a [[Generator]], given a list of names.  If the list of
     * names is empty names are assigned from field names in generator.
     */
    private[analysis] def makeGeneratorOutput(
        generator: Generator,
        names: Seq[String]): Seq[Attribute] = {
      val elementAttrs = generator.elementSchema.toAttributes

      if (names.length == elementAttrs.length) {
        names.zip(elementAttrs).map {
          case (name, attr) => attr.withName(name)
        }
      } else if (names.isEmpty) {
        elementAttrs
      } else {
        failAnalysis(
          "The number of aliases supplied in the AS clause does not match the number of columns " +
          s"output by the UDTF expected ${elementAttrs.size} aliases but got " +
          s"${names.mkString(",")} ")
      }
    }
  }

}


