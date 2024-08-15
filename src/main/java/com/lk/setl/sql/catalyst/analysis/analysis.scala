package com.lk.setl.sql.catalyst.analysis

import com.lk.setl.sql.AnalysisException
import com.lk.setl.sql.catalyst.expressions._
import com.lk.setl.sql.catalyst.plans.logical.LogicalPlan
import com.lk.setl.sql.catalyst.rules.{Rule, RuleExecutor}

import java.util.concurrent.atomic.AtomicBoolean

/**
 * Provides a logical query plan analyzer, which translates [[UnresolvedAttribute]]s and
 * [[UnresolvedRelation]]s into fully typed objects using information in a [[SessionCatalog]].
 */
class Analyzer extends RuleExecutor[LogicalPlan]{

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

  )

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
            }
          case u @ UnresolvedGenerator(name, children) =>
            withPosition(u) {
              // 匹配函数
              v1SessionCatalog.lookupFunction(name, children) match {
                case generator: Generator => generator
                case other =>
                  failAnalysis(s"$name is expected to be a generator. However, " +
                    s"its class is ${other.getClass.getCanonicalName}, which is not a generator.")
              }
            }*/
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
      case q: LogicalPlan =>
        logTrace(s"Attempting to resolve ${q.simpleString(25)}")
        q.mapExpressions(resolveExpressionTopDown(_, q))
    }
  }
}


