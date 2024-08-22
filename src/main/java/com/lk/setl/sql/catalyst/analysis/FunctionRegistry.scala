package com.lk.setl.sql.catalyst.analysis

import com.lk.setl.Logging

import java.util.Locale
import javax.annotation.concurrent.GuardedBy
import scala.collection.mutable
import scala.reflect.ClassTag
import com.lk.setl.sql.AnalysisException
import com.lk.setl.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import com.lk.setl.sql.catalyst.expressions._
import com.lk.setl.sql.catalyst.expressions.aggregate._
import com.lk.setl.sql.catalyst.trees.TreeNodeTag
import com.lk.setl.sql.types._


/**
 * A catalog for looking up user defined functions, used by an [[Analyzer]].
 *
 * Note:
 *   1) The implementation should be thread-safe to allow concurrent access.
 *   2) the database name is always case-sensitive here, callers are responsible to
 *      format the database name w.r.t. case-sensitive config.
 */
trait FunctionRegistry {

  final def registerFunction(name: String, builder: FunctionBuilder): Unit = {
    val info = new ExpressionInfo(
      builder.getClass.getCanonicalName, null, name)
    registerFunction(name, info, builder)
  }

  def registerFunction(
    name: String,
    info: ExpressionInfo,
    builder: FunctionBuilder): Unit

  /* Create or replace a temporary function. */
  final def createOrReplaceTempFunction(name: String, builder: FunctionBuilder): Unit = {
    registerFunction(name, builder)
  }

  @throws[AnalysisException]("If function does not exist")
  def lookupFunction(name: String, children: Seq[Expression]): Expression

  /* List all of the registered function names. */
  def listFunction(): Seq[String]

  /* Get the class of the registered function by specified name. */
  def lookupFunction(name: String): Option[ExpressionInfo]

  /* Get the builder of the registered function by specified name. */
  def lookupFunctionBuilder(name: String): Option[FunctionBuilder]

  /** Drop a function and return whether the function existed. */
  def dropFunction(name: String): Boolean

  /** Checks if a function with a given name exists. */
  def functionExists(name: String): Boolean = lookupFunction(name).isDefined

  /** Clear all registered functions. */
  def clear(): Unit

  /** Create a copy of this registry with identical functions as this registry. */
  override def clone(): FunctionRegistry = throw new CloneNotSupportedException()
}

class SimpleFunctionRegistry extends FunctionRegistry with Logging {

  @GuardedBy("this")
  private val functionBuilders =
    new mutable.HashMap[String, (ExpressionInfo, FunctionBuilder)]

  // Resolution of the function name is always case insensitive, but the database name
  // depends on the caller
  private def normalizeFuncName(name: String): String = {
    name.toLowerCase(Locale.ROOT)
  }

  override def registerFunction(
      name: String,
      info: ExpressionInfo,
      builder: FunctionBuilder): Unit = synchronized {
    val normalizedName = normalizeFuncName(name)
    val newFunction = (info, builder)
    functionBuilders.put(normalizedName, newFunction) match {
      case Some(previousFunction) if previousFunction != newFunction =>
        logWarning(s"The function $normalizedName replaced a previously registered function.")
      case _ =>
    }
  }

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    // 查找生成函数
    val func = synchronized {
      functionBuilders.get(normalizeFuncName(name)).map(_._2).getOrElse {
        throw new AnalysisException(s"undefined function $name")
      }
    }
    // 生成函数
    func(children)
  }

  override def listFunction(): Seq[String] = synchronized {
    functionBuilders.iterator.map(_._1).toList
  }

  override def lookupFunction(name: String): Option[ExpressionInfo] = synchronized {
    functionBuilders.get(normalizeFuncName(name)).map(_._1)
  }

  override def lookupFunctionBuilder(
      name: String): Option[FunctionBuilder] = synchronized {
    functionBuilders.get(normalizeFuncName(name)).map(_._2)
  }

  override def dropFunction(name: String): Boolean = synchronized {
    functionBuilders.remove(normalizeFuncName(name)).isDefined
  }

  override def clear(): Unit = synchronized {
    functionBuilders.clear()
  }

  override def clone(): SimpleFunctionRegistry = synchronized {
    val registry = new SimpleFunctionRegistry
    functionBuilders.iterator.foreach { case (name, (info, builder)) =>
      registry.registerFunction(name, info, builder)
    }
    registry
  }
}

/**
 * A trivial catalog that returns an error when a function is requested. Used for testing when all
 * functions are already filled in and the analyzer needs only to resolve attribute references.
 */
object EmptyFunctionRegistry extends FunctionRegistry {
  override def registerFunction(
      name: String, info: ExpressionInfo, builder: FunctionBuilder): Unit = {
    throw new UnsupportedOperationException
  }

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    throw new UnsupportedOperationException
  }

  override def listFunction(): Seq[String] = {
    throw new UnsupportedOperationException
  }

  override def lookupFunction(name: String): Option[ExpressionInfo] = {
    throw new UnsupportedOperationException
  }

  override def lookupFunctionBuilder(name: String): Option[FunctionBuilder] = {
    throw new UnsupportedOperationException
  }

  override def dropFunction(name: String): Boolean = {
    throw new UnsupportedOperationException
  }

  override def clear(): Unit = {
    throw new UnsupportedOperationException
  }

  override def clone(): FunctionRegistry = this
}

/**
 * 内置注册的函数
 */
object FunctionRegistry {

  type FunctionBuilder = Seq[Expression] => Expression

  val FUNC_ALIAS = TreeNodeTag[String]("functionAliasName")

  // 内置函数
  // Note: Whenever we add a new entry here, make sure we also update ExpressionToSQLSuite
  val expressions: Map[String, (ExpressionInfo, FunctionBuilder)] = Map(
    // misc non-aggregate functions
    expression[Coalesce]("coalesce"),
    expression[ProcessTimeWindow]("process_time_window"),
    expression[EventTimeWindow]("event_time_window"),
    expression[Explode]("explode"),
    expressionGeneratorOuter[Explode]("explode_outer"),
    expression[Greatest]("greatest"),
    expression[If]("if"),
    expression[IsNaN]("isnan"),
    expression[IfNull]("ifnull"),
    expression[IsNull]("isnull"),
    expression[IsNotNull]("isnotnull"),
    expression[Least]("least"),
    expression[NaNvl]("nanvl"),
    expression[NullIf]("nullif"),
    expression[Nvl]("nvl"),
    expression[Nvl2]("nvl2"),
    expression[CaseWhen]("when"),

    // math functions
    expression[Remainder]("mod", true),
    expression[UnaryMinus]("negative", true),
    expression[UnaryPositive]("positive"),

    expression[Add]("+"),
    expression[Subtract]("-"),
    expression[Multiply]("*"),
    expression[Divide]("/"),
    expression[IntegralDivide]("div"),
    expression[Remainder]("%"),

    // aggregate functions
    expression[Average]("avg"),
    expression[Count]("count"),
    expression[First]("first"),
    expression[First]("first_value", true),
    expression[Last]("last"),
    expression[Last]("last_value", true),
    expression[Max]("max"),
    expression[Average]("mean", true),
    expression[Min]("min"),
    expression[Sum]("sum"),

    // string functions
    expression[Chr]("char", true),
    expression[Chr]("chr"),
    expression[Length]("char_length", true),
    expression[Length]("character_length", true),
    expression[FormatString]("format_string"),
    expression[StringInstr]("instr"),
    expression[Lower]("lcase", true),
    expression[Length]("length"),
    expression[Like]("like"),
    expression[Lower]("lower"),
    expression[StringTrimLeft]("ltrim"),
    expression[FormatString]("printf", true),
    expression[RegExpExtract]("regexp_extract"),
    expression[RegExpExtractAll]("regexp_extract_all"),
    expression[RegExpReplace]("regexp_replace"),
    expression[StringReplace]("replace"),
    expression[RLike]("rlike"),
    expression[StringTrimRight]("rtrim"),
    expression[StringSplit]("split"),
    expression[Substring]("substr", true),
    expression[Substring]("substring"),
    expression[StringTrim]("trim"),
    expression[Upper]("ucase", true),
    expression[Upper]("upper"),

    // predicates
    expression[And]("and"),
    expression[In]("in"),
    expression[Not]("not"),
    expression[Or]("or"),

    // comparison operators
    expression[EqualNullSafe]("<=>"),
    expression[EqualTo]("="),
    expression[EqualTo]("=="),
    expression[GreaterThan](">"),
    expression[GreaterThanOrEqual](">="),
    expression[LessThan]("<"),
    expression[LessThanOrEqual]("<="),
    expression[Not]("!"),

    // cast
    expression[Cast]("cast"),
    // Cast aliases (SPARK-16730)
    castAlias("boolean", BooleanType),
    castAlias("int", IntegerType),
    castAlias("bigint", LongType),
    castAlias("float", FloatType),
    castAlias("double", DoubleType),
    castAlias("binary", BinaryType),
    castAlias("string", StringType),

  )

  val builtin: SimpleFunctionRegistry = {
    val fr = new SimpleFunctionRegistry
    expressions.foreach {
      case (name, (info, builder)) => fr.registerFunction(name, info, builder)
    }
    fr
  }

  val functionSet: Set[String] = builtin.listFunction().toSet

  /** See usage above. */
  private def expression[T <: Expression](name: String, setAlias: Boolean = false)
      (implicit tag: ClassTag[T]): (String, (ExpressionInfo, FunctionBuilder)) = {

    // For `RuntimeReplaceable`, skip the constructor with most arguments, which is the main
    // constructor and contains non-parameter `child` and should not be used as function builder.
    val constructors = if (classOf[RuntimeReplaceable].isAssignableFrom(tag.runtimeClass)) {
      val all = tag.runtimeClass.getConstructors
      val maxNumArgs = all.map(_.getParameterCount).max
      all.filterNot(_.getParameterCount == maxNumArgs)
    } else {
      tag.runtimeClass.getConstructors
    }
    // 看看我们是否能找到一个接受Seq[Expression]的构造函数
    // See if we can find a constructor that accepts Seq[Expression]
    val varargCtor = constructors.find(_.getParameterTypes.toSeq == Seq(classOf[Seq[_]]))
    val builder = (expressions: Seq[Expression]) => {
      if (varargCtor.isDefined) {
        // If there is an apply method that accepts Seq[Expression], use that one.
        try {
          val exp = varargCtor.get.newInstance(expressions).asInstanceOf[Expression]
          if (setAlias) exp.setTagValue(FUNC_ALIAS, name)
          exp
        } catch {
          // the exception is an invocation exception. To get a meaningful message, we need the
          // cause.
          case e: Exception => throw new AnalysisException(e.getCause.getMessage)
        }
      } else {
        // Otherwise, find a constructor method that matches the number of arguments, and use that.
        val params = Seq.fill(expressions.size)(classOf[Expression])
        val f = constructors.find(_.getParameterTypes.toSeq == params).getOrElse {
          val validParametersCount = constructors
            .filter(_.getParameterTypes.forall(_ == classOf[Expression]))
            .map(_.getParameterCount).distinct.sorted
          val invalidArgumentsMsg = if (validParametersCount.length == 0) {
            s"Invalid arguments for function $name"
          } else {
            val expectedNumberOfParameters = if (validParametersCount.length == 1) {
              validParametersCount.head.toString
            } else {
              validParametersCount.init.mkString("one of ", ", ", " and ") +
                validParametersCount.last
            }
            s"Invalid number of arguments for function $name. " +
              s"Expected: $expectedNumberOfParameters; Found: ${params.length}"
          }
          throw new AnalysisException(invalidArgumentsMsg)
        }
        try {
          val exp = f.newInstance(expressions : _*).asInstanceOf[Expression]
          if (setAlias) exp.setTagValue(FUNC_ALIAS, name)
          exp
        } catch {
          // the exception is an invocation exception. To get a meaningful message, we need the
          // cause.
          case e: Exception => throw new AnalysisException(e.getCause.getMessage)
        }
      }
    }

    (name, (expressionInfo[T](name), builder))
  }

  /**
   * Creates a function registry lookup entry for cast aliases (SPARK-16730).
   * For example, if name is "int", and dataType is IntegerType, this means int(x) would become
   * an alias for cast(x as IntegerType).
   * See usage above.
   */
  private def castAlias(
      name: String,
      dataType: DataType): (String, (ExpressionInfo, FunctionBuilder)) = {
    val builder = (args: Seq[Expression]) => {
      if (args.size != 1) {
        throw new AnalysisException(s"Function $name accepts only one argument")
      }
      Cast(args.head, dataType)
    }
    val clazz = scala.reflect.classTag[Cast].runtimeClass
    val usage = "_FUNC_(expr) - Casts the value `expr` to the target data type `_FUNC_`."
    val expressionInfo =
      new ExpressionInfo(clazz.getCanonicalName, null, name, usage, "", "", "", "", "2.0.1", "")
    (name, (expressionInfo, builder))
  }

  /**
   * Creates an [[ExpressionInfo]] for the function as defined by expression T using the given name.
   */
  private def expressionInfo[T <: Expression : ClassTag](name: String): ExpressionInfo = {
    val clazz = scala.reflect.classTag[T].runtimeClass
    val df = clazz.getAnnotation(classOf[ExpressionDescription])
    if (df != null) {
      if (df.extended().isEmpty) {
        new ExpressionInfo(
          clazz.getCanonicalName,
          null,
          name,
          df.usage(),
          df.arguments(),
          df.examples(),
          df.note(),
          df.group(),
          df.since(),
          df.deprecated())
      } else {
        // This exists for the backward compatibility with old `ExpressionDescription`s defining
        // the extended description in `extended()`.
        new ExpressionInfo(clazz.getCanonicalName, null, name, df.usage(), df.extended())
      }
    } else {
      new ExpressionInfo(clazz.getCanonicalName, name)
    }
  }

  private def expressionGeneratorOuter[T <: Generator : ClassTag](name: String)
    : (String, (ExpressionInfo, FunctionBuilder)) = {
    val (_, (info, generatorBuilder)) = expression[T](name)
    val outerBuilder = (args: Seq[Expression]) => {
      GeneratorOuter(generatorBuilder(args).asInstanceOf[Generator])
    }
    (name, (info, outerBuilder))
  }
}
