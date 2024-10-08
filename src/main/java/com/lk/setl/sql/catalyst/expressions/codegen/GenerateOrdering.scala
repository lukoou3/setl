package com.lk.setl.sql.catalyst.expressions.codegen

import com.lk.setl.Logging

import java.io.ObjectInputStream
import com.lk.setl.sql.Row
import com.lk.setl.sql.catalyst.expressions._
import com.lk.setl.sql.catalyst.expressions.BindReferences.bindReferences
import com.lk.setl.sql.types.StructType
import com.lk.setl.util.Utils

/**
 * Generates bytecode for an [[Ordering]] of rows for a given set of expressions.
 */
object GenerateOrdering extends CodeGenerator[Seq[SortOrder], BaseOrdering] with Logging {

  protected def canonicalize(in: Seq[SortOrder]): Seq[SortOrder] =
    //in.map(ExpressionCanonicalizer.execute(_).asInstanceOf[SortOrder])
    in

  protected def bind(in: Seq[SortOrder], inputSchema: Seq[Attribute]): Seq[SortOrder] =
    bindReferences(in, inputSchema)

  /**
   * Creates a code gen ordering for sorting this schema, in ascending order.
   */
  def create(schema: StructType): BaseOrdering = {
    create(schema.zipWithIndex.map { case (field, ordinal) =>
      SortOrder(BoundReference(ordinal, field.dataType), Ascending)
    })
  }

  /**
   * Generates the code for comparing a struct type according to its natural ordering
   * (i.e. ascending order by field 1, then field 2, ..., then field n.
   */
  def genComparisons(ctx: CodegenContext, schema: StructType): String = {
    val ordering = schema.fields.map(_.dataType).zipWithIndex.map {
      case(dt, index) => SortOrder(BoundReference(index, dt), Ascending)
    }
    genComparisons(ctx, ordering)
  }

  /**
   * Creates the variables for ordering based on the given order.
   */
  private def createOrderKeys(
      ctx: CodegenContext,
      row: String,
      ordering: Seq[SortOrder]): Seq[ExprCode] = {
    ctx.INPUT_ROW = row
    // to use INPUT_ROW we must make sure currentVars is null
    ctx.currentVars = null
    // SPARK-33260: To avoid unpredictable modifications to `ctx` when `ordering` is a Stream, we
    // use `toIndexedSeq` to make the transformation eager.
    ordering.toIndexedSeq.map(_.child.genCode(ctx))
  }

  /**
   * Generates the code for ordering based on the given order.
   */
  def genComparisons(ctx: CodegenContext, ordering: Seq[SortOrder]): String = {
    val oldInputRow = ctx.INPUT_ROW
    val oldCurrentVars = ctx.currentVars
    val rowAKeys = createOrderKeys(ctx, "a", ordering)
    val rowBKeys = createOrderKeys(ctx, "b", ordering)
    val comparisons = rowAKeys.zip(rowBKeys).zipWithIndex.map { case ((l, r), i) =>
      val dt = ordering(i).child.dataType
      val asc = ordering(i).isAscending
      val nullOrdering = ordering(i).nullOrdering
      val lRetValue = nullOrdering match {
        case NullsFirst => "-1"
        case NullsLast => "1"
      }
      val rRetValue = nullOrdering match {
        case NullsFirst => "1"
        case NullsLast => "-1"
      }
      s"""
          |${l.code}
          |${r.code}
          |if (${l.isNull} && ${r.isNull}) {
          |  // Nothing
          |} else if (${l.isNull}) {
          |  return $lRetValue;
          |} else if (${r.isNull}) {
          |  return $rRetValue;
          |} else {
          |  int comp = ${ctx.genComp(dt, l.value, r.value)};
          |  if (comp != 0) {
          |    return ${if (asc) "comp" else "-comp"};
          |  }
          |}
      """.stripMargin
    }

    val code = ctx.splitExpressions(
      expressions = comparisons,
      funcName = "compare",
      arguments = Seq(("Row", "a"), ("Row", "b")),
      returnType = "int",
      makeSplitFunction = { body =>
        s"""
          |$body
          |return 0;
        """.stripMargin
      },
      foldFunctions = { funCalls =>
        funCalls.zipWithIndex.map { case (funCall, i) =>
          val comp = ctx.freshName("comp")
          s"""
            |int $comp = $funCall;
            |if ($comp != 0) {
            |  return $comp;
            |}
          """.stripMargin
        }.mkString
      })
    ctx.currentVars = oldCurrentVars
    ctx.INPUT_ROW = oldInputRow
    code
  }

  protected def create(ordering: Seq[SortOrder]): BaseOrdering = {
    val ctx = newCodeGenContext()
    val comparisons = genComparisons(ctx, ordering)
    val codeBody = s"""
      public SpecificOrdering generate(Object[] references) {
        return new SpecificOrdering(references);
      }

      class SpecificOrdering extends ${classOf[BaseOrdering].getName} {

        private Object[] references;
        ${ctx.declareMutableStates()}

        public SpecificOrdering(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
        }

        public int compare(Row a, Row b) {
          $comparisons
          return 0;
        }

        ${ctx.declareAddedFunctions()}
      }"""

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"Generated Ordering by ${ordering.mkString(",")}:\n${CodeFormatter.format(code)}")

    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(ctx.references.toArray).asInstanceOf[BaseOrdering]
  }
}

/**
 * A lazily generated row ordering comparator.
 */
class LazilyGeneratedOrdering(val ordering: Seq[SortOrder])  extends Ordering[Row]{

  def this(ordering: Seq[SortOrder], inputSchema: Seq[Attribute]) =
    this(bindReferences(ordering, inputSchema))

  @transient
  private[this] var generatedOrdering = GenerateOrdering.generate(ordering)

  def compare(a: Row, b: Row): Int = {
    generatedOrdering.compare(a, b)
  }

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    generatedOrdering = GenerateOrdering.generate(ordering)
  }

}

object LazilyGeneratedOrdering {

  /**
   * Creates a [[LazilyGeneratedOrdering]] for the given schema, in natural ascending order.
   */
  def forSchema(schema: StructType): LazilyGeneratedOrdering = {
    new LazilyGeneratedOrdering(schema.zipWithIndex.map {
      case (field, ordinal) =>
        SortOrder(BoundReference(ordinal, field.dataType), Ascending)
    })
  }
}
