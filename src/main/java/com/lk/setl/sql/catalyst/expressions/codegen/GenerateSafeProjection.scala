package com.lk.setl.sql.catalyst.expressions.codegen

import com.lk.setl.sql.{ArrayData, GenericArrayData, GenericRow, Row}
import com.lk.setl.sql.catalyst.expressions._
import com.lk.setl.sql.catalyst.expressions.BindReferences.bindReferences
import com.lk.setl.sql.catalyst.expressions.codegen.Block._
import com.lk.setl.sql.types._

/**
 * Java can not access Projection (in package object)
 */
abstract class BaseProjection extends Projection {}

/**
 * Generates byte code that produces a [[Row]] object (not an [[UnsafeRow]]) that can update
 * itself based on a new input [[Row]] for a fixed set of [[Expression Expressions]].
 */
object GenerateSafeProjection extends CodeGenerator[Seq[Expression], Projection] {

  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    //in.map(ExpressionCanonicalizer.execute)
    in

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    bindReferences(in, inputSchema)

  private def createCodeForStruct(
      ctx: CodegenContext,
      input: String,
      schema: StructType): ExprCode = {
    // Puts `input` in a local variable to avoid to re-evaluate it if it's a statement.
    val tmpInput = ctx.freshName("tmpInput")
    val output = ctx.freshName("safeRow")
    val values = ctx.freshName("values")

    val rowClass = classOf[GenericRow].getName

    val fieldWriters = schema.map(_.dataType).zipWithIndex.map { case (dt, i) =>
      val converter = convertToSafe(
        ctx,
        JavaCode.expression(CodeGenerator.getValue(tmpInput, dt, i.toString), dt),
        dt)
      s"""
        if (!$tmpInput.isNullAt($i)) {
          ${converter.code}
          $values[$i] = ${converter.value};
        }
      """
    }
    val allFields = ctx.splitExpressions(
      expressions = fieldWriters,
      funcName = "writeFields",
      arguments = Seq("Row" -> tmpInput, "Object[]" -> values)
    )
    val code =
      code"""
         |final Row $tmpInput = $input;
         |final Object[] $values = new Object[${schema.length}];
         |$allFields
         |final Row $output = new $rowClass($values);
       """.stripMargin

    ExprCode(code, FalseLiteral, JavaCode.variable(output, classOf[Row]))
  }

  private def createCodeForArray(
      ctx: CodegenContext,
      input: String,
      elementType: DataType): ExprCode = {
    // Puts `input` in a local variable to avoid to re-evaluate it if it's a statement.
    val tmpInput = ctx.freshName("tmpInput")
    val output = ctx.freshName("safeArray")
    val values = ctx.freshName("values")
    val numElements = ctx.freshName("numElements")
    val index = ctx.freshName("index")
    val arrayClass = classOf[GenericArrayData].getName

    val elementConverter = convertToSafe(
      ctx,
      JavaCode.expression(CodeGenerator.getValue(tmpInput, elementType, index), elementType),
      elementType)
    val code = code"""
      final ArrayData $tmpInput = $input;
      final int $numElements = $tmpInput.numElements();
      final Object[] $values = new Object[$numElements];
      for (int $index = 0; $index < $numElements; $index++) {
        if (!$tmpInput.isNullAt($index)) {
          ${elementConverter.code}
          $values[$index] = ${elementConverter.value};
        }
      }
      final ArrayData $output = new $arrayClass($values);
    """

    ExprCode(code, FalseLiteral, JavaCode.variable(output, classOf[ArrayData]))
  }

  private def convertToSafe(
      ctx: CodegenContext,
      input: ExprValue,
      dataType: DataType): ExprCode = dataType match {
    //case s: StructType => createCodeForStruct(ctx, input, s)
    //case ArrayType(elementType, _) => createCodeForArray(ctx, input, elementType)
    case _ => ExprCode(FalseLiteral, input)
  }

  def generate(
      expressions: Seq[Expression],
      subexpressionEliminationEnabled: Boolean): Projection = {
    create(canonicalize(expressions), subexpressionEliminationEnabled)
  }

  protected def create(references: Seq[Expression]): Projection = {
    create(references, false)
  }

  protected def create(expressions: Seq[Expression], useSubexprElimination: Boolean): Projection = {
    val ctx = newCodeGenContext()
    // 为啥spark这里没有使用useSubexprElimination
    val exprEvals = ctx.generateExpressions(expressions, useSubexprElimination)
    val expressionCodes = expressions.zipWithIndex.map {
      // case (NoOp, _) => ""
      case (e, i) =>
        //val evaluationCode = e.genCode(ctx)
        val evaluationCode = exprEvals(i)
        val converter = convertToSafe(ctx, evaluationCode.value, e.dataType)
        evaluationCode.code +
          s"""
            if (${evaluationCode.isNull}) {
              mutableRow.setNullAt($i);
            } else {
              ${converter.code}
              ${CodeGenerator.setColumn("mutableRow", e.dataType, i, converter.value)};
            }
          """
    }
    val evalSubexpr = ctx.subexprFunctionsCode
    val allExpressions = ctx.splitExpressionsWithCurrentInputs(expressionCodes)

    val codeBody = s"""
      public java.lang.Object generate(Object[] references) {
        return new SpecificSafeProjection(references);
      }

      class SpecificSafeProjection extends ${classOf[BaseProjection].getName} {

        private Object[] references;
        private Row mutableRow;
        ${ctx.declareMutableStates()}

        public SpecificSafeProjection(Object[] references) {
          this.references = references;
          mutableRow = (Row) references[references.length - 1];
          ${ctx.initMutableStates()}
        }

        public void initialize(int partitionIndex) {
          ${ctx.initPartition()}
        }

        public Row apply(Row ${ctx.INPUT_ROW}) {
          $evalSubexpr
          $allExpressions
          return mutableRow;
        }

        ${ctx.declareAddedFunctions()}
      }
    """

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"code for ${expressions.mkString(",")}:\n${CodeFormatter.format(code)}")

    val (clazz, _) = CodeGenerator.compile(code)
    val resultRow = new GenericRow(new Array[Any](expressions.length))
    clazz.generate(ctx.references.toArray :+ resultRow).asInstanceOf[Projection]
  }
}
