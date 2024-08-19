package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.{ArrayData, GenericArrayData, GenericRow, Row}
import com.lk.setl.sql.catalyst.expressions.aggregate.NoOp
import com.lk.setl.sql.types._


/**
 * An interpreted version of a safe projection.
 *
 * @param expressions that produces the resulting fields. These expressions must be bound
 *                    to a schema.
 */
class InterpretedSafeProjection(expressions: Seq[Expression]) extends Projection {

  private[this] val subExprEliminationEnabled = false
  private[this] lazy val runtime = new SubExprEvaluationRuntime(100)
  private[this] val exprs = if (subExprEliminationEnabled) {
    runtime.proxyExpressions(expressions)
  } else {
    expressions
  }

  private[this] val mutableRow = new GenericRow(new Array[Any](expressions.length))

  /*private[this] val exprsWithWriters = expressions.zipWithIndex.filter {
    case (NoOp, _) => false
    case _ => true
  }.map { case (e, i) =>
    val converter = generateSafeValueConverter(e.dataType)
    val f = if (!e.nullable) {
      (v: Any) => mutableRow.update(i, converter(v))
    } else {
      (v: Any) => {
        if (v == null) {
          mutableRow.setNullAt(i)
        } else {
          mutableRow.update(i, converter(v))
        }
      }
    }
    (exprs(i), f)
  }*/

  private def generateSafeValueConverter(dt: DataType): Any => Any = dt match {
    case ArrayType(elemType, _) =>
      val elementConverter = generateSafeValueConverter(elemType)
      v => {
        val arrayValue = v.asInstanceOf[ArrayData]
        val result = new Array[Any](arrayValue.numElements())
        var i = 0
        while (i < arrayValue.length){
          result(i) = elementConverter(arrayValue.get(i))
          i += 1
        }
        new GenericArrayData(result)
      }

    case st: StructType =>
      val fieldTypes = st.fields.map(_.dataType)
      val fieldConverters = fieldTypes.map(generateSafeValueConverter)
      v => {
        val row = v.asInstanceOf[Row]
        val ar = new Array[Any](row.length)
        var idx = 0
        while (idx < row.length) {
          ar(idx) = fieldConverters(idx)(row.get(idx))
          idx += 1
        }
        new GenericRow(ar)
      }

    case _ => identity
  }

  override def apply(row: Row): Row = {
    if (subExprEliminationEnabled) {
      runtime.setInput(row)
    }

    var i = 0
    /*while (i < exprsWithWriters.length) {
      val (expr, writer) = exprsWithWriters(i)
      writer(expr.eval(row))
      i += 1
    }*/
    // 没有NoOp，也不需要转换, 没有unsafe版本row和array
    while (i < exprs.length) {
      mutableRow.update(i, exprs(i).eval(row))
      i += 1
    }
    mutableRow
  }
}

/**
 * Helper functions for creating an [[InterpretedSafeProjection]].
 */
object InterpretedSafeProjection {

  /**
   * Returns an [[SafeProjection]] for given sequence of bound Expressions.
   */
  def createProjection(exprs: Seq[Expression]): Projection = {
    // We need to make sure that we do not reuse stateful expressions.
    //val cleanedExpressions = exprs.map(_.transform {case s: Stateful => s.freshCopy()})
    val cleanedExpressions = exprs
    new InterpretedSafeProjection(cleanedExpressions)
  }
}
