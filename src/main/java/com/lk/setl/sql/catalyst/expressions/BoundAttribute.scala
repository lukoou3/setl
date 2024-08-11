package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.Logging
import com.lk.setl.sql.Row
import com.lk.setl.sql.catalyst.analysis.attachTree
import com.lk.setl.sql.types.DataType


/**
 * 绑定引用指向输入元组中的特定槽，从而更有效地检索实际值。
 * 然而，由于列修剪等操作会改变中间元组的布局，因此应在所有此类转换后运行BindReferences。
 * A bound reference points to a specific slot in the input tuple, allowing the actual value
 * to be retrieved more efficiently.  However, since operations like column pruning can change
 * the layout of intermediate tuples, BindReferences should be run after all such transformations.
 */
case class BoundReference(ordinal: Int, dataType: DataType)
  extends LeafExpression {
  var test = 1

  override def toString: String = s"input[$ordinal, ${dataType.simpleString}]"


  // Use special getter for primitive types (for UnsafeRow)
  override def eval(input: Row): Any = {
    input.get(ordinal)
  }

}

object BindReferences extends Logging {

  def bindReference[A <: Expression](
      expression: A,
      input: AttributeSeq,
      allowFailures: Boolean = false): A = {
    expression.transform { case a: AttributeReference =>
      attachTree(a, "Binding attribute") {
        val ordinal = input.indexOf(a.exprId)
        if (ordinal == -1) {
          if (allowFailures) {
            a
          } else {
            sys.error(s"Couldn't find $a in ${input.attrs.mkString("[", ",", "]")}")
          }
        } else {
          BoundReference(ordinal, a.dataType)
        }
      }
    }.asInstanceOf[A] // Kind of a hack, but safe.  TODO: Tighten return type when possible.
  }

  /**
   * A helper function to bind given expressions to an input schema.
   */
  def bindReferences[A <: Expression](
      expressions: Seq[A],
      input: AttributeSeq): Seq[A] = {
    expressions.map(BindReferences.bindReference(_, input))
  }
}
