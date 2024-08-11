package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.Row
import com.lk.setl.sql.types.{AtomicType, DataType, NullType}


/**
 * A base class for generated/interpreted row ordering.
 */
class BaseOrdering extends Ordering[Row] {
  def compare(a: Row, b: Row): Int = {
    throw new UnsupportedOperationException
  }
}



object RowOrdering {

  /**
   * Returns true iff the data type can be ordered (i.e. can be sorted).
   */
  def isOrderable(dataType: DataType): Boolean = dataType match {
    case NullType => true
    case dt: AtomicType => true
    case _ => false
  }

  /**
   * Returns true iff outputs from the expressions can be ordered.
   */
  def isOrderable(exprs: Seq[Expression]): Boolean = exprs.forall(e => isOrderable(e.dataType))

}
