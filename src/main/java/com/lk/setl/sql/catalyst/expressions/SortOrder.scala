package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.catalyst.analysis.TypeCheckResult
import com.lk.setl.sql.types._

abstract sealed class SortDirection {
  def sql: String
  def defaultNullOrdering: NullOrdering
}

abstract sealed class NullOrdering {
  def sql: String
}

case object Ascending extends SortDirection {
  override def sql: String = "ASC"
  override def defaultNullOrdering: NullOrdering = NullsFirst
}

case object Descending extends SortDirection {
  override def sql: String = "DESC"
  override def defaultNullOrdering: NullOrdering = NullsLast
}

case object NullsFirst extends NullOrdering{
  override def sql: String = "NULLS FIRST"
}

case object NullsLast extends NullOrdering{
  override def sql: String = "NULLS LAST"
}

/**
 * An expression that can be used to sort a tuple.  This class extends expression primarily so that
 * transformations over expression will descend into its child.
 * `sameOrderExpressions` is a set of expressions with the same sort order as the child. It is
 * derived from equivalence relation in an operator, e.g. left/right keys of an inner sort merge
 * join.
 */
case class SortOrder(
    child: Expression,
    direction: SortDirection,
    nullOrdering: NullOrdering,
    sameOrderExpressions: Seq[Expression])
  extends Expression with Unevaluable {

  override def children: Seq[Expression] = child +: sameOrderExpressions

  override def checkInputDataTypes(): TypeCheckResult = {
    if (RowOrdering.isOrderable(dataType)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(s"cannot sort data type ${dataType.catalogString}")
    }
  }

  override def dataType: DataType = child.dataType

  override def toString: String = s"$child ${direction.sql} ${nullOrdering.sql}"
  override def sql: String = child.sql + " " + direction.sql + " " + nullOrdering.sql

  def isAscending: Boolean = direction == Ascending

  def satisfies(required: SortOrder): Boolean = {
    children.exists(required.child.semanticEquals) &&
      direction == required.direction && nullOrdering == required.nullOrdering
  }
}

object SortOrder {
  def apply(
     child: Expression,
     direction: SortDirection,
     sameOrderExpressions: Seq[Expression] = Seq.empty): SortOrder = {
    new SortOrder(child, direction, direction.defaultNullOrdering, sameOrderExpressions)
  }

  /**
   * Returns if a sequence of SortOrder satisfies another sequence of SortOrder.
   *
   * SortOrder sequence A satisfies SortOrder sequence B if and only if B is an equivalent of A
   * or of A's prefix. Here are examples of ordering A satisfying ordering B:
   * <ul>
   *   <li>ordering A is [x, y] and ordering B is [x]</li>
   *   <li>ordering A is [x(sameOrderExpressions=x1)] and ordering B is [x1]</li>
   *   <li>ordering A is [x(sameOrderExpressions=x1), y] and ordering B is [x1]</li>
   * </ul>
   */
  def orderingSatisfies(ordering1: Seq[SortOrder], ordering2: Seq[SortOrder]): Boolean = {
    if (ordering2.isEmpty) {
      true
    } else if (ordering2.length > ordering1.length) {
      false
    } else {
      ordering2.zip(ordering1).forall {
        case (o2, o1) => o1.satisfies(o2)
      }
    }
  }
}

