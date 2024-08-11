package com.lk.setl.sql.catalyst.analysis

import com.lk.setl.sql.types.DataType

object TypeCoercion {

  /**
   * The method finds a common type for data types that differ only in nullable flags, including
   * `nullable`, `containsNull` of [[ArrayType]] and `valueContainsNull` of [[MapType]].
   * If the input types are different besides nullable flags, None is returned.
   */
  def findCommonTypeDifferentOnlyInNullFlags(t1: DataType, t2: DataType): Option[DataType] = {
    if (t1 == t2) {
      Some(t1)
    } else {
      None
    }
  }

  def findCommonTypeDifferentOnlyInNullFlags(types: Seq[DataType]): Option[DataType] = {
    if (types.isEmpty) {
      None
    } else {
      types.tail.foldLeft[Option[DataType]](Some(types.head)) {
        case (Some(t1), t2) => findCommonTypeDifferentOnlyInNullFlags(t1, t2)
        case _ => None
      }
    }
  }

  /**
   * Check whether the given types are equal ignoring nullable, containsNull and valueContainsNull.
   */
  def haveSameType(types: Seq[DataType]): Boolean = {
    if (types.size <= 1) {
      true
    } else {
      val head = types.head
      types.tail.forall(_.sameType(head))
    }
  }


}
