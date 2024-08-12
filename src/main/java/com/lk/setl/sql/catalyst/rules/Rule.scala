package com.lk.setl.sql.catalyst.rules

import com.lk.setl.Logging
import com.lk.setl.sql.catalyst.trees.TreeNode

abstract class Rule[TreeType <: TreeNode[_]] extends Logging {

  /** Name for this rule, automatically inferred based on class name. */
  val ruleName: String = {
    val className = getClass.getName
    if (className endsWith "$") className.dropRight(1) else className
  }

  def apply(plan: TreeType): TreeType
}
