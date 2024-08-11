package com.lk.setl.sql.catalyst.analysis

import com.lk.setl.sql.catalyst.trees.TreeNodeTag

object FunctionRegistry {
  val FUNC_ALIAS = TreeNodeTag[String]("functionAliasName")
}
