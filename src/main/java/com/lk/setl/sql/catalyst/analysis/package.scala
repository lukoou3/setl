package com.lk.setl.sql.catalyst

import com.lk.setl.sql.catalyst.trees.TreeNode

import scala.util.control.NonFatal

package object analysis {

  /**
   * Resolver should return true if the first string refers to the same entity as the second string.
   * For example, by using case insensitive equality.
   */
  type Resolver = (String, String) => Boolean

  class TreeNodeException[TreeType <: TreeNode[_]](
    @transient val tree: TreeType,
    msg: String,
    cause: Throwable)
    extends Exception(msg, cause) {

    val treeString = tree.toString

    // Yes, this is the same as a default parameter, but... those don't seem to work with SBT
    // external project dependencies for some reason.
    def this(tree: TreeType, msg: String) = this(tree, msg, null)

    override def getMessage: String = {
      s"${super.getMessage}, tree:${if (treeString contains "\n") "\n" else " "}$tree"
    }
  }

  /**
   *  Wraps any exceptions that are thrown while executing `f` in a
   *  [[TreeNodeException]], attaching the provided `tree`.
   */
  def attachTree[TreeType <: TreeNode[_], A](tree: TreeType, msg: String = "")(f: => A): A = {
    try f catch {
      // SPARK-16748: We do not want SparkExceptions from job failures in the planning phase
      // to create TreeNodeException. Hence, wrap exception only if it is not SparkException.
      case NonFatal(e)  =>
        throw new TreeNodeException(tree, msg, e)
    }
  }
}
