package com.lk.setl.sql.catalyst

import com.lk.setl.sql.AnalysisException
import com.lk.setl.sql.catalyst.trees.TreeNode

package object analysis {

  /**
   * Resolver should return true if the first string refers to the same entity as the second string.
   * For example, by using case insensitive equality.
   */
  type Resolver = (String, String) => Boolean

  val caseInsensitiveResolution = (a: String, b: String) => a.equalsIgnoreCase(b)
  val caseSensitiveResolution = (a: String, b: String) => a == b

  implicit class AnalysisErrorAt(t: TreeNode[_]) {
    /** Fails the analysis at the point where a specific tree node was parsed. */
    def failAnalysis(msg: String): Nothing = {
      throw new AnalysisException(msg, t.origin.line, t.origin.startPosition)
    }

    /** Fails the analysis at the point where a specific tree node was parsed. */
    def failAnalysis(msg: String, cause: Throwable): Nothing = {
      throw new AnalysisException(msg, t.origin.line, t.origin.startPosition, cause = Some(cause))
    }
  }

  /** Catches any AnalysisExceptions thrown by `f` and attaches `t`'s position if any. */
  def withPosition[A](t: TreeNode[_])(f: => A): A = {
    try f catch {
      case a: AnalysisException =>
        throw a.withPosition(t.origin.line, t.origin.startPosition)
    }
  }

  implicit class MultipartIdentifierHelper(parts: Seq[String]) {
    if (parts.isEmpty) {
      throw new AnalysisException("multi-part identifier cannot be empty.")
    }

    def quoted: String = parts.map(quoteIfNeeded).mkString(".")
  }

  def quoteIfNeeded(part: String): String = {
    if (part.contains(".") || part.contains("`")) {
      s"`${part.replace("`", "``")}`"
    } else {
      part
    }
  }

}
