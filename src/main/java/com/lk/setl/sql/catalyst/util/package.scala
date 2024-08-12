package com.lk.setl.sql.catalyst

import com.lk.setl.Logging

import java.util.concurrent.atomic.AtomicBoolean

package object util extends Logging{
  /** Whether we have warned about plan string truncation yet. */
  private val truncationWarningPrinted = new AtomicBoolean(false)

  def quoteIdentifier(name: String): String = {
    // Escapes back-ticks within the identifier name with double-back-ticks, and then quote the
    // identifier with back-ticks.
    "`" + name.replace("`", "``") + "`"
  }


  def sideBySide(left: String, right: String): Seq[String] = {
    sideBySide(left.split("\n"), right.split("\n"))
  }

  def sideBySide(left: Seq[String], right: Seq[String]): Seq[String] = {
    val maxLeftSize = left.map(_.length).max
    val leftPadded = left ++ Seq.fill(math.max(right.size - left.size, 0))("")
    val rightPadded = right ++ Seq.fill(math.max(left.size - right.size, 0))("")

    leftPadded.zip(rightPadded).map {
      case (l, r) => (if (l == r) " " else "!") + l + (" " * ((maxLeftSize - l.length) + 3)) + r
    }
  }

  /**
   * Format a sequence with semantics similar to calling .mkString(). Any elements beyond
   * maxNumToStringFields will be dropped and replaced by a "... N more fields" placeholder.
   *
   * @return the trimmed and formatted string.
   */
  def truncatedString[T](
    seq: Seq[T],
    start: String,
    sep: String,
    end: String,
    maxFields: Int): String = {
    if (seq.length > maxFields) {
      if (truncationWarningPrinted.compareAndSet(false, true)) {
        logWarning(
          "Truncated the string representation of a plan since it was too large. This " +
            s"behavior can be adjusted by setting 'MAX_TO_STRING_FIELDS'.")
      }
      val numFields = math.max(0, maxFields - 1)
      seq.take(numFields).mkString(
        start, sep, sep + "... " + (seq.length - numFields) + " more fields" + end)
    } else {
      seq.mkString(start, sep, end)
    }
  }

  /** Shorthand for calling truncatedString() without start or end strings. */
  def truncatedString[T](seq: Seq[T], sep: String, maxFields: Int): String = {
    truncatedString(seq, "", sep, "", maxFields)
  }
}
