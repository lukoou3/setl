package com.lk.setl.util

import com.lk.setl.Logging

import java.io.IOException
import scala.util.control.NonFatal

object Utils extends Logging{
  /**
   * Safer than Class obj's getSimpleName which may throw Malformed class name error in scala.
   * This method mimics scalatest's getSimpleNameOfAnObjectsClass.
   */
  def getSimpleName(cls: Class[_]): String = {
    try {
      cls.getSimpleName
    } catch {
      // TODO: the value returned here isn't even quite right; it returns simple names
      // like UtilsSuite$MalformedClassObject$MalformedClass instead of MalformedClass
      // The exact value may not matter much as it's used in log statements
      case _: InternalError =>
        stripDollars(stripPackages(cls.getName))
    }
  }

  /**
   * Remove the packages from full qualified class name
   */
  private def stripPackages(fullyQualifiedName: String): String = {
    fullyQualifiedName.split("\\.").takeRight(1)(0)
  }

  /**
   * Remove trailing dollar signs from qualified class name,
   * and return the trailing part after the last dollar sign in the middle
   */
  private def stripDollars(s: String): String = {
    val lastDollarIndex = s.lastIndexOf('$')
    if (lastDollarIndex < s.length - 1) {
      // The last char is not a dollar sign
      if (lastDollarIndex == -1 || !s.contains("$iw")) {
        // The name does not have dollar sign or is not an interpreter
        // generated class, so we should return the full string
        s
      } else {
        // The class name is interpreter generated,
        // return the part after the last dollar sign
        // This is the same behavior as getClass.getSimpleName
        s.substring(lastDollarIndex + 1)
      }
    }
    else {
      // The last char is a dollar sign
      // Find last non-dollar char
      val lastNonDollarChar = s.reverse.find(_ != '$')
      lastNonDollarChar match {
        case None => s
        case Some(c) =>
          val lastNonDollarIndex = s.lastIndexOf(c)
          if (lastNonDollarIndex == -1) {
            s
          } else {
            // Strip the trailing dollar signs
            // Invoke stripDollars again to get the simple name
            stripDollars(s.substring(0, lastNonDollarIndex + 1))
          }
      }
    }
  }

  /**
   * Execute a block of code that returns a value, re-throwing any non-fatal uncaught
   * exceptions as IOException. This is used when implementing Externalizable and Serializable's
   * read and write methods, since Java's serializer will not report non-IOExceptions properly;
   * see SPARK-4080 for more context.
   */
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        logError("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        logError("Exception encountered", e)
        throw new IOException(e)
    }
  }

  // scalastyle:off classforname
  /**
   * Preferred alternative to Class.forName(className), as well as
   * Class.forName(className, initialize, loader) with current thread's ContextClassLoader.
   */
  def classForName[C](
    className: String,
    initialize: Boolean = true): Class[C] = {
    Class.forName(className, initialize, Thread.currentThread().getContextClassLoader).
      asInstanceOf[Class[C]]
    // scalastyle:on classforname
  }

  /**
   * Indicates whether Spark is currently running unit tests.
   */
  def isTesting: Boolean = {
    // Scala's `sys.env` creates a ton of garbage by constructing Scala immutable maps, so
    // we directly use the Java APIs instead.
    System.getenv("SPARK_TESTING") != null || System.getProperty("spark.testing") != null
  }

}
