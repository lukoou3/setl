package com.lk.setl.sql.catalyst.util

object SQLOrderingUtil {

  /**
   * A special version of double comparison that follows SQL semantic:
   *  1. NaN == NaN
   *  2. NaN is greater than any non-NaN double
   *  3. -0.0 == 0.0
   */
  def compareDoubles(x: Double, y: Double): Int = {
    if (x == y) 0 else java.lang.Double.compare(x, y)
  }

  /**
   * A special version of float comparison that follows SQL semantic:
   *  1. NaN == NaN
   *  2. NaN is greater than any non-NaN float
   *  3. -0.0 == 0.0
   */
  def compareFloats(x: Float, y: Float): Int = {
    if (x == y) 0 else java.lang.Float.compare(x, y)
  }
}
