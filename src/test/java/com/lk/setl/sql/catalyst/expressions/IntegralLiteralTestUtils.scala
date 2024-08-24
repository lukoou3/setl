package com.lk.setl.sql.catalyst.expressions

/**
 * Utilities to make sure we pass the proper numeric ranges
 */
object IntegralLiteralTestUtils {

  val positiveShort: Short = (Byte.MaxValue + 1).toShort
  val negativeShort: Short = (Byte.MinValue - 1).toShort

  val positiveShortLit: Literal = Literal(positiveShort)
  val negativeShortLit: Literal = Literal(negativeShort)

  val positiveInt: Int = Short.MaxValue + 1
  val negativeInt: Int = Short.MinValue - 1

  val positiveIntLit: Literal = Literal(positiveInt)
  val negativeIntLit: Literal = Literal(negativeInt)

  val positiveLong: Long = Int.MaxValue + 1L
  val negativeLong: Long = Int.MinValue - 1L

  val positiveLongLit: Literal = Literal(positiveLong)
  val negativeLongLit: Literal = Literal(negativeLong)
}
