package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.AnalysisException
import com.lk.setl.sql.catalyst.expressions.TimeWindowAssigner.parseExpression
import com.lk.setl.sql.types._

abstract class TimeWindowAssigner extends LeafExpression with Unevaluable {
  override def dataType: DataType = LongType

  def windowSchema: StructType = new StructType() .add(StructField("window_start", LongType)) .add(StructField("window_end", LongType))

  override def nullable: Boolean = false

}

object TimeWindowAssigner{
  def parseExpression(expr: Expression): Long = expr match {
    case IntegerLiteral(i) => i.toLong
    case NonNullLiteral(l, LongType) => l.toString.toLong
    case _ => throw new AnalysisException("The duration and time inputs to window must be " +
      "an integer, long or string literal.")
  }
}

case class ProcessTimeWindow(
  size: Long,
  slide: Long,
  offset: Long
) extends TimeWindowAssigner {

  def this(size: Expression, slide: Expression, offset: Expression) = {
    this(parseExpression(size), parseExpression(slide), parseExpression(offset))
  }

  def this(size: Expression, slide: Expression) = {
    this(parseExpression(size), parseExpression(slide), 0)
  }

  def this(size: Expression) = {
    this(size, size)
  }

}

case class EventTimeWindow(
  size: Long,
  slide: Long,
  offset: Long
) extends TimeWindowAssigner {

  def this(size: Expression, slide: Expression, offset: Expression) = {
    this(parseExpression(size), parseExpression(slide), parseExpression(offset))
  }

  def this(size: Expression, slide: Expression) = {
    this(parseExpression(size), parseExpression(slide), 0)
  }

  def this(size: Expression) = {
    this(size, size)
  }
}