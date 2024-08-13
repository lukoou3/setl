package com.lk.setl.sql.catalyst.expressions.aggregate

import com.lk.setl.sql.catalyst.analysis.TypeCheckResult
import com.lk.setl.sql.catalyst.dsl.expressions._
import com.lk.setl.sql.catalyst.expressions._
import com.lk.setl.sql.types._

case class Count(children: Seq[Expression]) extends DeclarativeAggregate {

  override def nullable: Boolean = false

  // Return data type.
  override def dataType: DataType = LongType

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.isEmpty && false /*!SQLConf.get.getConf(SQLConf.ALLOW_PARAMETERLESS_COUNT)*/) {
      TypeCheckResult.TypeCheckFailure(s"$prettyName requires at least one argument. " +
        s"If you have to call the function $prettyName without arguments, set the legacy " +
        s"configuration `ALLOW_PARAMETERLESS_COUNT` as true")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  protected lazy val count = AttributeReference("count", LongType, nullable = false)()

  override lazy val aggBufferAttributes = count :: Nil

  override lazy val initialValues = Seq(
    /* count = */ Literal(0L)
  )

  override lazy val mergeExpressions = Seq(
    /* count = */ count.left + count.right
  )

  override lazy val evaluateExpression = count

  override def defaultResult: Option[Literal] = Option(Literal(0L))

  override lazy val updateExpressions = {
    val nullableChildren = children.filter(_.nullable)
    if (nullableChildren.isEmpty) {
      Seq(
        /* count = */ count + 1L
      )
    } else {
      Seq(
        /* count = */ If(nullableChildren.map(IsNull).reduce(Or), count, count + 1L)
      )
    }
  }
}

object Count {
  def apply(child: Expression): Count = Count(child :: Nil)
}
