package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.catalyst.dsl.expressions._
import com.lk.setl.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Complete, Count, Sum}
import com.lk.setl.sql.types.{IntegerType, LongType}
import org.scalatest.funsuite.AnyFunSuite

class AggregationEvalSuite extends AnyFunSuite {
  test("a") {
    val groupingExpressions: Seq[Expression] = Seq(BoundReference(0, IntegerType).as("id"))
    val aggregateExpressions: Seq[NamedExpression] = Seq(
      AggregateExpression(Sum(BoundReference(1, LongType)), Complete, false).as("bytes"),
      AggregateExpression(Sum(BoundReference(2, LongType)), Complete, false).as("sessions"),
      AggregateExpression(Average(BoundReference(2, LongType)), Complete, false).as("avg_sessions"),
      AggregateExpression(Count(Literal(1)), Complete, false).as("cnt"),
    )
    val resultExpressions: Seq[NamedExpression] = aggregateExpressions.map{ expr =>
      expr.transformDown {
        case ae: AggregateExpression =>
          ae.resultAttribute
      }.asInstanceOf[NamedExpression]
    }



  }
}
