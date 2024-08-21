package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.{GenericRow, JoinedRow, Row}
import com.lk.setl.sql.catalyst.dsl.expressions._
import com.lk.setl.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Average, Complete, Count, DeclarativeAggregate, Final, NoOp, Partial, PartialMerge, Sum}
import com.lk.setl.sql.types.{IntegerType, LongType}
import org.scalatest.funsuite.AnyFunSuite

class AggregationEvalSuite extends AnyFunSuite {
  System.setProperty("spark.testing", "true")

  test("agg") {
    val inputs = Seq(AttributeReference("id", IntegerType)(), AttributeReference("bytes", LongType)(), AttributeReference("sessions", LongType)())
    def get = {
      val groupingExpressions: Seq[NamedExpression] = Seq(inputs(0))
      val aggResultExpressions: Seq[NamedExpression] = Seq(
        (inputs(0) + 1).as("id"),
        (AggregateExpression(Sum(inputs(1)), Complete, false) + 100).as("bytes"),
        AggregateExpression(Sum(inputs(2)), Complete, false).as("sessions"),
        AggregateExpression(Average(inputs(2)), Complete, false).as("avg_sessions"),
        AggregateExpression(Count(Literal(1)), Complete, false).as("cnt"),
      )
      val aggregateExpressions = aggResultExpressions.flatMap { expr =>
        expr.collect {
          case agg: AggregateExpression => agg
        }
      }

      val resultExpressions: Seq[NamedExpression] = aggResultExpressions.map{ expr =>
        expr.transformDown {
          case ae: AggregateExpression =>
            ae.resultAttribute
        }.asInstanceOf[NamedExpression]
      }

      println("aggregateExpressions:" + aggregateExpressions)
      println("resultExpressions:" + resultExpressions)
      val groupingAttributes = groupingExpressions.map(_.toAttribute)
      val partialAggregateExpressions = aggregateExpressions.map(_.copy(mode = Partial)) // Partial
      val partialAggregateAttributes = partialAggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
      val partialResultExpressions = groupingAttributes ++ partialAggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)
      // 不需要map段聚合
      //(groupingExpressions, partialAggregateExpressions, partialAggregateAttributes, partialResultExpressions)
      (groupingExpressions, partialAggregateExpressions, partialAggregateAttributes, resultExpressions)
    }
    //MutableProjection.fallbackMode = CodegenObjectFactoryMode.NO_CODEGEN
    val a = get
    val groupingExpressions: Seq[NamedExpression] = a._1
    val aggregateExpressions: Seq[AggregateExpression] = a._2
    val aggregateAttributes: Seq[Attribute] = a._3
    val resultExpressions: Seq[NamedExpression] = a._4

    val inputAttributes: Seq[Attribute] = inputs
    val aggregateFunctions: Array[AggregateFunction] = aggregateExpressions.map(_.aggregateFunction).toArray
    val processRow: (Row, Row) => Unit = generateProcessRow(aggregateExpressions, aggregateFunctions, inputAttributes)

    val buffer = new GenericRow(new Array[Any](aggregateFunctions.flatMap(_.aggBufferAttributes).length))
    buffer.update(0, null)
    buffer.update(1, null)
    buffer.update(2, 0D)
    buffer.update(3, 0L)
    buffer.update(4, 0L)
    val rows = Seq[Row](
      new GenericRow(Array(1, 10L, 1L)),
      new GenericRow(Array(1, 20L, 2L)),
      new GenericRow(Array(1, 30L, 2L)),
      new GenericRow(Array(1, 5L, 3L))
    )

    rows.foreach{ newInput =>
      processRow(buffer, newInput)
      println(buffer)
    }

    val evalExpressions = aggregateFunctions.map {
      case ae: DeclarativeAggregate => ae.evaluateExpression
      case agg: AggregateFunction => NoOp // 占位符，不生成代码
    }

    val bufferAttributes = aggregateFunctions.flatMap(_.aggBufferAttributes)
    val aggregateResult = new GenericRow(new Array[Any](evalExpressions.length))
    val expressionAggEvalProjection =  MutableProjection.create(evalExpressions, bufferAttributes)
    expressionAggEvalProjection.target(aggregateResult) // expressionAggEvalProjection更新aggregateResult
    expressionAggEvalProjection(buffer)

    println(aggregateResult)
    val groupingAttributes = groupingExpressions.map(_.toAttribute)
    val finalAggregateAttributes = aggregateExpressions.map(_.resultAttribute)
    val resultProjection = SafeProjection.create(resultExpressions, groupingAttributes ++ finalAggregateAttributes)
    // 就是把key和聚合值合在一起输出
    val joinedRow = new JoinedRow
    val currentGroupingKey = new GenericRow(Array[Any](11))
    val rst = resultProjection(joinedRow(currentGroupingKey, aggregateResult))
    println(rst)
  }

  // Initializing functions used to process a row.
  protected def generateProcessRow(
      expressions: Seq[AggregateExpression],
      functions: Seq[AggregateFunction],
      inputAttributes: Seq[Attribute]): (Row, Row) => Unit = {
    val joinedRow = new JoinedRow
    if (expressions.nonEmpty) {
      // mergeExpressions聚合函数更新，这里使用的是flatMap，这样Average等就能够和其它sum等函数展平在一个row里处理了
      val mergeExpressions =
        functions.zip(expressions.map(ae => (ae.mode, ae.isDistinct, ae.filter))).flatMap {
          case (ae: DeclarativeAggregate, (mode, isDistinct, filter)) =>
            mode match {
              case Partial | Complete =>  ae.updateExpressions
              case PartialMerge | Final => ae.mergeExpressions
            }
          // case (agg: AggregateFunction, _) => Seq.fill(agg.aggBufferAttributes.length)(NoOp) // ImperativeAggregate聚合函数站位
        }
      // 更新ImperativeAggregate聚合函数，暂时不需要
      // This projection is used to merge buffer values for all expression-based aggregates.
      val aggregationBufferSchema = functions.flatMap(_.aggBufferAttributes)
      // 更新expression-based aggregates的逻辑
      // 用于创建mutable projections，就是初始化buffer。
      val updateProjection = MutableProjection.create(mergeExpressions, aggregationBufferSchema ++ inputAttributes)

      (currentBuffer: Row, row: Row) => {
        // map阶段：就是把flatMap后的AggregateFunction处理结果update到mutableRow
        // reduce阶段：也是是把flatMap后的AggregateFunction处理结果update到mutableRow，就是输入Schema不同
        // Process all expression-based aggregate functions.
        updateProjection.target(currentBuffer)(joinedRow(currentBuffer, row))
        // Process all imperative aggregate functions.
      }
    } else {
      // Grouping only.
      (currentBuffer: Row, row: Row) => {}
    }
  }
}
