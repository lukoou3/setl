package com.lk.setl.sql.catalyst.parser

import com.lk.setl.sql.{GenericRow, Row}
import com.lk.setl.sql.catalyst.expressions._
import com.lk.setl.sql.catalyst.analysis.UnresolvedAttribute
import com.lk.setl.sql.catalyst.expressions.codegen.{GeneratePredicate, GenerateSafeProjection}
import com.lk.setl.sql.types.{IntegerType, LongType}
import org.scalatest.funsuite.AnyFunSuite

class ParserSuite extends AnyFunSuite {

  test("parse") {
    val sql = "select id, name, split(name, '_') names, split(name, '_')[1] name1, age1 + age2 age from t where name like '%aa%'"
    val singleStatement = new CatalystSqlParser().parseQuery(sql)
    println(singleStatement)
    val where = singleStatement.where.get
    println(where)
  }

  test("parseExpression") {
    val map = Map("age1" -> BoundReference(0, IntegerType), "age2" -> BoundReference(1, IntegerType))
    val sql = "age1 + age2 + 3 age"
    var expression = new CatalystSqlParser().parseExpression(sql)
    println(expression)
    expression = expression.transformUp{
      case a: UnresolvedAttribute =>{
        map(a.name)
      }
    }
    println(expression)
    val row = new GenericRow(Array[Any](10, 20))
    val rst: Any = expression.eval(row)
    println("rst:" + rst)
  }

  test("predicateByEval") {
    val expr = And(And(GreaterThan(BoundReference(0, IntegerType),Literal(10, IntegerType)),
      LessThanOrEqual(BoundReference(1, IntegerType),Literal(20, IntegerType))),
      LessThanOrEqual(BoundReference(0, IntegerType),BoundReference(1, IntegerType)))
    val row = new GenericRow(new Array[Any](2))
    row.update(0, 15)
    row.update(1, 20)
    println(expr.eval(row))
    val startMs = System.currentTimeMillis()
    var i = 0
    while (i < 10000000){
      expr.eval(row)
      i+=1;
    }
    val endMs = System.currentTimeMillis()
    println(s"time:${endMs - startMs}")
  }

  test("predicateByCode") {
    val expr = And(And(GreaterThan(BoundReference(0, IntegerType),Literal(10, IntegerType)),
      LessThanOrEqual(BoundReference(1, IntegerType),Literal(20, IntegerType))),
      LessThanOrEqual(BoundReference(0, IntegerType),BoundReference(1, IntegerType)))
    val instance = GeneratePredicate.generate(expr)
    val row = new GenericRow(new Array[Any](2))
    row.update(0, 15)
    row.update(1, 20)
    println(instance.eval(row))
    val startMs = System.currentTimeMillis()
    var i = 0
    while (i < 10000000){
      instance.eval(row)
      i+=1;
    }
    val endMs = System.currentTimeMillis()
    println(s"time:${endMs - startMs}")
  }

  test("generatePredicate") {
    val expr = GreaterThan(
      BoundReference(0, IntegerType),
      BoundReference(1, IntegerType)
    )
    val instance = GeneratePredicate.generate(expr)
    val row = new GenericRow(new Array[Any](2))
    row.update(0, 0)
    row.update(1, 1)
    println(instance.eval(row))
    row.update(0, 1)
    row.update(1, 1)
    println(instance.eval(row))
    row.update(0, 2)
    row.update(1, 1)
    println(instance.eval(row))
  }

  test("generateSafeProjection") {
    val expressions = Seq(
      Add(BoundReference(0, IntegerType), Literal(10, IntegerType)),
      Add(BoundReference(1, IntegerType), Literal(20, IntegerType)),
      Add(BoundReference(0, IntegerType), BoundReference(1, IntegerType)),
    )
    val instance = GenerateSafeProjection.generate(expressions)
    val row = new GenericRow(new Array[Any](2))
    row.update(0, 0)
    row.update(1, 1)
    val rst1 = instance.apply(row)
    println(rst1)
    row.update(0, 10)
    row.update(1, 11)
    val rst2 = instance.apply(row)
    println(rst2)

    val instance2 = GenerateSafeProjection.generate(expressions)
    val rst3 = instance2.apply(row)
    println(rst3)

    val expressions2 = Seq(
      Add(BoundReference(0, IntegerType), Literal(100, IntegerType)),
      Add(BoundReference(1, IntegerType), Literal(200, IntegerType)),
      Add(BoundReference(0, IntegerType), BoundReference(1, IntegerType)),
    )
    val instance3 = GenerateSafeProjection.generate(expressions2)
    val rst4 = instance3.apply(row)
    println(rst4)
  }

}
