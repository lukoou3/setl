package com.lk.setl.sql.catalyst.parser

import com.lk.setl.sql.GenericRow
import com.lk.setl.sql.catalyst.expressions._
import com.lk.setl.sql.catalyst.analysis.UnresolvedAttribute
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

}
