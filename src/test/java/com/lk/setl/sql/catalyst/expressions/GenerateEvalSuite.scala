package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.{ArrayData, GenericArrayData, GenericRow, JoinedRow, Row, SqlUtils}
import com.lk.setl.sql.catalyst.dsl.expressions._
import com.lk.setl.sql.catalyst.QueryPlanningTracker
import com.lk.setl.sql.catalyst.analysis.UnresolvedAttribute
import com.lk.setl.sql.catalyst.execution.QueryExecution
import com.lk.setl.sql.catalyst.parser.CatalystSqlParser
import com.lk.setl.sql.catalyst.plans.logical._
import com.lk.setl.sql.types.IntegerType
import org.scalatest.funsuite.AnyFunSuite

class GenerateEvalSuite  extends AnyFunSuite{

  test("generate") {
    val schema = SqlUtils.parseStructType("id: bigint, name: string, age: int, datas: array<int>")
    val tempViews = Map("tab" -> RelationPlaceholder(schema.toAttributes))
    val sql = "select id, name, data from tab lateral view explode(datas) t as data where id > 10"
    val singleStatement = new CatalystSqlParser().parsePlan(sql)
    println(singleStatement)
    val tracker = new QueryPlanningTracker
    val logicalPlan = new QueryExecution(tempViews, singleStatement, tracker)
    logicalPlan.assertAnalyzed()
    val analyzed = logicalPlan.analyzed
    val optimized = logicalPlan.optimizedPlan
    println(analyzed)
    println(optimized)
  }

  test("generateNoChildOutput") {
    val (g, p, rows) = getGenerateAndRows
    val generator = g.generator
    val requiredChildOutput = g.requiredChildOutput
    val outer = true // false, true
    val generatorOutput = g.qualifiedGeneratorOutput
    val child = g.child

    val generatorNullRow = new GenericRow(new Array(generator.elementSchema.length))
    val boundGenerator: Generator = BindReferences.bindReference(generator, child.output)

    rows.flatMap { row =>
      val outputRows = boundGenerator.eval(row)
      if (outer && outputRows.isEmpty) {
        Seq(generatorNullRow)
      } else {
        outputRows
      }
    }.foreach{r =>
      println(r)
    }

  }

  test("generateWithChildOutput") {
    val (g, p, rows) = getGenerateAndRows
    val iter = rows.toIterator
    val generator = g.generator
    val requiredChildOutput = g.requiredChildOutput
    val outer = true // false, true
    val generatorOutput = g.qualifiedGeneratorOutput
    val child = g.child

    val generatorNullRow = new GenericRow(new Array(generator.elementSchema.length))
    val boundGenerator: Generator = BindReferences.bindReference(generator, child.output)

    val pruneChildForResult: Projection =
      if (child.outputSet == AttributeSet(requiredChildOutput)) {
        (r: Row) => r
      } else {
        SafeProjection.create(requiredChildOutput, child.output)
      }

    val joinedRow = new JoinedRow
    val rsts:Iterator[Row] = iter.flatMap { row =>
      // we should always set the left (required child output)
      joinedRow.withLeft(pruneChildForResult(row))
      val outputRows = boundGenerator.eval(row) //Explode等Generator不执行代码生成
      if (outer && outputRows.isEmpty) {
        joinedRow.withRight(generatorNullRow) :: Nil
      } else {
        outputRows.toIterator.map(joinedRow.withRight)
      }
    }
    println(rsts.getClass)

    rsts.foreach { r =>
      println(r)
    }
  }

  test("generateWithChildOutputAndProjection") {
    val (g,p, rows) = getGenerateAndRows
    val iter = rows.toIterator
    val generator = g.generator
    val requiredChildOutput = g.requiredChildOutput
    val outer = true // false, true
    val generatorOutput = g.qualifiedGeneratorOutput
    val child = g.child

    val generatorNullRow = new GenericRow(new Array(generator.elementSchema.length))
    val boundGenerator: Generator = BindReferences.bindReference(generator, child.output)

    val pruneChildForResult: Projection =
      if (child.outputSet == AttributeSet(requiredChildOutput)) {
        (r: Row) => r
      } else {
        SafeProjection.create(requiredChildOutput, child.output)
      }

    val joinedRow = new JoinedRow
    val rsts: Iterator[Row] = iter.flatMap { row =>
      // we should always set the left (required child output)
      joinedRow.withLeft(pruneChildForResult(row))
      val outputRows = boundGenerator.eval(row) //Explode等Generator不执行代码生成
      if (outer && outputRows.isEmpty) {
        joinedRow.withRight(generatorNullRow) :: Nil
      } else {
        outputRows.toIterator.map(joinedRow.withRight)
      }
    }
    println(rsts.getClass)

    val out = requiredChildOutput ++ generatorOutput
    println(out)
    println(g.output)
    println(p.child.output)

    val project = SafeProjection.create(p.projectList, p.child.output)
    rsts.foreach { r =>
      val row = project(r)
      println(row)
    }
  }

  def getGenerateAndRows = {
    val schema = SqlUtils.parseStructType("id: bigint, name: string, age: int, datas: array<int>")
    val tempViews = Map("tab" -> RelationPlaceholder(schema.toAttributes))
    // val filter = Filter(GreaterThan(UnresolvedAttribute(Seq("id")),  Literal(10)), RelationPlaceholder(schema.toAttributes))
    val filter = RelationPlaceholder(schema.toAttributes)
    val plan = Project(Seq((UnresolvedAttribute(Seq("id")) + 10).as("id2"), UnresolvedAttribute(Seq("name")).substr(1, 2).as("name2"), (UnresolvedAttribute(Seq("data")) + 100).as("data")),
      Generate(Explode(UnresolvedAttribute(Seq("datas"))), Seq(3), false, None, Seq(UnresolvedAttribute(Seq("data"))), filter))
    println(plan)
    val tracker = new QueryPlanningTracker
    val logicalPlan = new QueryExecution(tempViews, plan, tracker)
    logicalPlan.assertAnalyzed()
    val analyzed = logicalPlan.analyzed
    val p = logicalPlan.optimizedPlan.asInstanceOf[Project]
    val g = p.child.asInstanceOf[Generate]
    println(analyzed)
    println(p)
    val rows = Seq[Row](
      new GenericRow(Array(1L, "aaa", 18, new GenericArrayData(Array(1, 2, 3)))),
      new GenericRow(Array(2L, "bbb", 18, new GenericArrayData(Array()))),
      new GenericRow(Array(3L, "ccc", 20, new GenericArrayData(Array(4, 5, 6))))
    )
    (g, p, rows)
  }
}
