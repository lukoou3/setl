package com.lk.setl.sql.catalyst.parser

import com.lk.setl.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class DataTypeParserSuite extends AnyFunSuite  {
  val sqlParser =  new CatalystSqlParser()

  def parse(sql: String): DataType = sqlParser.parseDataType(sql)

  def checkDataType(dataTypeString: String, expectedDataType: DataType): Unit = {
    test(s"parse ${dataTypeString.replace("\n", "")}") {
      val parsedType = parse(dataTypeString)
      println(parsedType)
      assert(parsedType === expectedDataType)
    }
  }

  def intercept(sql: String): ParseException =
    intercept[ParseException](sqlParser.parseDataType(sql))

  def unsupported(dataTypeString: String): Unit = {
    test(s"$dataTypeString is not supported") {
      intercept(dataTypeString)
    }
  }

  checkDataType("int", IntegerType)
  checkDataType("integer", IntegerType)
  checkDataType("BooLean", BooleanType)
  checkDataType("INT", IntegerType)
  checkDataType("INTEGER", IntegerType)
  checkDataType("bigint", LongType)
  checkDataType("float", FloatType)
  checkDataType("dOUBle", DoubleType)
  checkDataType("string", StringType)
  checkDataType("BINARY", BinaryType)
  checkDataType("void", NullType)

  checkDataType("array<doublE>", ArrayType(DoubleType, true))

  checkDataType(
    "struct<intType: int, longType:bigint>",
    StructType(
      StructField("intType", IntegerType) ::
      StructField("longType", LongType) :: Nil)
  )
  // It is fine to use the data type string as the column name.
  checkDataType(
    "Struct<int: int, bigint:bigint>",
    StructType(
      StructField("int", IntegerType) ::
      StructField("bigint", LongType) :: Nil)
  )
  checkDataType(
    """
      |struct<
      |  struct:struct<intType: int, longType:bigint>,
      |  arrAy:Array<double>,
      |  anotherArray:Array<string>>
    """.stripMargin,
    StructType(
      StructField("struct", StructType(StructField("intType", IntegerType) ::StructField("longType", LongType) :: Nil)) ::
      StructField("arrAy", ArrayType(DoubleType, true)) ::
      StructField("anotherArray", ArrayType(StringType, true)) :: Nil)
  )
  // Use backticks to quote column names having special characters.
  checkDataType(
    "struct<`x+y`:int, `!@#$%^&*()`:string, `1_2.345<>:\"`:int>",
    StructType(
      StructField("x+y", IntegerType) ::
      StructField("!@#$%^&*()", StringType) ::
      StructField("1_2.345<>:\"", IntegerType) :: Nil)
  )
  // Empty struct.
  checkDataType("strUCt<>", StructType(Nil))

  unsupported("it is not a data type")
  unsupported("struct<x+y: int, 1.1:timestamp>")
  unsupported("struct<x: int")
  unsupported("struct<x int, y string>")

  test("Do not print empty parentheses for no params") {
    assert(intercept("unkwon").getMessage.contains("unkwon is not supported"))
    assert(intercept("unkwon(1,2,3)").getMessage.contains("unkwon(1,2,3) is not supported"))
  }

  // DataType parser accepts certain reserved keywords.
  checkDataType(
    "Struct<TABLE: string, DATE:boolean>",
    StructType(
      StructField("TABLE", StringType) ::
        StructField("DATE", BooleanType) :: Nil)
  )

  // Use SQL keywords.
  checkDataType("struct<end: long, select: int, from: string>",
    (new StructType).add("end", LongType).add("select", IntegerType).add("from", StringType))

  // DataType parser accepts comments.
  checkDataType("Struct<x: INT, y: STRING COMMENT 'test'>",
    (new StructType).add("x", IntegerType).add("y", StringType))
}
