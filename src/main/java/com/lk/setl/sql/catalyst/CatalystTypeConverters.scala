package com.lk.setl.sql.catalyst

import com.lk.setl.sql.types._

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

/**
 * Functions to convert Scala types to Catalyst types and vice versa.
 */
object CatalystTypeConverters {
  // The Predef.Map is scala.collection.immutable.Map.
  // Since the map values can be mutable, we explicitly import scala.collection.Map at here.
  import scala.collection.Map

  private[sql] def isPrimitive(dataType: DataType): Boolean = {
    dataType match {
      case BooleanType => true
      case IntegerType => true
      case LongType => true
      case FloatType => true
      case DoubleType => true
      case _ => false
    }
  }

  /**
   *  Converts Scala objects to Catalyst rows / types.
   *
   *  Note: This should be called before do evaluation on Row
   *        (It does not support UDT)
   *  This is used to create an RDD or test results with correct types for Catalyst.
   */
  def convertToCatalyst(a: Any): Any = a match {
    case arr: Array[Byte] => arr
    case other => other
  }
}
