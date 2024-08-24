package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.sql.CalendarInterval
import org.scalatest.funsuite.AnyFunSuite
import com.lk.setl.sql.catalyst.util.DateTimeUtils
import com.lk.setl.sql.catalyst.util.TimestampFormatter
import com.lk.setl.sql.catalyst.util.DateTimeTestUtils._
import com.lk.setl.sql.catalyst.util.DateTimeConstants._
import com.lk.setl.sql.types.{CalendarIntervalType, DateType, StringType, TimestampType}

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Calendar, Locale, TimeZone}

class DateExpressionsSuite extends AnyFunSuite with ExpressionEvalHelper {
 import IntegralLiteralTestUtils._

  private val PST_OPT = Option(PST.getId)
  private val JST_OPT = Option(JST.getId)

  def toMillis(timestamp: String): Long = {
    val tf = TimestampFormatter("yyyy-MM-dd HH:mm:ss", UTC, isParsing = true)
    DateTimeUtils.microsToMillis(tf.parse(timestamp))
  }
  val date = "2015-04-08 13:10:15"
  val d = new Date(toMillis(date))
  val time = "2013-11-08 13:10:15"
  val ts = new Timestamp(toMillis(time))

  test("datetime function current_date") {
    val d0 = DateTimeUtils.currentDate(UTC)
    val cd = CurrentDate(UTC_OPT).eval(EmptyRow).asInstanceOf[Int]
    val d1 = DateTimeUtils.currentDate(UTC)
    assert(d0 <= cd && cd <= d1 && d1 - d0 <= 1)

    val cdjst = CurrentDate(JST_OPT).eval(EmptyRow).asInstanceOf[Int]
    val cdpst = CurrentDate(PST_OPT).eval(EmptyRow).asInstanceOf[Int]
    assert(cdpst <= cd && cd <= cdjst)
  }

  test("datetime function current_timestamp") {
    val ct = DateTimeUtils.toJavaTimestamp(CurrentTimestamp().eval(EmptyRow).asInstanceOf[Long])
    val t1 = System.currentTimeMillis()
    assert(math.abs(t1 - ct.getTime) < 5000)
  }

  test("DayOfYear") {
    val sdfDay = new SimpleDateFormat("D", Locale.US)

    val c = Calendar.getInstance()
    (0 to 3).foreach { m =>
      (0 to 5).foreach { i =>
        c.set(2000, m, 28, 0, 0, 0)
        c.add(Calendar.DATE, i)
        checkEvaluation(DayOfYear(Literal(new Date(c.getTimeInMillis))),
          sdfDay.format(c.getTime).toInt)
      }
    }
    checkEvaluation(DayOfYear(Literal.create(null, DateType)), null)

    checkEvaluation(DayOfYear(Cast(Literal("1582-10-15 13:10:15"), DateType)), 288)
    checkEvaluation(DayOfYear(Cast(Literal("1582-10-04 13:10:15"), DateType)), 277)
  }

  test("date add interval") {
    val d = Date.valueOf("2016-02-28")
    checkEvaluation(
      DateAddInterval(Literal(d), Literal(new CalendarInterval(0, 1, 0))),
      DateTimeUtils.fromJavaDate(Date.valueOf("2016-02-29")))
    checkEvaluation(
      DateAddInterval(Literal(d), Literal(new CalendarInterval(1, 1, 0))),
      DateTimeUtils.fromJavaDate(Date.valueOf("2016-03-29")))
    checkEvaluation(DateAddInterval(Literal(d), Literal.create(null, CalendarIntervalType)),
      null)
    checkEvaluation(DateAddInterval(Literal.create(null, DateType),
      Literal(new CalendarInterval(1, 1, 0))),
      null)

    checkExceptionInExpression[IllegalArgumentException](
      DateAddInterval(Literal(d), Literal(new CalendarInterval(1, 1, 25 * MICROS_PER_HOUR)),
        ansiEnabled = true),
      "Cannot add hours, minutes or seconds, milliseconds, microseconds to a date")
  }

  test("time_add") {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.US)
    for (zid <- outstandingZoneIds) {
      val timeZoneId = Option(zid.getId)
      sdf.setTimeZone(TimeZone.getTimeZone(zid))

      checkEvaluation(
        TimeAdd(
          Literal(new Timestamp(sdf.parse("2016-01-29 10:00:00.000").getTime)),
          Literal(new CalendarInterval(1, 2, 123000L)),
          timeZoneId),
        DateTimeUtils.fromJavaTimestamp(
          new Timestamp(sdf.parse("2016-03-02 10:00:00.123").getTime)))

      checkEvaluation(
        TimeAdd(
          Literal.create(null, TimestampType),
          Literal(new CalendarInterval(1, 2, 123000L)),
          timeZoneId),
        null)
      checkEvaluation(
        TimeAdd(
          Literal(new Timestamp(sdf.parse("2016-01-29 10:00:00.000").getTime)),
          Literal.create(null, CalendarIntervalType),
          timeZoneId),
        null)
      checkEvaluation(
        TimeAdd(
          Literal.create(null, TimestampType),
          Literal.create(null, CalendarIntervalType),
          timeZoneId),
        null)
    }
  }

  private def testTruncDate(input: Date, fmt: String, expected: Date): Unit = {
    val expectedDate = Option(expected).map(DateTimeUtils.fromJavaDate(_)).getOrElse(null)
    checkEvaluation(TruncDate(Literal.create(input, DateType), Literal.create(fmt, StringType)),
      expectedDate)
    checkEvaluation(
      TruncDate(Literal.create(input, DateType), NonFoldableLiteral.create(fmt, StringType)),
      expectedDate)
  }

  test("TruncDate") {
    val date = Date.valueOf("2015-07-22")
    Seq("yyyy", "YYYY", "year", "YEAR", "yy", "YY").foreach { fmt =>
      testTruncDate(date, fmt, Date.valueOf("2015-01-01"))
    }
    Seq("month", "MONTH", "mon", "MON", "mm", "MM").foreach { fmt =>
      testTruncDate(date, fmt, Date.valueOf("2015-07-01"))
    }
    testTruncDate(date, "DD", null)
    testTruncDate(date, "SECOND", null)
    testTruncDate(date, "HOUR", null)
    testTruncDate(null, "MON", null)
  }

  private def testTruncTimestamp(input: Timestamp, fmt: String, expected: Timestamp): Unit = {
    val expectedTimestamp = Option(expected).map(DateTimeUtils.fromJavaTimestamp(_)).getOrElse(null)
    checkEvaluation(
      TruncTimestamp(Literal.create(fmt, StringType), Literal.create(input, TimestampType)),
      expectedTimestamp)
    checkEvaluation(
      TruncTimestamp(
        NonFoldableLiteral.create(fmt, StringType), Literal.create(input, TimestampType)),
      expectedTimestamp)
  }

  test("TruncTimestamp") {
    withDefaultTimeZone(UTC) {
      val inputDate = Timestamp.valueOf("2015-07-22 05:30:06")

      Seq("yyyy", "YYYY", "year", "YEAR", "yy", "YY").foreach { fmt =>
        testTruncTimestamp(
          inputDate, fmt,
          Timestamp.valueOf("2015-01-01 00:00:00"))
      }

      Seq("month", "MONTH", "mon", "MON", "mm", "MM").foreach { fmt =>
        testTruncTimestamp(
          inputDate, fmt,
          Timestamp.valueOf("2015-07-01 00:00:00"))
      }

      Seq("DAY", "day", "DD", "dd").foreach { fmt =>
        testTruncTimestamp(
          inputDate, fmt,
          Timestamp.valueOf("2015-07-22 00:00:00"))
      }

      Seq("HOUR", "hour").foreach { fmt =>
        testTruncTimestamp(
          inputDate, fmt,
          Timestamp.valueOf("2015-07-22 05:00:00"))
      }

      Seq("MINUTE", "minute").foreach { fmt =>
        testTruncTimestamp(
          inputDate, fmt,
          Timestamp.valueOf("2015-07-22 05:30:00"))
      }

      Seq("SECOND", "second").foreach { fmt =>
        testTruncTimestamp(
          inputDate, fmt,
          Timestamp.valueOf("2015-07-22 05:30:06"))
      }

      Seq("WEEK", "week").foreach { fmt =>
        testTruncTimestamp(
          inputDate, fmt,
          Timestamp.valueOf("2015-07-20 00:00:00"))
      }

      Seq("QUARTER", "quarter").foreach { fmt =>
        testTruncTimestamp(
          inputDate, fmt,
          Timestamp.valueOf("2015-07-01 00:00:00"))
      }

      testTruncTimestamp(null, "MON", null)
    }
  }
}
