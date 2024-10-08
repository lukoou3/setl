package com.lk.setl.sql.catalyst.util

import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId}
import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

import com.lk.setl.sql.catalyst.util.DateTimeUtils._

sealed trait DateFormatter extends Serializable {
  def parse(s: String): Int // returns days since epoch

  def format(days: Int): String
  def format(date: Date): String
  def format(localDate: LocalDate): String

  def validatePatternString(): Unit
}

class Iso8601DateFormatter(
    pattern: String,
    zoneId: ZoneId,
    locale: Locale,
    legacyFormat: LegacyDateFormats.LegacyDateFormat,
    isParsing: Boolean)
  extends DateFormatter with DateTimeFormatterHelper {

  @transient
  private lazy val formatter = getOrCreateFormatter(pattern, locale, isParsing)

  @transient
  private lazy val legacyFormatter = DateFormatter.getLegacyFormatter(
    pattern, zoneId, locale, legacyFormat)

  override def parse(s: String): Int = {
    val specialDate = convertSpecialDate(s.trim, zoneId)
    specialDate.getOrElse {
      try {
        val localDate = toLocalDate(formatter.parse(s))
        localDateToDays(localDate)
      } catch checkParsedDiff(s, legacyFormatter.parse)
    }
  }

  override def format(localDate: LocalDate): String = {
    try {
      localDate.format(formatter)
    } catch checkFormattedDiff(toJavaDate(localDateToDays(localDate)),
      (d: Date) => format(d))
  }

  override def format(days: Int): String = {
    format(LocalDate.ofEpochDay(days))
  }

  override def format(date: Date): String = {
    legacyFormatter.format(date)
  }

  override def validatePatternString(): Unit = {
    try {
      formatter
    } catch checkLegacyFormatter(pattern, legacyFormatter.validatePatternString)
  }
}

trait LegacyDateFormatter extends DateFormatter {
  def parseToDate(s: String): Date

  override def parse(s: String): Int = {
    fromJavaDate(new java.sql.Date(parseToDate(s).getTime))
  }

  override def format(days: Int): String = {
    format(DateTimeUtils.toJavaDate(days))
  }

  override def format(localDate: LocalDate): String = {
    format(localDateToDays(localDate))
  }
}

/**
 * The legacy formatter is based on Apache Commons FastDateFormat. The formatter uses the default
 * JVM time zone intentionally for compatibility with Spark 2.4 and earlier versions.
 *
 * Note: Using of the default JVM time zone makes the formatter compatible with the legacy
 *       `DateTimeUtils` methods `toJavaDate` and `fromJavaDate` that are based on the default
 *       JVM time zone too.
 *
 * @param pattern `java.text.SimpleDateFormat` compatible pattern.
 * @param locale The locale overrides the system locale and is used in parsing/formatting.
 */
class LegacyFastDateFormatter(pattern: String, locale: Locale) extends LegacyDateFormatter {
  @transient
  private lazy val fdf = FastDateFormat.getInstance(pattern, locale)
  override def parseToDate(s: String): Date = fdf.parse(s)
  override def format(d: Date): String = fdf.format(d)
  override def validatePatternString(): Unit = fdf
}

// scalastyle:off line.size.limit
/**
 * The legacy formatter is based on `java.text.SimpleDateFormat`. The formatter uses the default
 * JVM time zone intentionally for compatibility with Spark 2.4 and earlier versions.
 *
 * Note: Using of the default JVM time zone makes the formatter compatible with the legacy
 *       `DateTimeUtils` methods `toJavaDate` and `fromJavaDate` that are based on the default
 *       JVM time zone too.
 *
 * @param pattern The pattern describing the date and time format.
 *                See <a href="https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html">
 *                Date and Time Patterns</a>
 * @param locale  The locale whose date format symbols should be used. It overrides the system
 *                locale in parsing/formatting.
 */
// scalastyle:on line.size.limit
class LegacySimpleDateFormatter(pattern: String, locale: Locale) extends LegacyDateFormatter {
  @transient
  private lazy val sdf = new SimpleDateFormat(pattern, locale)
  override def parseToDate(s: String): Date = sdf.parse(s)
  override def format(d: Date): String = sdf.format(d)
  override def validatePatternString(): Unit = sdf

}

object DateFormatter {
  import LegacyDateFormats._

  val defaultLocale: Locale = Locale.US

  val defaultPattern: String = "yyyy-MM-dd"

  private def getFormatter(
      format: Option[String],
      zoneId: ZoneId,
      locale: Locale = defaultLocale,
      legacyFormat: LegacyDateFormat = LENIENT_SIMPLE_DATE_FORMAT,
      isParsing: Boolean): DateFormatter = {
    val pattern = format.getOrElse(defaultPattern)
    if (false) {
      getLegacyFormatter(pattern, zoneId, locale, legacyFormat)
    } else {
      val df = new Iso8601DateFormatter(pattern, zoneId, locale, legacyFormat, isParsing)
      df.validatePatternString()
      df
    }
  }

  def getLegacyFormatter(
      pattern: String,
      zoneId: ZoneId,
      locale: Locale,
      legacyFormat: LegacyDateFormat): DateFormatter = {
    legacyFormat match {
      case FAST_DATE_FORMAT =>
        new LegacyFastDateFormatter(pattern, locale)
      case SIMPLE_DATE_FORMAT | LENIENT_SIMPLE_DATE_FORMAT =>
        new LegacySimpleDateFormatter(pattern, locale)
    }
  }

  def apply(
      format: String,
      zoneId: ZoneId,
      locale: Locale,
      legacyFormat: LegacyDateFormat,
      isParsing: Boolean): DateFormatter = {
    getFormatter(Some(format), zoneId, locale, legacyFormat, isParsing)
  }

  def apply(format: String, zoneId: ZoneId, isParsing: Boolean = false): DateFormatter = {
    getFormatter(Some(format), zoneId, isParsing = isParsing)
  }

  def apply(zoneId: ZoneId): DateFormatter = {
    getFormatter(None, zoneId, isParsing = false)
  }
}
