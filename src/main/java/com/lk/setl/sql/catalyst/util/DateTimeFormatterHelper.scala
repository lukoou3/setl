package com.lk.setl.sql.catalyst.util

import java.time._
import java.time.chrono.IsoChronology
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, ResolverStyle}
import java.time.temporal.{ChronoField, TemporalAccessor, TemporalQueries}
import java.util.{Date, Locale}

import com.google.common.cache.CacheBuilder

import com.lk.setl.sql.catalyst.util.DateTimeFormatterHelper._

trait DateTimeFormatterHelper {
  private def getOrDefault(accessor: TemporalAccessor, field: ChronoField, default: Int): Int = {
    if (accessor.isSupported(field)) {
      accessor.get(field)
    } else {
      default
    }
  }

  private def verifyLocalDate(
      accessor: TemporalAccessor, field: ChronoField, candidate: LocalDate): Unit = {
    if (accessor.isSupported(field)) {
      val actual = accessor.get(field)
      val expected = candidate.get(field)
      if (actual != expected) {
        throw new DateTimeException(s"Conflict found: Field $field $actual differs from" +
          s" $field $expected derived from $candidate")
      }
    }
  }

  protected def toLocalDate(accessor: TemporalAccessor): LocalDate = {
    val localDate = accessor.query(TemporalQueries.localDate())
    // If all the date fields are specified, return the local date directly.
    if (localDate != null) return localDate

    // Users may want to parse only a few datetime fields from a string and extract these fields
    // later, and we should provide default values for missing fields.
    // To be compatible with Spark 2.4, we pick 1970 as the default value of year.
    val year = getOrDefault(accessor, ChronoField.YEAR, 1970)
    if (accessor.isSupported(ChronoField.DAY_OF_YEAR)) {
      val dayOfYear = accessor.get(ChronoField.DAY_OF_YEAR)
      val date = LocalDate.ofYearDay(year, dayOfYear)
      verifyLocalDate(accessor, ChronoField.MONTH_OF_YEAR, date)
      verifyLocalDate(accessor, ChronoField.DAY_OF_MONTH, date)
      date
    } else {
      val month = getOrDefault(accessor, ChronoField.MONTH_OF_YEAR, 1)
      val day = getOrDefault(accessor, ChronoField.DAY_OF_MONTH, 1)
      LocalDate.of(year, month, day)
    }
  }

  private def toLocalTime(accessor: TemporalAccessor): LocalTime = {
    val localTime = accessor.query(TemporalQueries.localTime())
    // If all the time fields are specified, return the local time directly.
    if (localTime != null) return localTime

    val hour = if (accessor.isSupported(ChronoField.HOUR_OF_DAY)) {
      accessor.get(ChronoField.HOUR_OF_DAY)
    } else if (accessor.isSupported(ChronoField.HOUR_OF_AMPM)) {
      // When we reach here, it means am/pm is not specified. Here we assume it's am.
      // All of CLOCK_HOUR_OF_AMPM(h)/HOUR_OF_DAY(H)/CLOCK_HOUR_OF_DAY(k)/HOUR_OF_AMPM(K) will
      // be resolved to HOUR_OF_AMPM here, we do not need to handle them separately
      accessor.get(ChronoField.HOUR_OF_AMPM)
    } else if (accessor.isSupported(ChronoField.AMPM_OF_DAY) &&
      accessor.get(ChronoField.AMPM_OF_DAY) == 1) {
      // When reach here, the `hour` part is missing, and PM is specified.
      // None of CLOCK_HOUR_OF_AMPM(h)/HOUR_OF_DAY(H)/CLOCK_HOUR_OF_DAY(k)/HOUR_OF_AMPM(K) is
      // specified
      12
    } else {
      0
    }
    val minute = getOrDefault(accessor, ChronoField.MINUTE_OF_HOUR, 0)
    val second = getOrDefault(accessor, ChronoField.SECOND_OF_MINUTE, 0)
    val nanoSecond = getOrDefault(accessor, ChronoField.NANO_OF_SECOND, 0)
    LocalTime.of(hour, minute, second, nanoSecond)
  }

  // Converts the parsed temporal object to ZonedDateTime. It sets time components to zeros
  // if they does not exist in the parsed object.
  protected def toZonedDateTime(accessor: TemporalAccessor, zoneId: ZoneId): ZonedDateTime = {
    val localDate = toLocalDate(accessor)
    val localTime = toLocalTime(accessor)
    ZonedDateTime.of(localDate, localTime, zoneId)
  }

  // Gets a formatter from the cache or creates new one. The buildFormatter method can be called
  // a few times with the same parameters in parallel if the cache does not contain values
  // associated to those parameters. Since the formatter is immutable, it does not matter.
  // In this way, synchronised is intentionally omitted in this method to make parallel calls
  // less synchronised.
  // The Cache.get method is not used here to avoid creation of additional instances of Callable.
  protected def getOrCreateFormatter(
      pattern: String,
      locale: Locale,
      isParsing: Boolean): DateTimeFormatter = {
    val newPattern = convertIncompatiblePattern(pattern, isParsing)
    val useVarLen = isParsing && newPattern.contains('S')
    val key = (newPattern, locale, useVarLen)
    var formatter = cache.getIfPresent(key)
    if (formatter == null) {
      formatter = buildFormatter(newPattern, locale, useVarLen)
      cache.put(key, formatter)
    }
    formatter
  }

  private def needConvertToSparkUpgradeException(e: Throwable): Boolean = e match {
    // case _: DateTimeException if true => true
    case _ => false
  }
  // When legacy time parser policy set to EXCEPTION, check whether we will get different results
  // between legacy parser and new parser. If new parser fails but legacy parser works, throw a
  // SparkUpgradeException. On the contrary, if the legacy policy set to CORRECTED,
  // DateTimeParseException will address by the caller side.
  protected def checkParsedDiff[T](
      s: String, legacyParseFunc: String => T): PartialFunction[Throwable, T] = {
    case e if needConvertToSparkUpgradeException(e) =>
      try {
        legacyParseFunc(s)
      } catch {
        case _: Throwable => throw e
      }
      throw new RuntimeException(s"Fail to parse '$s' in the new parser. You can " +
        s"set LEGACY_TIME_PARSER_POLICY to LEGACY to restore the behavior " +
        s"before Spark 3.0, or set to CORRECTED and treat it as an invalid datetime string. ", e)
  }

  // When legacy time parser policy set to EXCEPTION, check whether we will get different results
  // between legacy formatter and new formatter. If new formatter fails but legacy formatter works,
  // throw a SparkUpgradeException. On the contrary, if the legacy policy set to CORRECTED,
  // DateTimeParseException will address by the caller side.
  protected def checkFormattedDiff[T <: Date](
      d: T,
      legacyFormatFunc: T => String): PartialFunction[Throwable, String] = {
    case e if needConvertToSparkUpgradeException(e) =>
      val resultCandidate = try {
        legacyFormatFunc(d)
      } catch {
        case _: Throwable => throw e
      }
      throw new RuntimeException(s"Fail to format it to '$resultCandidate' in the new" +
        s" formatter. You can set LEGACY_TIME_PARSER_POLICY to LEGACY to restore" +
        " the behavior before Spark 3.0, or set to CORRECTED and treat it as an invalid" +
        " datetime string.", e)
  }

  /**
   * When the new DateTimeFormatter failed to initialize because of invalid datetime pattern, it
   * will throw IllegalArgumentException. If the pattern can be recognized by the legacy formatter
   * it will raise SparkUpgradeException to tell users to restore the previous behavior via LEGACY
   * policy or follow our guide to correct their pattern. Otherwise, the original
   * IllegalArgumentException will be thrown.
   *
   * @param pattern the date time pattern
   * @param tryLegacyFormatter a func to capture exception, identically which forces a legacy
   *                           datetime formatter to be initialized
   */
  protected def checkLegacyFormatter(
      pattern: String,
      tryLegacyFormatter: => Unit): PartialFunction[Throwable, DateTimeFormatter] = {
    case e: IllegalArgumentException =>
      try {
        tryLegacyFormatter
      } catch {
        case _: Throwable => throw e
      }
      throw new RuntimeException(s"Fail to recognize '$pattern' pattern in the" +
        s" DateTimeFormatter. 1) You can set LEGACY_TIME_PARSER_POLICY to LEGACY" +
        s" to restore the behavior before Spark 3.0. 2) You can form a valid datetime pattern" +
        s" with the guide from https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html",
        e)
  }
}

private object DateTimeFormatterHelper {
  val cache = CacheBuilder.newBuilder()
    .maximumSize(128)
    .build[(String, Locale, Boolean), DateTimeFormatter]()

  final val extractor = "^([^S]*)(S*)(.*)$".r

  def createBuilder(): DateTimeFormatterBuilder = {
    new DateTimeFormatterBuilder().parseCaseInsensitive()
  }

  def toFormatter(builder: DateTimeFormatterBuilder, locale: Locale): DateTimeFormatter = {
    builder
      .toFormatter(locale)
      .withChronology(IsoChronology.INSTANCE)
      .withResolverStyle(ResolverStyle.STRICT)
  }

  /**
   * Building a formatter for parsing seconds fraction with variable length
   */
  def createBuilderWithVarLengthSecondFraction(
      pattern: String): DateTimeFormatterBuilder = {
    val builder = createBuilder()
    pattern.split("'").zipWithIndex.foreach {
      // Split string starting with the regex itself which is `'` here will produce an extra empty
      // string at res(0). So when the first element here is empty string we do not need append `'`
      // literal to the DateTimeFormatterBuilder.
      case ("", idx) if idx != 0 => builder.appendLiteral("'")
      case (pattenPart, idx) if idx % 2 == 0 =>
        var rest = pattenPart
        while (rest.nonEmpty) {
          rest match {
            case extractor(prefix, secondFraction, suffix) =>
              builder.appendPattern(prefix)
              if (secondFraction.nonEmpty) {
                builder.appendFraction(ChronoField.NANO_OF_SECOND, 1, secondFraction.length, false)
              }
              rest = suffix
            case _ => throw new IllegalArgumentException(s"Unrecognized datetime pattern: $pattern")
          }
        }
      case (patternPart, _) => builder.appendLiteral(patternPart)
    }
    builder
  }

  def buildFormatter(
      pattern: String,
      locale: Locale,
      varLenEnabled: Boolean): DateTimeFormatter = {
    val builder = if (varLenEnabled) {
      createBuilderWithVarLengthSecondFraction(pattern)
    } else {
      createBuilder().appendPattern(pattern)
    }
    toFormatter(builder, locale)
  }

  lazy val fractionFormatter: DateTimeFormatter = {
    val builder = createBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .appendLiteral(' ')
      .appendValue(ChronoField.HOUR_OF_DAY, 2).appendLiteral(':')
      .appendValue(ChronoField.MINUTE_OF_HOUR, 2).appendLiteral(':')
      .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
      .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
    toFormatter(builder, TimestampFormatter.defaultLocale)
  }

  private final val bugInStandAloneForm = {
    // Java 8 has a bug for stand-alone form. See https://bugs.openjdk.java.net/browse/JDK-8114833
    // Note: we only check the US locale so that it's a static check. It can produce false-negative
    // as some locales are not affected by the bug. Since `L`/`q` is rarely used, we choose to not
    // complicate the check here.
    // TODO: remove it when we drop Java 8 support.
    val formatter = DateTimeFormatter.ofPattern("LLL qqq", Locale.US)
    formatter.format(LocalDate.of(2000, 1, 1)) == "1 1"
  }
  // SPARK-31892: The week-based date fields are rarely used and really confusing for parsing values
  // to datetime, especially when they are mixed with other non-week-based ones;
  // SPARK-31879: It's also difficult for us to restore the behavior of week-based date fields
  // formatting, in DateTimeFormatter the first day of week for week-based date fields become
  // localized, for the default Locale.US, it uses Sunday as the first day of week, while in Spark
  // 2.4, the SimpleDateFormat uses Monday as the first day of week.
  final val weekBasedLetters = Set('Y', 'W', 'w', 'u', 'e', 'c')
  final val unsupportedLetters = Set('A', 'n', 'N', 'p')
  // The quarter fields will also be parsed strangely, e.g. when the pattern contains `yMd` and can
  // be directly resolved then the `q` do check for whether the month is valid, but if the date
  // fields is incomplete, e.g. `yM`, the checking will be bypassed.
  final val unsupportedLettersForParsing = Set('E', 'F', 'q', 'Q')
  final val unsupportedPatternLengths = {
    // SPARK-31771: Disable Narrow-form TextStyle to avoid silent data change, as it is Full-form in
    // 2.4
    Seq("G", "M", "L", "E", "Q", "q").map(_ * 5) ++
      // SPARK-31867: Disable year pattern longer than 10 which will cause Java time library throw
      // unchecked `ArrayIndexOutOfBoundsException` by the `NumberPrinterParser` for formatting. It
      // makes the call side difficult to handle exceptions and easily leads to silent data change
      // because of the exceptions being suppressed.
      // SPARK-32424: The max year that we can actually handle is 6 digits, otherwise, it will
      // overflow
      Seq("y").map(_ * 7)
  }.toSet

  /**
   * In Spark 3.0, we switch to the Proleptic Gregorian calendar and use DateTimeFormatter for
   * parsing/formatting datetime values. The pattern string is incompatible with the one defined
   * by SimpleDateFormat in Spark 2.4 and earlier. This function converts all incompatible pattern
   * for the new parser in Spark 3.0. See more details in SPARK-31030.
   * @param pattern The input pattern.
   * @return The pattern for new parser
   */
  def convertIncompatiblePattern(pattern: String, isParsing: Boolean): String = {
    val eraDesignatorContained = pattern.split("'").zipWithIndex.exists {
      case (patternPart, index) =>
        // Text can be quoted using single quotes, we only check the non-quote parts.
        index % 2 == 0 && patternPart.contains("G")
    }
    (pattern + " ").split("'").zipWithIndex.map {
      case (patternPart, index) =>
        if (index % 2 == 0) {
          for (c <- patternPart if weekBasedLetters.contains(c)) {
            throw new IllegalArgumentException(s"All week-based patterns are unsupported since" +
              s" Spark 3.0, detected: $c, Please use the SQL function EXTRACT instead")
          }
          for (c <- patternPart if unsupportedLetters.contains(c) ||
            (isParsing && unsupportedLettersForParsing.contains(c))) {
            throw new IllegalArgumentException(s"Illegal pattern character: $c")
          }
          for (style <- unsupportedPatternLengths if patternPart.contains(style)) {
            throw new IllegalArgumentException(s"Too many pattern letters: ${style.head}")
          }
          if (bugInStandAloneForm && (patternPart.contains("LLL") || patternPart.contains("qqq"))) {
            throw new IllegalArgumentException("Java 8 has a bug to support stand-alone " +
              "form (3 or more 'L' or 'q' in the pattern string). Please use 'M' or 'Q' instead, " +
              "or upgrade your Java version. For more details, please read " +
              "https://bugs.openjdk.java.net/browse/JDK-8114833")
          }
          // In DateTimeFormatter, 'u' supports negative years. We substitute 'y' to 'u' here for
          // keeping the support in Spark 3.0. If parse failed in Spark 3.0, fall back to 'y'.
          // We only do this substitution when there is no era designator found in the pattern.
          if (!eraDesignatorContained) {
            patternPart.replace("y", "u")
          } else {
            patternPart
          }
        } else {
          patternPart
        }
    }.mkString("'").stripSuffix(" ")
  }
}
