package com.lk.setl.sql.catalyst.util;

public class DateTimeConstants {

    public static final int MONTHS_PER_YEAR = 12;

    public static final byte DAYS_PER_WEEK = 7;

    public static final long HOURS_PER_DAY = 24L;

    public static final long MINUTES_PER_HOUR = 60L;

    public static final long SECONDS_PER_MINUTE = 60L;
    public static final long SECONDS_PER_HOUR = MINUTES_PER_HOUR * SECONDS_PER_MINUTE;
    public static final long SECONDS_PER_DAY = HOURS_PER_DAY * SECONDS_PER_HOUR;

    public static final long MILLIS_PER_SECOND = 1000L;
    public static final long MILLIS_PER_MINUTE = SECONDS_PER_MINUTE * MILLIS_PER_SECOND;
    public static final long MILLIS_PER_HOUR = MINUTES_PER_HOUR * MILLIS_PER_MINUTE;
    public static final long MILLIS_PER_DAY = HOURS_PER_DAY * MILLIS_PER_HOUR;

    public static final long MICROS_PER_MILLIS = 1000L;
    public static final long MICROS_PER_SECOND = MILLIS_PER_SECOND * MICROS_PER_MILLIS;
    public static final long MICROS_PER_MINUTE = SECONDS_PER_MINUTE * MICROS_PER_SECOND;
    public static final long MICROS_PER_HOUR = MINUTES_PER_HOUR * MICROS_PER_MINUTE;
    public static final long MICROS_PER_DAY = HOURS_PER_DAY * MICROS_PER_HOUR;

    public static final long NANOS_PER_MICROS = 1000L;
    public static final long NANOS_PER_MILLIS = MICROS_PER_MILLIS * NANOS_PER_MICROS;
    public static final long NANOS_PER_SECOND = MILLIS_PER_SECOND * NANOS_PER_MILLIS;
}

