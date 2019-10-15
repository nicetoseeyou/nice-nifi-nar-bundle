package lab.nice.nifi.processor.db.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Time utility class
 */
public final class TimeUtility {
    private TimeUtility() {
    }

    /**
     * Convert date string to epoch milliseconds
     *
     * @param date   the date to convert
     * @param format the date format, support ISO standard format name or customise format
     * @param zoneId the timezone of the date
     * @return epoch milliseconds
     */
    public static Long dateToEpochMilli(String date, String format, String zoneId) {
        final DateTimeFormatter formatter = parseDateTimeFormatter(format);
        final LocalDate localDate = LocalDate.parse(date, formatter);
        return localDate.atStartOfDay().atZone(ZoneId.of(zoneId)).toInstant().toEpochMilli();
    }

    /**
     * Convert time string to epoch milliseconds
     *
     * @param time   the time to convert
     * @param format the time format, support ISO standard format name or customise format
     * @param zoneId the timezone of the time
     * @return epoch milliseconds
     */
    public static Long timeToEpochMilli(String time, String format, String zoneId) {
        final DateTimeFormatter formatter = parseDateTimeFormatter(format);
        final LocalTime localTime = LocalTime.parse(time, formatter);
        return localTime.atDate(LocalDate.ofEpochDay(0)).atZone(ZoneId.of(zoneId)).toInstant().toEpochMilli();
    }

    /**
     * Convert timestamp string to epoch milliseconds
     *
     * @param timestamp the timestamp to convert
     * @param format    the timestamp format, support ISO standard format name or customise format
     * @param zoneId    the timezone of the timestamp
     * @return epoch milliseconds
     */
    public static Long timestampToEpochMilli(String timestamp, String format, String zoneId) {
        final DateTimeFormatter formatter = parseDateTimeFormatter(format);
        final LocalDateTime localDateTime = LocalDateTime.parse(timestamp, formatter);
        return localDateTime.atZone(ZoneId.of(zoneId)).toInstant().toEpochMilli();
    }

    /**
     * Convert epoch milliseconds to ZonedDateTime
     *
     * @param epochMilli       the epoch milliseconds to convert
     * @param targetTimezoneId the target timezone
     * @return {@link ZonedDateTime} ZonedDateTime
     */
    public static ZonedDateTime toZonedDateTime(Long epochMilli, String targetTimezoneId) {
        return Instant.ofEpochMilli(epochMilli).atZone(ZoneId.of(targetTimezoneId));
    }

    /**
     * Get formatter from string pattern.
     * Standard name format support:
     * BASIC_ISO_DATE -> {@link DateTimeFormatter#BASIC_ISO_DATE}
     * ISO_LOCAL_DATE -> {@link DateTimeFormatter#ISO_LOCAL_DATE}
     * ISO_OFFSET_DATE -> {@link DateTimeFormatter#ISO_OFFSET_DATE}
     * ISO_DATE -> {@link DateTimeFormatter#ISO_DATE}
     * ISO_LOCAL_TIME -> {@link DateTimeFormatter#ISO_LOCAL_TIME}
     * ISO_OFFSET_TIME -> {@link DateTimeFormatter#ISO_OFFSET_TIME}
     * ISO_TIME -> {@link DateTimeFormatter#ISO_TIME}
     * ISO_LOCAL_DATE_TIME -> {@link DateTimeFormatter#ISO_LOCAL_DATE_TIME}
     * ISO_OFFSET_DATE_TIME -> {@link DateTimeFormatter#ISO_OFFSET_DATE_TIME}
     * ISO_ZONED_DATE_TIME -> {@link DateTimeFormatter#ISO_ZONED_DATE_TIME}
     * ISO_DATE_TIME -> {@link DateTimeFormatter#ISO_DATE_TIME}
     * ISO_ORDINAL_DATE -> {@link DateTimeFormatter#ISO_ORDINAL_DATE}
     * ISO_WEEK_DATE -> {@link DateTimeFormatter#ISO_WEEK_DATE}
     * ISO_INSTANT -> {@link DateTimeFormatter#ISO_INSTANT}
     * RFC_1123_DATE_TIME -> {@link DateTimeFormatter#RFC_1123_DATE_TIME}
     *
     * @param pattern standard format name or customise datetime format pattern
     * @return formatter instant
     */
    public static DateTimeFormatter parseDateTimeFormatter(String pattern) {
        switch (pattern) {
            case "BASIC_ISO_DATE":
                return DateTimeFormatter.BASIC_ISO_DATE;
            case "ISO_LOCAL_DATE":
                return DateTimeFormatter.ISO_LOCAL_DATE;
            case "ISO_OFFSET_DATE":
                return DateTimeFormatter.ISO_OFFSET_DATE;
            case "ISO_DATE":
                return DateTimeFormatter.ISO_DATE;
            case "ISO_LOCAL_TIME":
                return DateTimeFormatter.ISO_LOCAL_TIME;
            case "ISO_OFFSET_TIME":
                return DateTimeFormatter.ISO_OFFSET_TIME;
            case "ISO_TIME":
                return DateTimeFormatter.ISO_TIME;
            case "ISO_LOCAL_DATE_TIME":
                return DateTimeFormatter.ISO_LOCAL_DATE_TIME;
            case "ISO_OFFSET_DATE_TIME":
                return DateTimeFormatter.ISO_OFFSET_DATE_TIME;
            case "ISO_ZONED_DATE_TIME":
                return DateTimeFormatter.ISO_ZONED_DATE_TIME;
            case "ISO_DATE_TIME":
                return DateTimeFormatter.ISO_DATE_TIME;
            case "ISO_ORDINAL_DATE":
                return DateTimeFormatter.ISO_ORDINAL_DATE;
            case "ISO_WEEK_DATE":
                return DateTimeFormatter.ISO_WEEK_DATE;
            case "ISO_INSTANT":
                return DateTimeFormatter.ISO_INSTANT;
            case "RFC_1123_DATE_TIME":
                return DateTimeFormatter.RFC_1123_DATE_TIME;
            default:
                return DateTimeFormatter.ofPattern(pattern);
        }
    }
}
