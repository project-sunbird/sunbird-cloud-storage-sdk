package org.sunbird.cloud.storage.util;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Date range utility for computing date-based prefixes used in object search operations.
 * Replaces Joda-Time usage with java.time API.
 */
public final class DateRangeUtil {

    private static final DateTimeFormatter DEFAULT_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private DateRangeUtil() {
    }

    /**
     * Calculate the start date by subtracting delta days from the end date.
     *
     * @param endDate end date string (nullable — defaults to today)
     * @param delta   number of days to subtract
     * @return the start date as a string in yyyy-MM-dd format
     */
    public static String getStartDate(String endDate, int delta) {
        LocalDate to = (endDate != null && !endDate.isEmpty())
                ? LocalDate.parse(endDate, DEFAULT_FORMAT)
                : LocalDate.now();
        return to.minusDays(delta).toString();
    }

    /**
     * Get all dates between fromDate and toDate (inclusive) formatted with the given pattern.
     *
     * @param fromDate start date string
     * @param toDate   end date string (nullable — defaults to today)
     * @param pattern  date format pattern
     * @return array of formatted date strings
     */
    public static String[] getDatesBetween(String fromDate, String toDate, String pattern) {
        DateTimeFormatter df = DateTimeFormatter.ofPattern(pattern);
        LocalDate to = (toDate != null && !toDate.isEmpty())
                ? LocalDate.parse(toDate, df)
                : LocalDate.now();
        LocalDate from = LocalDate.parse(fromDate, df);
        long daysBetween = ChronoUnit.DAYS.between(from, to);
        List<String> dates = new ArrayList<>();
        for (long i = 0; i <= daysBetween; i++) {
            dates.add(from.plusDays(i).format(df));
        }
        return dates.toArray(new String[0]);
    }
}
