package org.sunbird.cloud.storage.util;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DateRangeUtilTest {

    @Test
    void getStartDate_subtractsDeltaFromEndDate() {
        String result = DateRangeUtil.getStartDate("2024-05-10", 5);
        assertEquals("2024-05-05", result);
    }

    @Test
    void getStartDate_nullEndDateUsesToday() {
        // Just assert it returns a valid date string without throwing
        String result = DateRangeUtil.getStartDate(null, 3);
        assertNotNull(result);
        assertTrue(result.matches("\\d{4}-\\d{2}-\\d{2}"));
    }

    @Test
    void getStartDate_emptyEndDateUsesToday() {
        String result = DateRangeUtil.getStartDate("", 7);
        assertNotNull(result);
        assertTrue(result.matches("\\d{4}-\\d{2}-\\d{2}"));
    }

    @Test
    void getDatesBetween_inclusiveDateRange() {
        String[] dates = DateRangeUtil.getDatesBetween("2024-03-01", "2024-03-05", "yyyy-MM-dd");
        assertEquals(5, dates.length);
        assertEquals("2024-03-01", dates[0]);
        assertEquals("2024-03-05", dates[4]);
    }

    @Test
    void getDatesBetween_singleDayRange() {
        String[] dates = DateRangeUtil.getDatesBetween("2024-06-15", "2024-06-15", "yyyy-MM-dd");
        assertEquals(1, dates.length);
        assertEquals("2024-06-15", dates[0]);
    }

    @Test
    void getDatesBetween_nullToDateUsesToday() {
        // fromDate = yesterday relative to today; result should have >= 1 element
        String yesterday = DateRangeUtil.getStartDate(null, 1);
        String[] dates = DateRangeUtil.getDatesBetween(yesterday, null, "yyyy-MM-dd");
        assertTrue(dates.length >= 1);
    }

    @Test
    void getDatesBetween_customPattern() {
        String[] dates = DateRangeUtil.getDatesBetween("01/03/2024", "03/03/2024", "dd/MM/yyyy");
        assertEquals(3, dates.length);
        assertEquals("01/03/2024", dates[0]);
        assertEquals("03/03/2024", dates[2]);
    }

    @Test
    void getDatesBetween_containsConsecutiveDates() {
        String[] dates = DateRangeUtil.getDatesBetween("2024-01-30", "2024-02-02", "yyyy-MM-dd");
        assertEquals(4, dates.length);
        assertEquals("2024-01-30", dates[0]);
        assertEquals("2024-01-31", dates[1]);
        assertEquals("2024-02-01", dates[2]);
        assertEquals("2024-02-02", dates[3]);
    }
}
