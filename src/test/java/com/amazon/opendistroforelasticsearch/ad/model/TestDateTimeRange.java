package com.amazon.opendistroforelasticsearch.ad.model;

import org.elasticsearch.test.ESTestCase;

public class TestDateTimeRange extends ESTestCase {
    public void testDateTimeRangeGetStart() {
        DateTimeRange timeRange = new DateTimeRange(10, 20);
        assertEquals(10, timeRange.getStart());
    }

    public void testDateTimeRangeGetEnd() {
        DateTimeRange timeRange = new DateTimeRange(10, 20);
        assertEquals(20, timeRange.getEnd());
    }

    public void testDateTimeRangeSetEnd() {
        DateTimeRange timeRange = new DateTimeRange(10, 20);
        timeRange.setEnd(50);
        assertEquals(50, timeRange.getEnd());
    }

    public void testDateTimeRangeSetStart() {
        DateTimeRange timeRange = DateTimeRange.rangeBasedOfInterval(0, 20, 2);
        timeRange.setStart(10);
        assertEquals(10, timeRange.getStart());
    }
}
