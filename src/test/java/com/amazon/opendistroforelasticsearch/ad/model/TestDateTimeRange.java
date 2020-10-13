/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

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
