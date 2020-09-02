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

import java.time.Instant;

/**
 * A DateTimeRange is used to represent start and end time for a timeRange
 */
public class DateTimeRange {

    private long start;
    private long end;

    public DateTimeRange(long start, long end) {
        this.start = start;
        this.end = end;
    }

    public static DateTimeRange rangeBasedOfInterval(long windowDelay, long intervalLength, int numOfIntervals) {
        long dataEndTime = Instant.now().toEpochMilli() - windowDelay;
        long dataStartTime = dataEndTime - ((long) (numOfIntervals) * intervalLength);
        return new DateTimeRange(dataStartTime, dataEndTime);

    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public void setEnd(long end) {
        this.end = end;
    }

}
