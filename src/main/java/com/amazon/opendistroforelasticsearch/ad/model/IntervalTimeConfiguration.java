/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.opendistroforelasticsearch.ad.annotation.Generated;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.Set;

public class IntervalTimeConfiguration extends TimeConfiguration {

    private long interval;
    private ChronoUnit unit;

    private static final Set<ChronoUnit> SUPPORTED_UNITS = ImmutableSet.of(ChronoUnit.MINUTES, ChronoUnit.SECONDS);

    /**
     * Constructor function.
     *
     * @param interval interval period value
     * @param unit     time unit
     */
    public IntervalTimeConfiguration(long interval, ChronoUnit unit) {
        if (interval < 0) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Interval %s should be non-negative", interval));
        }
        if (!SUPPORTED_UNITS.contains(unit)) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Timezone %s is not supported", unit));
        }
        this.interval = interval;
        this.unit = unit;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().startObject(PERIOD_FIELD).field(INTERVAL_FIELD, interval).field(UNIT_FIELD, unit).endObject().endObject();
        return builder;
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        IntervalTimeConfiguration that = (IntervalTimeConfiguration) o;
        return getInterval() == that.getInterval() && getUnit() == that.getUnit();
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(interval, unit);
    }

    public long getInterval() {
        return interval;
    }

    public ChronoUnit getUnit() {
        return unit;
    }

    /**
     * Returns the duration of the interval.
     *
     * @return the duration of the interval
     */
    public Duration toDuration() {
        return Duration.of(interval, unit);
    }
}
