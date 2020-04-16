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

import com.amazon.opendistroforelasticsearch.ad.util.ParseUtils;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.Schedule;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.ScheduleParser;
import com.google.common.base.Objects;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.DEFAULT_AD_JOB_LOC_DURATION_SECONDS;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Anomaly detector job.
 */
public class AnomalyDetectorJob implements ToXContentObject, ScheduledJobParameter {

    public static final String ANOMALY_DETECTOR_JOB_INDEX = ".opendistro-anomaly-detector-jobs";
    public static final String NAME_FIELD = "name";
    public static final String LAST_UPDATE_TIME_FIELD = "last_update_time";
    public static final String LOCK_DURATION_SECONDS = "lock_duration_seconds";

    private static final String SCHEDULE_FIELD = "schedule";
    private static final String WINDOW_DELAY_FIELD = "window_delay";
    private static final String IS_ENABLED_FIELD = "enabled";
    private static final String ENABLED_TIME_FIELD = "enabled_time";
    private static final String DISABLED_TIME_FIELD = "disabled_time";

    private final String name;
    private final Schedule schedule;
    private final TimeConfiguration windowDelay;
    private final Boolean isEnabled;
    private final Instant enabledTime;
    private final Instant disabledTime;
    private final Instant lastUpdateTime;
    private final Long lockDurationSeconds;

    public AnomalyDetectorJob(
        String name,
        Schedule schedule,
        TimeConfiguration windowDelay,
        Boolean isEnabled,
        Instant enabledTime,
        Instant disabledTime,
        Instant lastUpdateTime,
        Long lockDurationSeconds
    ) {
        this.name = name;
        this.schedule = schedule;
        this.windowDelay = windowDelay;
        this.isEnabled = isEnabled;
        this.enabledTime = enabledTime;
        this.disabledTime = disabledTime;
        this.lastUpdateTime = lastUpdateTime;
        this.lockDurationSeconds = lockDurationSeconds;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
            .startObject()
            .field(NAME_FIELD, name)
            .field(SCHEDULE_FIELD, schedule)
            .field(WINDOW_DELAY_FIELD, windowDelay)
            .field(IS_ENABLED_FIELD, isEnabled)
            .field(ENABLED_TIME_FIELD, enabledTime.toEpochMilli())
            .field(LAST_UPDATE_TIME_FIELD, lastUpdateTime.toEpochMilli())
            .field(LOCK_DURATION_SECONDS, lockDurationSeconds);
        if (disabledTime != null) {
            xContentBuilder.field(DISABLED_TIME_FIELD, disabledTime.toEpochMilli());
        }
        return xContentBuilder.endObject();
    }

    public static AnomalyDetectorJob parse(XContentParser parser) throws IOException {
        String name = null;
        Schedule schedule = null;
        TimeConfiguration windowDelay = null;
        // we cannot set it to null as isEnabled() would do the unboxing and results in null pointer exception
        Boolean isEnabled = Boolean.FALSE;
        Instant enabledTime = null;
        Instant disabledTime = null;
        Instant lastUpdateTime = null;
        Long lockDurationSeconds = DEFAULT_AD_JOB_LOC_DURATION_SECONDS;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case NAME_FIELD:
                    name = parser.text();
                    break;
                case SCHEDULE_FIELD:
                    schedule = ScheduleParser.parse(parser);
                    break;
                case WINDOW_DELAY_FIELD:
                    windowDelay = TimeConfiguration.parse(parser);
                    break;
                case IS_ENABLED_FIELD:
                    isEnabled = parser.booleanValue();
                    break;
                case ENABLED_TIME_FIELD:
                    enabledTime = ParseUtils.toInstant(parser);
                    break;
                case DISABLED_TIME_FIELD:
                    disabledTime = ParseUtils.toInstant(parser);
                    break;
                case LAST_UPDATE_TIME_FIELD:
                    lastUpdateTime = ParseUtils.toInstant(parser);
                    break;
                case LOCK_DURATION_SECONDS:
                    lockDurationSeconds = parser.longValue();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return new AnomalyDetectorJob(
            name,
            schedule,
            windowDelay,
            isEnabled,
            enabledTime,
            disabledTime,
            lastUpdateTime,
            lockDurationSeconds
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AnomalyDetectorJob that = (AnomalyDetectorJob) o;
        return Objects.equal(getName(), that.getName())
            && Objects.equal(getSchedule(), that.getSchedule())
            && Objects.equal(isEnabled(), that.isEnabled())
            && Objects.equal(getEnabledTime(), that.getEnabledTime())
            && Objects.equal(getDisabledTime(), that.getDisabledTime())
            && Objects.equal(getLastUpdateTime(), that.getLastUpdateTime())
            && Objects.equal(getLockDurationSeconds(), that.getLockDurationSeconds());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, schedule, isEnabled, enabledTime, lastUpdateTime);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Schedule getSchedule() {
        return schedule;
    }

    public TimeConfiguration getWindowDelay() {
        return windowDelay;
    }

    @Override
    public boolean isEnabled() {
        return isEnabled;
    }

    @Override
    public Instant getEnabledTime() {
        return enabledTime;
    }

    public Instant getDisabledTime() {
        return disabledTime;
    }

    @Override
    public Instant getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public Long getLockDurationSeconds() {
        return lockDurationSeconds;
    }
}
