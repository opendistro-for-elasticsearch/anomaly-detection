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
import com.amazon.opendistroforelasticsearch.ad.util.ParseUtils;
import com.google.common.base.Objects;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Input data needed to trigger anomaly detector.
 */
public class AnomalyDetectorExecutionInput implements ToXContentObject {

    private static final String DETECTOR_ID_FIELD = "detector_id";
    private static final String PERIOD_START_FIELD = "period_start";
    private static final String PERIOD_END_FIELD = "period_end";
    private static final String DETECTOR_FIELD = "detector";
    private Instant periodStart;
    private Instant periodEnd;
    private String detectorId;
    private AnomalyDetector detector;

    public AnomalyDetectorExecutionInput(String detectorId, Instant periodStart, Instant periodEnd, AnomalyDetector detector) {
        this.periodStart = periodStart;
        this.periodEnd = periodEnd;
        this.detectorId = detectorId;
        this.detector = detector;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
            .startObject()
            .field(DETECTOR_ID_FIELD, detectorId)
            .field(PERIOD_START_FIELD, periodStart.toEpochMilli())
            .field(PERIOD_END_FIELD, periodEnd.toEpochMilli())
            .field(DETECTOR_FIELD, detector);
        return xContentBuilder.endObject();
    }

    public static AnomalyDetectorExecutionInput parse(XContentParser parser, String detectorId) throws IOException {
        Instant periodStart = null;
        Instant periodEnd = null;
        AnomalyDetector detector = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case PERIOD_START_FIELD:
                    periodStart = ParseUtils.toInstant(parser);
                    break;
                case PERIOD_END_FIELD:
                    periodEnd = ParseUtils.toInstant(parser);
                    break;
                case DETECTOR_FIELD:
                    XContentParser.Token token = parser.currentToken();
                    if (parser.currentToken().equals(XContentParser.Token.START_OBJECT)) {
                        detector = AnomalyDetector.parse(parser, detectorId);
                    }
                    break;
                default:
                    break;
            }
        }
        return new AnomalyDetectorExecutionInput(detectorId, periodStart, periodEnd, detector);
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AnomalyDetectorExecutionInput that = (AnomalyDetectorExecutionInput) o;
        return Objects.equal(getPeriodStart(), that.getPeriodStart())
            && Objects.equal(getPeriodEnd(), that.getPeriodEnd())
            && Objects.equal(getDetectorId(), that.getDetectorId())
            && Objects.equal(getDetector(), that.getDetector());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(periodStart, periodEnd, detectorId);
    }

    public Instant getPeriodStart() {
        return periodStart;
    }

    public Instant getPeriodEnd() {
        return periodEnd;
    }

    public String getDetectorId() {
        return detectorId;
    }

    public AnomalyDetector getDetector() {
        return detector;
    }

    public void setDetectorId(String detectorId) {
        this.detectorId = detectorId;
    }
}
