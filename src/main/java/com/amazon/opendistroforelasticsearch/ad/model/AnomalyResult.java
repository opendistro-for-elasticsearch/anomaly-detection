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
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Include result returned from RCF model and feature data.
 * TODO: fix rotating anomaly result index
 */
public class AnomalyResult implements ToXContentObject {

    public static final String ANOMALY_RESULT_INDEX = ".opendistro-anomaly-results";

    public static final String DETECTOR_ID_FIELD = "detector_id";
    private static final String ANOMALY_SCORE_FIELD = "anomaly_score";
    private static final String ANOMALY_GRADE_FIELD = "anomaly_grade";
    private static final String CONFIDENCE_FIELD = "confidence";
    private static final String FEATURE_DATA_FIELD = "feature_data";
    private static final String START_TIME_FIELD = "start_time";
    public static final String END_TIME_FIELD = "end_time";


    private final String detectorId;
    private final Double anomalyScore;
    private final Double anomalyGrade;
    private final Double confidence;
    private final List<FeatureData> featureData;
    private final Instant startTime;
    private final Instant endTime;

    public AnomalyResult(String detectorId, Double anomalyScore, Double anomalyGrade,
                         Double confidence, List<FeatureData> featureData, Instant startTime, Instant endTime) {
        this.detectorId = detectorId;
        this.anomalyScore = anomalyScore;
        this.anomalyGrade = anomalyGrade;
        this.confidence = confidence;
        this.featureData = featureData;
        this.startTime = startTime;
        this.endTime = endTime;
    }


    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject()
                .field(DETECTOR_ID_FIELD, detectorId)
                .field(ANOMALY_SCORE_FIELD, anomalyScore)
                .field(ANOMALY_GRADE_FIELD, anomalyGrade)
                .field(CONFIDENCE_FIELD, confidence)
                .field(FEATURE_DATA_FIELD, featureData.toArray())
                .field(START_TIME_FIELD, startTime.toEpochMilli())
                .field(END_TIME_FIELD, endTime.toEpochMilli());
        return xContentBuilder.endObject();
    }

    public static AnomalyResult parse(XContentParser parser) throws IOException {
        String detectorId = null;
        Double anomalyScore = null;
        Double anomalyGrade = null;
        Double confidence = null;
        List<FeatureData> featureData = new ArrayList<>();
        Instant startTime = null;
        Instant endTime = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case DETECTOR_ID_FIELD:
                    detectorId = parser.text();
                    break;
                case ANOMALY_SCORE_FIELD:
                    anomalyScore = parser.doubleValue();
                    break;
                case ANOMALY_GRADE_FIELD:
                    anomalyGrade = parser.doubleValue();
                    break;
                case CONFIDENCE_FIELD:
                    confidence = parser.doubleValue();
                    break;
                case FEATURE_DATA_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser::getTokenLocation);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        featureData.add(FeatureData.parse(parser));
                    }
                    break;
                case START_TIME_FIELD:
                    startTime = ParseUtils.toInstant(parser);
                    break;
                case END_TIME_FIELD:
                    endTime = ParseUtils.toInstant(parser);
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return new AnomalyResult(detectorId, anomalyScore, anomalyGrade, confidence, featureData,
                startTime, endTime);
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnomalyResult that = (AnomalyResult) o;
        return Objects.equal(getDetectorId(), that.getDetectorId()) &&
                Objects.equal(getAnomalyScore(), that.getAnomalyScore()) &&
                Objects.equal(getAnomalyGrade(), that.getAnomalyGrade()) &&
                Objects.equal(getConfidence(), that.getConfidence()) &&
                Objects.equal(getFeatureData(), that.getFeatureData()) &&
                Objects.equal(getStartTime(), that.getStartTime()) &&
                Objects.equal(getEndTime(), that.getEndTime());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(getDetectorId(), getAnomalyScore(), getAnomalyGrade(), getConfidence(),
                getFeatureData(), getStartTime(), getEndTime());
    }

    public String getDetectorId() {
        return detectorId;
    }

    public Double getAnomalyScore() {
        return anomalyScore;
    }

    public Double getAnomalyGrade() {
        return anomalyGrade;
    }

    public Double getConfidence() {
        return confidence;
    }

    public List<FeatureData> getFeatureData() {
        return featureData;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public Instant getEndTime() {
        return endTime;
    }
}
