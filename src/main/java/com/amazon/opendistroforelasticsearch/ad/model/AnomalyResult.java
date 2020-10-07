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

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import com.amazon.opendistroforelasticsearch.ad.annotation.Generated;
import com.amazon.opendistroforelasticsearch.ad.util.ParseUtils;
import com.google.common.base.Objects;

/**
 * Include result returned from RCF model and feature data.
 * TODO: fix rotating anomaly result index
 */
public class AnomalyResult implements ToXContentObject, Writeable {

    public static final String PARSE_FIELD_NAME = "AnomalyResult";
    public static final NamedXContentRegistry.Entry XCONTENT_REGISTRY = new NamedXContentRegistry.Entry(
        AnomalyResult.class,
        new ParseField(PARSE_FIELD_NAME),
        it -> parse(it)
    );

    public static final String DETECTOR_ID_FIELD = "detector_id";
    public static final String ANOMALY_SCORE_FIELD = "anomaly_score";
    private static final String ANOMALY_GRADE_FIELD = "anomaly_grade";
    private static final String CONFIDENCE_FIELD = "confidence";
    private static final String FEATURE_DATA_FIELD = "feature_data";
    private static final String DATA_START_TIME_FIELD = "data_start_time";
    public static final String DATA_END_TIME_FIELD = "data_end_time";
    private static final String EXECUTION_START_TIME_FIELD = "execution_start_time";
    public static final String EXECUTION_END_TIME_FIELD = "execution_end_time";
    public static final String ERROR_FIELD = "error";
    public static final String ENTITY_FIELD = "entity";

    private final String detectorId;
    private final Double anomalyScore;
    private final Double anomalyGrade;
    private final Double confidence;
    private final List<FeatureData> featureData;
    private final Instant dataStartTime;
    private final Instant dataEndTime;
    private final Instant executionStartTime;
    private final Instant executionEndTime;
    private final String error;
    private final List<Entity> entity;

    public AnomalyResult(
        String detectorId,
        Double anomalyScore,
        Double anomalyGrade,
        Double confidence,
        List<FeatureData> featureData,
        Instant dataStartTime,
        Instant dataEndTime,
        Instant executionStartTime,
        Instant executionEndTime,
        String error
    ) {
        this.detectorId = detectorId;
        this.anomalyScore = anomalyScore;
        this.anomalyGrade = anomalyGrade;
        this.confidence = confidence;
        this.featureData = featureData;
        this.dataStartTime = dataStartTime;
        this.dataEndTime = dataEndTime;
        this.executionStartTime = executionStartTime;
        this.executionEndTime = executionEndTime;
        this.error = error;
        this.entity = null;
    }

    public AnomalyResult(
        String detectorId,
        Double anomalyScore,
        Double anomalyGrade,
        Double confidence,
        List<FeatureData> featureData,
        Instant dataStartTime,
        Instant dataEndTime,
        Instant executionStartTime,
        Instant executionEndTime,
        String error,
        List<Entity> entity
    ) {
        this.detectorId = detectorId;
        this.anomalyScore = anomalyScore;
        this.anomalyGrade = anomalyGrade;
        this.confidence = confidence;
        this.featureData = featureData;
        this.dataStartTime = dataStartTime;
        this.dataEndTime = dataEndTime;
        this.executionStartTime = executionStartTime;
        this.executionEndTime = executionEndTime;
        this.error = error;
        this.entity = entity;
    }

    public AnomalyResult(StreamInput input) throws IOException {
        this.detectorId = input.readString();
        this.anomalyScore = input.readDouble();
        this.anomalyGrade = input.readDouble();
        this.confidence = input.readDouble();
        int featureSize = input.readVInt();
        this.featureData = new ArrayList<FeatureData>(featureSize);
        for (int i = 0; i < featureSize; i++) {
            featureData.add(new FeatureData(input));
        }
        this.dataStartTime = input.readInstant();
        this.dataEndTime = input.readInstant();
        this.executionStartTime = input.readInstant();
        this.executionEndTime = input.readInstant();
        this.error = input.readOptionalString();
        int entitySize = input.readVInt();
        this.entity = new ArrayList<Entity>(entitySize);
        for (int i = 0; i < entitySize; i++) {
            entity.add(new Entity(input));
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
            .startObject()
            .field(DETECTOR_ID_FIELD, detectorId)
            .field(DATA_START_TIME_FIELD, dataStartTime.toEpochMilli())
            .field(DATA_END_TIME_FIELD, dataEndTime.toEpochMilli());
        if (featureData != null) {
            // can be null during preview
            xContentBuilder.field(FEATURE_DATA_FIELD, featureData.toArray());
        }
        if (executionStartTime != null) {
            // can be null during preview
            xContentBuilder.field(EXECUTION_START_TIME_FIELD, executionStartTime.toEpochMilli());
        }
        if (executionEndTime != null) {
            // can be null during preview
            xContentBuilder.field(EXECUTION_END_TIME_FIELD, executionEndTime.toEpochMilli());
        }
        if (anomalyScore != null && !anomalyScore.isNaN()) {
            xContentBuilder.field(ANOMALY_SCORE_FIELD, anomalyScore);
        }
        if (anomalyGrade != null && !anomalyGrade.isNaN()) {
            xContentBuilder.field(ANOMALY_GRADE_FIELD, anomalyGrade);
        }
        if (confidence != null && !confidence.isNaN()) {
            xContentBuilder.field(CONFIDENCE_FIELD, confidence);
        }
        if (error != null) {
            xContentBuilder.field(ERROR_FIELD, error);
        }
        if (entity != null) {
            xContentBuilder.field(ENTITY_FIELD, entity.toArray());
        }
        return xContentBuilder.endObject();
    }

    public static AnomalyResult parse(XContentParser parser) throws IOException {
        String detectorId = null;
        Double anomalyScore = null;
        Double anomalyGrade = null;
        Double confidence = null;
        List<FeatureData> featureData = new ArrayList<>();
        Instant dataStartTime = null;
        Instant dataEndTime = null;
        Instant executionStartTime = null;
        Instant executionEndTime = null;
        String error = null;
        List<Entity> entityList = null;

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
                case DATA_START_TIME_FIELD:
                    dataStartTime = ParseUtils.toInstant(parser);
                    break;
                case DATA_END_TIME_FIELD:
                    dataEndTime = ParseUtils.toInstant(parser);
                    break;
                case EXECUTION_START_TIME_FIELD:
                    executionStartTime = ParseUtils.toInstant(parser);
                    break;
                case EXECUTION_END_TIME_FIELD:
                    executionEndTime = ParseUtils.toInstant(parser);
                    break;
                case ERROR_FIELD:
                    error = parser.text();
                    break;
                case ENTITY_FIELD:
                    entityList = new ArrayList<>();
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser::getTokenLocation);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        entityList.add(Entity.parse(parser));
                    }
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return new AnomalyResult(
            detectorId,
            anomalyScore,
            anomalyGrade,
            confidence,
            featureData,
            dataStartTime,
            dataEndTime,
            executionStartTime,
            executionEndTime,
            error,
            entityList
        );
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AnomalyResult that = (AnomalyResult) o;
        return Objects.equal(getDetectorId(), that.getDetectorId())
            && Objects.equal(getAnomalyScore(), that.getAnomalyScore())
            && Objects.equal(getAnomalyGrade(), that.getAnomalyGrade())
            && Objects.equal(getConfidence(), that.getConfidence())
            && Objects.equal(getFeatureData(), that.getFeatureData())
            && Objects.equal(getDataStartTime(), that.getDataStartTime())
            && Objects.equal(getDataEndTime(), that.getDataEndTime())
            && Objects.equal(getExecutionStartTime(), that.getExecutionStartTime())
            && Objects.equal(getExecutionEndTime(), that.getExecutionEndTime())
            && Objects.equal(getError(), that.getError())
            && Objects.equal(getEntity(), that.getEntity());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects
            .hashCode(
                getDetectorId(),
                getAnomalyScore(),
                getAnomalyGrade(),
                getConfidence(),
                getFeatureData(),
                getDataStartTime(),
                getDataEndTime(),
                getExecutionStartTime(),
                getExecutionEndTime(),
                getError(),
                getEntity()
            );
    }

    @Generated
    @Override
    public String toString() {
        return new ToStringBuilder(this)
            .append("detectorId", detectorId)
            .append("anomalyScore", anomalyScore)
            .append("anomalyGrade", anomalyGrade)
            .append("confidence", confidence)
            .append("featureData", featureData)
            .append("dataStartTime", dataStartTime)
            .append("dataEndTime", dataEndTime)
            .append("executionStartTime", executionStartTime)
            .append("executionEndTime", executionEndTime)
            .append("error", error)
            .append("entity", entity)
            .toString();
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

    public Instant getDataStartTime() {
        return dataStartTime;
    }

    public Instant getDataEndTime() {
        return dataEndTime;
    }

    public Instant getExecutionStartTime() {
        return executionStartTime;
    }

    public Instant getExecutionEndTime() {
        return executionEndTime;
    }

    public String getError() {
        return error;
    }

    public List<Entity> getEntity() {
        return entity;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(detectorId);
        out.writeDouble(anomalyScore);
        out.writeDouble(anomalyGrade);
        out.writeDouble(confidence);
        out.writeVInt(featureData.size());
        for (FeatureData feature : featureData) {
            feature.writeTo(out);
        }
        out.writeInstant(dataStartTime);
        out.writeInstant(dataEndTime);
        out.writeInstant(executionStartTime);
        out.writeInstant(executionEndTime);
        out.writeOptionalString(error);
        out.writeVInt(entity.size());
        for (Entity entityItem : entity) {
            entityItem.writeTo(out);
        }
    }
}
