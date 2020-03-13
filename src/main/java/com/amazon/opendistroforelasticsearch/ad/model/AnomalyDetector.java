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
import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

/**
 * An AnomalyDetector is used to represent anomaly detection model(RCF) related parameters.
 */
public class AnomalyDetector implements ToXContentObject {

    public static final String PARSE_FIELD_NAME = "AnomalyDetector";
    public static final NamedXContentRegistry.Entry XCONTENT_REGISTRY = new NamedXContentRegistry.Entry(
        AnomalyDetector.class,
        new ParseField(PARSE_FIELD_NAME),
        it -> parse(it)
    );
    public static final String NO_ID = "";
    public static final String ANOMALY_DETECTORS_INDEX = ".opendistro-anomaly-detectors";
    public static final String TYPE = "_doc";
    public static final String QUERY_PARAM_PERIOD_START = "period_start";
    public static final String QUERY_PARAM_PERIOD_END = "period_end";

    private static final String NAME_FIELD = "name";
    private static final String DESCRIPTION_FIELD = "description";
    private static final String TIMEFIELD_FIELD = "time_field";
    private static final String SCHEMA_VERSION_FIELD = "schema_version";
    private static final String INDICES_FIELD = "indices";
    private static final String FILTER_QUERY_FIELD = "filter_query";
    private static final String FEATURE_ATTRIBUTES_FIELD = "feature_attributes";
    private static final String DETECTION_INTERVAL_FIELD = "detection_interval";
    private static final String WINDOW_DELAY_FIELD = "window_delay";
    private static final String LAST_UPDATE_TIME_FIELD = "last_update_time";
    public static final String UI_METADATA_FIELD = "ui_metadata";

    private final String detectorId;
    private final Long version;
    private final String name;
    private final String description;
    private final String timeField;
    private final List<String> indices;
    private final List<Feature> featureAttributes;
    private final QueryBuilder filterQuery;
    private final TimeConfiguration detectionInterval;
    private final TimeConfiguration windowDelay;
    private final Map<String, Object> uiMetadata;
    private final Integer schemaVersion;
    private final Instant lastUpdateTime;

    /**
     * Constructor function.
     *
     * @param detectorId        detector identifier
     * @param version           detector document version
     * @param name              detector name
     * @param description       description of detector
     * @param timeField         time field
     * @param indices           indices used as detector input
     * @param features          detector feature attributes
     * @param filterQuery       detector filter query
     * @param detectionInterval detecting interval
     * @param windowDelay       max delay window for realtime data
     * @param uiMetadata        metadata used by Kibana
     * @param schemaVersion     anomaly detector index mapping version
     * @param lastUpdateTime    detector's last update time
     */
    public AnomalyDetector(
        String detectorId,
        Long version,
        String name,
        String description,
        String timeField,
        List<String> indices,
        List<Feature> features,
        QueryBuilder filterQuery,
        TimeConfiguration detectionInterval,
        TimeConfiguration windowDelay,
        Map<String, Object> uiMetadata,
        Integer schemaVersion,
        Instant lastUpdateTime
    ) {
        if (Strings.isBlank(name)) {
            throw new IllegalArgumentException("Detector name should be set");
        }
        if (timeField == null) {
            throw new IllegalArgumentException("Time field should be set");
        }
        if (indices == null || indices.isEmpty()) {
            throw new IllegalArgumentException("Indices should be set");
        }
        if (detectionInterval == null) {
            throw new IllegalArgumentException("Detection interval should be set");
        }
        this.detectorId = detectorId;
        this.version = version;
        this.name = name;
        this.description = description;
        this.timeField = timeField;
        this.indices = indices;
        this.featureAttributes = features;
        this.filterQuery = filterQuery;
        this.detectionInterval = detectionInterval;
        this.windowDelay = windowDelay;
        this.uiMetadata = uiMetadata;
        this.schemaVersion = schemaVersion;
        this.lastUpdateTime = lastUpdateTime;
    }

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        return toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
            .startObject()
            .field(NAME_FIELD, name)
            .field(DESCRIPTION_FIELD, description)
            .field(TIMEFIELD_FIELD, timeField)
            .field(INDICES_FIELD, indices.toArray())
            .field(FEATURE_ATTRIBUTES_FIELD, featureAttributes.toArray())
            .field(FILTER_QUERY_FIELD, filterQuery)
            .field(DETECTION_INTERVAL_FIELD, detectionInterval)
            .field(WINDOW_DELAY_FIELD, windowDelay)
            .field(SCHEMA_VERSION_FIELD, schemaVersion);

        if (uiMetadata != null && !uiMetadata.isEmpty()) {
            xContentBuilder.field(UI_METADATA_FIELD, uiMetadata);
        }
        if (lastUpdateTime != null) {
            xContentBuilder.timeField(LAST_UPDATE_TIME_FIELD, LAST_UPDATE_TIME_FIELD, lastUpdateTime.toEpochMilli());
        }
        return xContentBuilder.endObject();
    }

    /**
     * Parse raw json content into anomaly detector instance.
     *
     * @param parser json based content parser
     * @return anomaly detector instance
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static AnomalyDetector parse(XContentParser parser) throws IOException {
        return parse(parser, null);
    }

    public static AnomalyDetector parse(XContentParser parser, String detectorId) throws IOException {
        return parse(parser, detectorId, null);
    }

    /**
     * Parse raw json content and given detector id into anomaly detector instance.
     *
     * @param parser     json based content parser
     * @param detectorId detector id
     * @param version    detector document version
     * @return anomaly detector instance
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static AnomalyDetector parse(XContentParser parser, String detectorId, Long version) throws IOException {
        return parse(parser, detectorId, version, null, null);
    }

    /**
     * Parse raw json content and given detector id into anomaly detector instance.
     *
     * @param parser                      json based content parser
     * @param detectorId                  detector id
     * @param version                     detector document version
     * @param defaultDetectionInterval    default detection interval
     * @param defaultDetectionWindowDelay default detection window delay
     * @return anomaly detector instance
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static AnomalyDetector parse(
        XContentParser parser,
        String detectorId,
        Long version,
        TimeValue defaultDetectionInterval,
        TimeValue defaultDetectionWindowDelay
    ) throws IOException {
        String name = null;
        String description = null;
        String timeField = null;
        List<String> indices = new ArrayList<String>();
        QueryBuilder filterQuery = QueryBuilders.matchAllQuery();
        TimeConfiguration detectionInterval = defaultDetectionInterval == null
            ? null
            : new IntervalTimeConfiguration(defaultDetectionInterval.getMinutes(), ChronoUnit.MINUTES);
        TimeConfiguration windowDelay = defaultDetectionWindowDelay == null
            ? null
            : new IntervalTimeConfiguration(defaultDetectionWindowDelay.getSeconds(), ChronoUnit.SECONDS);
        List<Feature> features = new ArrayList<>();
        int schemaVersion = 0;
        Map<String, Object> uiMetadata = null;
        Instant lastUpdateTime = Instant.now();

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case NAME_FIELD:
                    name = parser.text();
                    break;
                case DESCRIPTION_FIELD:
                    description = parser.text();
                    break;
                case TIMEFIELD_FIELD:
                    timeField = parser.text();
                    break;
                case INDICES_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser::getTokenLocation);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        indices.add(parser.text());
                    }
                    break;
                case UI_METADATA_FIELD:
                    uiMetadata = parser.map();
                    break;
                case SCHEMA_VERSION_FIELD:
                    schemaVersion = parser.intValue();
                    break;
                case FILTER_QUERY_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
                    try {
                        filterQuery = parseInnerQueryBuilder(parser);
                    } catch (IllegalArgumentException e) {
                        if (!e.getMessage().contains("empty clause")) {
                            throw e;
                        }
                    }
                    break;
                case DETECTION_INTERVAL_FIELD:
                    detectionInterval = TimeConfiguration.parse(parser);
                    break;
                case FEATURE_ATTRIBUTES_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser::getTokenLocation);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        features.add(Feature.parse(parser));
                    }
                    break;
                case WINDOW_DELAY_FIELD:
                    windowDelay = TimeConfiguration.parse(parser);
                    break;
                case LAST_UPDATE_TIME_FIELD:
                    lastUpdateTime = ParseUtils.toInstant(parser);
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return new AnomalyDetector(
            detectorId,
            version,
            name,
            description,
            timeField,
            indices,
            features,
            filterQuery,
            detectionInterval,
            windowDelay,
            uiMetadata,
            schemaVersion,
            lastUpdateTime
        );
    }

    public SearchSourceBuilder generateFeatureQuery() {
        SearchSourceBuilder generatedFeatureQuery = new SearchSourceBuilder().query(filterQuery);
        if (this.getFeatureAttributes() != null) {
            this.getFeatureAttributes().stream().forEach(feature -> generatedFeatureQuery.aggregation(feature.getAggregation()));
        }
        return generatedFeatureQuery;
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AnomalyDetector detector = (AnomalyDetector) o;
        return Objects.equal(getName(), detector.getName())
            && Objects.equal(getDescription(), detector.getDescription())
            && Objects.equal(getTimeField(), detector.getTimeField())
            && Objects.equal(getIndices(), detector.getIndices())
            && Objects.equal(getFeatureAttributes(), detector.getFeatureAttributes())
            && Objects.equal(getFilterQuery(), detector.getFilterQuery())
            && Objects.equal(getDetectionInterval(), detector.getDetectionInterval())
            && Objects.equal(getWindowDelay(), detector.getWindowDelay())
            && Objects.equal(getSchemaVersion(), detector.getSchemaVersion());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects
            .hashCode(
                detectorId,
                name,
                description,
                timeField,
                indices,
                featureAttributes,
                detectionInterval,
                windowDelay,
                uiMetadata,
                schemaVersion,
                lastUpdateTime
            );
    }

    public String getDetectorId() {
        return detectorId;
    }

    public Long getVersion() {
        return version;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getTimeField() {
        return timeField;
    }

    public List<String> getIndices() {
        return indices;
    }

    public List<Feature> getFeatureAttributes() {
        return featureAttributes;
    }

    public QueryBuilder getFilterQuery() {
        return filterQuery;
    }

    /**
     * Returns enabled feature ids in the same order in feature attributes.
     *
     * @return a list of filtered feature ids.
     */
    public List<String> getEnabledFeatureIds() {
        return featureAttributes.stream().filter(Feature::getEnabled).map(Feature::getId).collect(Collectors.toList());
    }

    public List<String> getEnabledFeatureNames() {
        return featureAttributes.stream().filter(Feature::getEnabled).map(Feature::getName).collect(Collectors.toList());
    }

    public TimeConfiguration getDetectionInterval() {
        return detectionInterval;
    }

    public TimeConfiguration getWindowDelay() {
        return windowDelay;
    }

    public Map<String, Object> getUiMetadata() {
        return uiMetadata;
    }

    public Integer getSchemaVersion() {
        return schemaVersion;
    }

    public Instant getLastUpdateTime() {
        return lastUpdateTime;
    }

}
