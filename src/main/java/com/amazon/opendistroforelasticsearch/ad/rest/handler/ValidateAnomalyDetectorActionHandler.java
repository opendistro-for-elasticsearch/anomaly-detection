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

package com.amazon.opendistroforelasticsearch.ad.rest.handler;

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;

import java.io.IOException;
import java.text.ParseException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.DateTimeRange;
import com.amazon.opendistroforelasticsearch.ad.model.Feature;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.model.TimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.model.ValidateResponse;
import com.amazon.opendistroforelasticsearch.ad.model.ValidationFailures;
import com.amazon.opendistroforelasticsearch.ad.model.ValidationSuggestedChanges;
import com.amazon.opendistroforelasticsearch.ad.util.ParseUtils;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;

/**
 * Anomaly detector REST action handler to process POST request.
 * POST request is for validating anomaly detector.
 */
public class ValidateAnomalyDetectorActionHandler extends AbstractActionHandler {

    protected static final String AGG_NAME_MAX = "max_timefield";
    protected static final int NUM_OF_INTERVAL_SAMPLES = 128;
    protected static final int MAX_NUM_OF_SAMPLES_VIEWED = 128;
    protected static final int NUM_OF_INTERVALS_CHECKED = 256;
    protected static final double SAMPLE_SUCCESS_RATE = 0.75;
    protected static final int FEATURE_VALIDATION_TIME_BACK_MINUTES = 10080;
    protected static final int NUM_OF_INTERVALS_CHECKED_FILTER = 384;
    protected static final long MAX_INTERVAL_LENGTH = (30L * 24 * 60 * 60 * 1000);
    protected static final long HISTORICAL_CHECK_IN_MS = (90L * 24 * 60 * 60 * 1000);
    protected static final String NAME_REGEX = "[a-zA-Z0-9._-]+";
    protected static final double INTERVAL_RECOMMENDATION_MULTIPLIER = 1.2;
    protected static final String[] numericType = { "long", "integer", "short", "double", "float" };

    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final AnomalyDetector anomalyDetector;
    private final AdminClient adminClient;

    private final Logger logger = LogManager.getLogger(IndexAnomalyDetectorActionHandler.class);
    private final Integer maxAnomalyDetectors;
    private final Integer maxAnomalyFeatures;
    private final TimeValue requestTimeout;
    private final NamedXContentRegistry xContent;

    private final ValidateResponse responseValidate;
    private final Map<String, List<String>> failuresMap;
    private final Map<String, List<String>> suggestedChangesMap;
    private final AtomicBoolean inferAgain;

    /**
     * Constructor function.
     *
     * @param client                  ES node client that executes actions on the local node
     * @param channel                 ES channel used to construct bytes / builder based outputs, and send responses
     * @param anomalyDetectionIndices anomaly detector index manager
     * @param anomalyDetector         anomaly detector instance
     */
    public ValidateAnomalyDetectorActionHandler(
        NodeClient client,
        RestChannel channel,
        AnomalyDetectionIndices anomalyDetectionIndices,
        AnomalyDetector anomalyDetector,
        Integer maxAnomalyDetectors,
        Integer maxAnomalyFeatures,
        TimeValue requestTimeout,
        NamedXContentRegistry xContentRegistry
    ) {
        super(client, channel);
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.anomalyDetector = anomalyDetector;
        this.maxAnomalyDetectors = maxAnomalyDetectors;
        this.maxAnomalyFeatures = maxAnomalyFeatures;
        this.requestTimeout = requestTimeout;
        this.responseValidate = new ValidateResponse();
        this.xContent = xContentRegistry;
        this.inferAgain = new AtomicBoolean(true);
        this.failuresMap = new HashMap<>();
        this.suggestedChangesMap = new HashMap<>();
        this.adminClient = client.admin();
    }

    /**
     * Start function to process validate anomaly detector request.
     * Checks if anomaly detector index exist first, if not, add it as a failure case.
     *
     * @throws IOException IOException from {@link AnomalyDetectionIndices#initAnomalyDetectorIndexIfAbsent(ActionListener)}
     */
    public void startValidation() throws IOException {
        boolean indexExists = anomalyDetectionIndices.doesAnomalyDetectorIndexExist();
        preDataValidationSteps(indexExists);
    }

    private void preDataValidationSteps(boolean indexExists) {
        List<String> missingFields = new ArrayList<>();
        List<String> formatErrors = new ArrayList<>();
        if (StringUtils.isBlank(anomalyDetector.getName())) {
            missingFields.add("name");
        } else if (!anomalyDetector.getName().matches(NAME_REGEX)) {
            formatErrors.add(anomalyDetector.getName());
            failuresMap.put(ValidationFailures.FORMAT.getName(), formatErrors);
        }
        if (anomalyDetector.getTimeField() == null) {
            missingFields.add("time_field");
        }
        if (anomalyDetector.getIndices() == null) {
            missingFields.add("indices");
        }
        if (anomalyDetector.getWindowDelay() == null) {
            missingFields.add("window_delay");
        }
        if (anomalyDetector.getDetectionInterval() == null) {
            missingFields.add("detector_interval");
        }
        if (anomalyDetector.getFeatureAttributes().isEmpty()) {
            missingFields.add("feature_attributes");
        }
        if (!missingFields.isEmpty()) {
            failuresMap.put(ValidationFailures.MISSING.getName(), missingFields);
        }
        String error = RestHandlerUtils.validateAnomalyDetector(anomalyDetector, maxAnomalyFeatures);
        if (StringUtils.isNotBlank(error)) {
            List<String> dupErrorsFeatures = new ArrayList<>();
            dupErrorsFeatures.addAll(Arrays.asList(error.split("\\r?\\n")));
            failuresMap.put(ValidationFailures.DUPLICATES.getName(), dupErrorsFeatures);
        }
        if (!failuresMap.isEmpty()) {
            sendAnomalyDetectorValidationResponse();
        } else if (indexExists) {
            validateNumberOfDetectors();
        } else {
            searchAdInputIndices(false);
        }
    }

    private void validateNumberOfDetectors() {
        try {
            QueryBuilder query = QueryBuilders.matchAllQuery();
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query).size(0).timeout(requestTimeout);
            SearchRequest searchRequest = new SearchRequest(ANOMALY_DETECTORS_INDEX).source(searchSourceBuilder);
            client.search(searchRequest, ActionListener.wrap(response -> onSearchAdResponse(response), exception -> onFailure(exception)));
        } catch (Exception e) {
            logger.error("Failed to create search request for validation", e);
            onFailure(e);
        }
    }

    private void onSearchAdResponse(SearchResponse response) {
        if (response.getHits().getTotalHits().value >= maxAnomalyDetectors) {
            suggestedChangesMap
                .computeIfAbsent(ValidationSuggestedChanges.OTHERS.getName(), k -> new ArrayList<>())
                .add("Can't create anomaly detector more than " + maxAnomalyDetectors + " ,please delete unused detectors");
        } else {
            searchAdInputIndices(true);
        }
    }

    private void searchAdInputIndices(boolean indexExists) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery())
            .size(0)
            .timeout(requestTimeout);
        SearchRequest searchRequest = new SearchRequest(anomalyDetector.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        client
            .search(
                searchRequest,
                ActionListener.wrap(searchResponse -> onSearchAdInputIndicesResponse(searchResponse, indexExists), exception -> {
                    onFailure(exception);
                    logger.error("Failed to create search request for validation", exception);
                })
            );
    }

    private void onSearchAdInputIndicesResponse(SearchResponse response, boolean indexExists) throws IOException {
        if (response.getHits().getTotalHits().value == 0) {
            String errorMsg = String
                .format("Can't create anomaly detector as no document found in indices: %s", anomalyDetector.getIndices());
            failuresMap.computeIfAbsent(ValidationFailures.OTHERS.getName(), k -> new ArrayList<>()).add(errorMsg);
            sendAnomalyDetectorValidationResponse();
        } else if (indexExists) {
            checkADNameExists();
        } else {
            checkForHistoricalData();
        }
    }

    private void checkADNameExists() {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(QueryBuilders.termQuery("name.keyword", anomalyDetector.getName()));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(boolQueryBuilder).timeout(requestTimeout);
        SearchRequest searchRequest = new SearchRequest(ANOMALY_DETECTORS_INDEX).source(searchSourceBuilder);
        client
            .search(
                searchRequest,
                ActionListener.wrap(searchResponse -> onSearchADNameResponse(searchResponse, anomalyDetector.getName()), exception -> {
                    onFailure(exception);
                    logger.error("Failed to create search request for validation", exception);
                })
            );
    }

    private void onSearchADNameResponse(SearchResponse response, String name) {
        if (response.getHits().getTotalHits().value > 0) {
            failuresMap.computeIfAbsent(ValidationFailures.DUPLICATES.getName(), k -> new ArrayList<>()).add(name);
            sendAnomalyDetectorValidationResponse();
        } else {
            checkForHistoricalData();
        }
    }

    public void checkForHistoricalData() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .aggregation(AggregationBuilders.max(AGG_NAME_MAX).field(anomalyDetector.getTimeField()))
            .size(1)
            .sort(new FieldSortBuilder(anomalyDetector.getTimeField()).order(SortOrder.DESC));
        SearchRequest searchRequest = new SearchRequest().indices(anomalyDetector.getIndices().get(0)).source(searchSourceBuilder);
        client.search(searchRequest, ActionListener.wrap(response -> checkIfAnyHistoricalData(getLatestDataTime(response)), exception -> {
            onFailure(exception);
            logger.error("Failed to create search request for validation", exception);
        }));
    }

    private void checkIfAnyHistoricalData(Optional<Long> lastTimeStamp) {
        if (lastTimeStamp.isPresent() && (Instant.now().toEpochMilli() - HISTORICAL_CHECK_IN_MS > lastTimeStamp.get())) {
            failuresMap
                .computeIfAbsent(ValidationFailures.OTHERS.getName(), k -> new ArrayList<>())
                .add("No historical data for past 3 months");
            sendAnomalyDetectorValidationResponse();
        } else {
            queryFilterValidation();
        }
    }

    private Long timeConfigToMilliSec(TimeConfiguration config) {
        return Optional.ofNullable((IntervalTimeConfiguration) config).map(t -> t.toDuration().toMillis()).orElse(0L);
    }

    private DateTimeRange startEndTimeRangeWithIntervals(int numOfIntervals) {
        long delayMillis = timeConfigToMilliSec(anomalyDetector.getWindowDelay());
        long detectorInterval = timeConfigToMilliSec(anomalyDetector.getDetectionInterval());
        return DateTimeRange.rangeBasedOfInterval(delayMillis, detectorInterval, numOfIntervals);
    }

    private void queryFilterValidation() {
        DateTimeRange timeRange = startEndTimeRangeWithIntervals(NUM_OF_INTERVALS_CHECKED_FILTER);
        long dataStartTime = timeRange.getStart();
        long dataEndTime = timeRange.getEnd();
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder(anomalyDetector.getTimeField())
            .from(dataStartTime)
            .to(dataEndTime)
            .format("epoch_millis");
        BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().must(rangeQuery).must(anomalyDetector.getFilterQuery());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .query(internalFilterQuery)
            .size(1)
            .terminateAfter(1)
            .timeout(requestTimeout);
        SearchRequest searchRequest = new SearchRequest(anomalyDetector.getIndices().get(0)).source(searchSourceBuilder);
        client.search(searchRequest, ActionListener.wrap(searchResponse -> onQueryFilterSearch(searchResponse), exception -> {
            onFailure(exception);
            logger.error("Failed to create data query search request for validation", exception);
        }));
    }

    private void onQueryFilterSearch(SearchResponse response) throws IOException, ParseException {
        if (response.getHits().getTotalHits().value <= 0) {
            List<String> filterError = new ArrayList<>();
            filterError
                .add(
                    "query filter is potentially wrong as no hits were found at all or no historical data in last "
                        + NUM_OF_INTERVALS_CHECKED_FILTER
                        + " intervals"
                );
            suggestedChangesMap.put(ValidationSuggestedChanges.FILTER_QUERY.getName(), filterError);
            sendAnomalyDetectorValidationResponse();
        } else {
            featureQueryValidation();
        }
    }

    private List<String> parseAggregationRequest(XContentParser parser) throws IOException {
        List<String> fieldNames = new ArrayList<>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                final String field = parser.currentName();
                switch (field) {
                    case "field":
                        parser.nextToken();
                        fieldNames.add(parser.textOrNull());
                        break;
                    default:
                        parser.skipChildren();
                        break;
                }
            }
        }
        return fieldNames;
    }

    private List<String> ForFieldMapping(XContentParser parser) throws IOException {
        List<String> fieldNameAggType = new ArrayList<>();
        XContentParser.Token token;
        String agg = "";
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                final String field = parser.currentName();
                switch (field) {
                    case "max":
                        agg = parser.currentName();
                        break;
                    case "avg":
                        agg = parser.currentName();
                        break;
                    case "sum":
                        agg = parser.currentName();
                        break;
                    case "field":
                        parser.nextToken();
                        fieldNameAggType.add(agg);
                        fieldNameAggType.add(parser.textOrNull());
                        break;
                    default:
                        parser.skipChildren();
                        break;
                }
            }
        }
        return fieldNameAggType;
    }

    private void checkFeatureAggregationType() throws IOException {
        Map<String, String> featureToField = new TreeMap<>();
        if (anomalyDetector.getFeatureAttributes() != null) {
            for (Feature feature : anomalyDetector.getFeatureAttributes()) {
                ParseUtils.parseAggregators(feature.getAggregation().toString(), xContent, feature.getId());
                XContentParser parser = XContentType.JSON
                    .xContent()
                    .createParser(xContent, LoggingDeprecationHandler.INSTANCE, feature.getAggregation().toString());
                parser.nextToken();
                List<String> aggTypeFieldName = ForFieldMapping(parser);
                featureToField.put(feature.getName(), aggTypeFieldName.get(1));
            }
        }
        getFieldMapping(featureToField);
    }

    private void getFieldMapping(Map<String, String> featureToAgg) {
        GetFieldMappingsRequest request = new GetFieldMappingsRequest().indices(anomalyDetector.getIndices().get(0));
        request.fields(featureToAgg.values().toArray(new String[0]));
        adminClient
            .indices()
            .getFieldMappings(
                request,
                ActionListener
                    .wrap(response -> checkFieldIndex(response, featureToAgg.values().toArray(new String[0]), featureToAgg), exception -> {
                        onFailure(exception);
                        logger.error("Failed to get field mapping for validation", exception);
                    })
            );
    }

    private void checkFieldIndex(GetFieldMappingsResponse response, String[] fields, Map<String, String> featuresToAgg) throws IOException {
        List<String> numericTypes = Arrays.asList(numericType);
        for (int i = 0; i < fields.length; i++) {
            Map<String, Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>>> mappings = response.mappings();
            final Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>> fieldMappings = mappings
                .get(anomalyDetector.getIndices().get(0));
            final GetFieldMappingsResponse.FieldMappingMetadata metadata = fieldMappings.get("_doc").get(fields[i]);
            final Map<String, Object> source = metadata.sourceAsMap();
            String fieldTypeJSON = source.get(fields[i]).toString();
            String fieldType = fieldTypeJSON.substring(fieldTypeJSON.lastIndexOf('=') + 1, fieldTypeJSON.length() - 1).trim();
            if (!numericTypes.contains(fieldType)) {
                failuresMap
                    .computeIfAbsent(ValidationFailures.FIELD_TYPE.getName(), k -> new ArrayList<>())
                    .add(
                        "Field named: "
                            + fields[i]
                            + " can't be aggregated due to it being of type "
                            + fieldType
                            + " which isn't numeric, please use a different aggregation type"
                    );
            }
        }
        if (!failuresMap.isEmpty()) {
            sendAnomalyDetectorValidationResponse();
        } else {
            intervalValidation();
        }
    }

    private DateTimeRange getFeatureQueryValidationDateRange() {
        long delayMillis = timeConfigToMilliSec(anomalyDetector.getWindowDelay());
        DateTimeRange timeRange = startEndTimeRangeWithIntervals(NUM_OF_INTERVALS_CHECKED_FILTER);
        IntervalTimeConfiguration searchRange = new IntervalTimeConfiguration(FEATURE_VALIDATION_TIME_BACK_MINUTES, ChronoUnit.MINUTES);
        long searchRangeTime = Optional.ofNullable(searchRange).map(t -> t.toDuration().toMillis()).orElse(0L);
        long startTimeWithSetTime = timeRange.getEnd() - (searchRangeTime - delayMillis);
        if (timeRange.getStart() > startTimeWithSetTime) {
            timeRange.setStart(startTimeWithSetTime);
        }
        return timeRange;
    }

    private void featureQueryValidation() throws IOException {
        DateTimeRange timeRange = getFeatureQueryValidationDateRange();
        AtomicInteger featureCounter = new AtomicInteger();
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder(anomalyDetector.getTimeField())
            .from(timeRange.getStart())
            .to(timeRange.getEnd())
            .format("epoch_millis")
            .includeLower(true)
            .includeUpper(false);
        if (anomalyDetector.getFeatureAttributes() != null) {
            for (Feature feature : anomalyDetector.getFeatureAttributes()) {
                ParseUtils.parseAggregators(feature.getAggregation().toString(), xContent, feature.getId());
                XContentParser parser = XContentType.JSON
                    .xContent()
                    .createParser(xContent, LoggingDeprecationHandler.INSTANCE, feature.getAggregation().toString());
                parser.nextToken();
                List<String> fieldNames = parseAggregationRequest(parser);
                BoolQueryBuilder boolQuery = QueryBuilders
                    .boolQuery()
                    .filter(rangeQuery)
                    .filter(anomalyDetector.getFilterQuery())
                    .filter(QueryBuilders.existsQuery(fieldNames.get(0)));
                SearchSourceBuilder internalSearchSourceBuilder = new SearchSourceBuilder().query(boolQuery).size(1).terminateAfter(1);
                SearchRequest searchRequest = new SearchRequest(anomalyDetector.getIndices().get(0)).source(internalSearchSourceBuilder);
                client.search(searchRequest, ActionListener.wrap(searchResponse -> {
                    featureCounter.incrementAndGet();
                    onFeatureAggregationValidation(searchResponse, feature, featureCounter);
                }, exception -> {
                    onFailure(exception);
                    logger.error("Failed to create feature search request for validation", exception);
                }));
            }
        }
    }

    private void onFeatureAggregationValidation(SearchResponse response, Feature feature, AtomicInteger counter) throws IOException {
        if (response.getHits().getTotalHits().value <= 0) {
            String errorMsg = feature.getName() + ": feature query is potentially wrong as no hits were found";
            suggestedChangesMap
                .computeIfAbsent(ValidationSuggestedChanges.FEATURE_ATTRIBUTES.getName(), k -> new ArrayList<>())
                .add(errorMsg);
        }
        if (counter.get() == anomalyDetector.getFeatureAttributes().size()) {
            if (!suggestedChangesMap.isEmpty()) {
                sendAnomalyDetectorValidationResponse();
            } else {
                checkFeatureAggregationType();
            }
        }
    }

    // creates a new 2D array of time ranges based of a different detector interval inorder to validate
    // detector interval with a new range every time. Creates 128 new interval time ranges
    private DateTimeRange[] createNewTimeRange(long detectorInterval) {
        DateTimeRange timeRanges[] = new DateTimeRange[MAX_NUM_OF_SAMPLES_VIEWED];
        long delayMillis = timeConfigToMilliSec(anomalyDetector.getWindowDelay());
        long dataEndTime = Instant.now().toEpochMilli() - delayMillis;
        long dataStartTime = dataEndTime - ((long) (MAX_NUM_OF_SAMPLES_VIEWED) * detectorInterval);
        for (int i = 0; i < MAX_NUM_OF_SAMPLES_VIEWED; i++) {
            long newStartTime = dataStartTime + (i * detectorInterval);
            long newEndTime = newStartTime + detectorInterval;
            timeRanges[i] = new DateTimeRange(newStartTime, newEndTime);
        }
        return timeRanges;
    }

    private synchronized void intervalValidation() {
        long detectorInterval = timeConfigToMilliSec(anomalyDetector.getDetectionInterval());
        for (long inferredDetectorInterval = detectorInterval; inferredDetectorInterval <= MAX_INTERVAL_LENGTH; inferredDetectorInterval *=
            INTERVAL_RECOMMENDATION_MULTIPLIER) {
            DateTimeRange timeRanges[] = createNewTimeRange(inferredDetectorInterval);
            try {
                if (inferAgain.get()) {
                    verifyWithInterval(timeRanges, inferredDetectorInterval);
                }
                wait();
            } catch (Exception ex) {
                onFailure(ex);
                logger.error(ex);
            }
        }
    }

    private List<String> getFieldNamesForFeature(Feature feature) throws IOException {
        ParseUtils.parseAggregators(feature.getAggregation().toString(), xContent, feature.getId());
        XContentParser parser = XContentType.JSON
            .xContent()
            .createParser(xContent, LoggingDeprecationHandler.INSTANCE, feature.getAggregation().toString());
        parser.nextToken();
        List<String> fieldNames = parseAggregationRequest(parser);
        return fieldNames;
    }

    private List<String> getFeatureFieldNames() throws IOException {
        List<String> featureFields = new ArrayList<>();
        for (Feature feature : anomalyDetector.getFeatureAttributes()) {
            featureFields.add(getFieldNamesForFeature(feature).get(0));
        }
        return featureFields;
    }

    private void verifyWithInterval(DateTimeRange[] timeRanges, long detectorInterval) throws IOException {
        inferAgain.set(false);
        List<String> featureFields = getFeatureFieldNames();
        MultiSearchRequest sr = new MultiSearchRequest();
        for (int i = 0; i < NUM_OF_INTERVAL_SAMPLES; i++) {
            long rangeStart = timeRanges[i].getStart();
            long rangeEnd = timeRanges[i].getEnd();
            RangeQueryBuilder rangeQuery = new RangeQueryBuilder(anomalyDetector.getTimeField())
                .from(rangeStart)
                .to(rangeEnd)
                .format("epoch_millis")
                .includeLower(true)
                .includeUpper(false);
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().filter(rangeQuery).filter(anomalyDetector.getFilterQuery());
            for (int j = 0; j < featureFields.size(); j++) {
                boolQuery.filter(QueryBuilders.existsQuery(featureFields.get(j)));
            }
            SearchSourceBuilder internalSearchSourceBuilder = new SearchSourceBuilder().query(boolQuery).size(1).terminateAfter(1);
            sr.add(new SearchRequest(anomalyDetector.getIndices().get(0)).source(internalSearchSourceBuilder));
        }
        client.multiSearch(sr, ActionListener.wrap(searchResponse -> {
            if (doneInferring(detectorInterval, searchResponse)) {
                checkWindowDelay();
            }
        }, exception -> {
            onFailure(exception);
            logger.error("Failed to create multi search request for validation", exception);
        }));
    }

    private synchronized boolean doneInferring(long detectorInterval, MultiSearchResponse searchResponse) {
        long originalInterval = Optional
            .ofNullable((IntervalTimeConfiguration) anomalyDetector.getDetectionInterval())
            .map(t -> t.toDuration().toMillis())
            .orElse(0L);
        final AtomicInteger hitCounter = new AtomicInteger();
        for (MultiSearchResponse.Item item : searchResponse) {
            SearchResponse response = item.getResponse();
            if (response.getHits().getTotalHits().value > 0) {
                hitCounter.incrementAndGet();
            }
        }
        inferAgain.set(true);
        notify();
        if (hitCounter.doubleValue() / (double) NUM_OF_INTERVAL_SAMPLES < SAMPLE_SUCCESS_RATE) {
            if ((detectorInterval * INTERVAL_RECOMMENDATION_MULTIPLIER) >= MAX_INTERVAL_LENGTH) {
                suggestedChangesMap
                    .computeIfAbsent(ValidationSuggestedChanges.DETECTION_INTERVAL.getName(), k -> new ArrayList<>())
                    .add("detector interval: failed to infer max up too: " + MAX_INTERVAL_LENGTH);
            } else {
                return false;
            }
        } else if (detectorInterval != originalInterval) {
            suggestedChangesMap
                .computeIfAbsent(ValidationSuggestedChanges.DETECTION_INTERVAL.getName(), k -> new ArrayList<>())
                .add(Long.toString(detectorInterval));
        }
        inferAgain.set(false);
        return true;
    }

    private void checkWindowDelay() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .aggregation(AggregationBuilders.max(AGG_NAME_MAX).field(anomalyDetector.getTimeField()))
            .size(1)
            .sort(new FieldSortBuilder(anomalyDetector.getTimeField()).order(SortOrder.DESC));
        SearchRequest searchRequest = new SearchRequest().indices(anomalyDetector.getIndices().get(0)).source(searchSourceBuilder);
        client.search(searchRequest, ActionListener.wrap(response -> checkDelayResponse(getLatestDataTime(response)), exception -> {
            onFailure(exception);
            logger.error("Failed to create search request for last data point", exception);
        }));
    }

    private Optional<Long> getLatestDataTime(SearchResponse searchResponse) {
        return Optional
            .ofNullable(searchResponse)
            .map(SearchResponse::getAggregations)
            .map(aggs -> aggs.asMap())
            .map(map -> (Max) map.get(AGG_NAME_MAX))
            .map(agg -> (long) agg.getValue());
    }

    private void checkDelayResponse(Optional<Long> lastTimeStamp) {
        long delayMillis = timeConfigToMilliSec(anomalyDetector.getWindowDelay());
        if (lastTimeStamp.isPresent() && (Instant.now().toEpochMilli() - lastTimeStamp.get() > delayMillis)) {
            long minutesSinceLastStamp = TimeUnit.MILLISECONDS.toMinutes(Instant.now().toEpochMilli() - lastTimeStamp.get());
            suggestedChangesMap
                .computeIfAbsent(ValidationSuggestedChanges.WINDOW_DELAY.getName(), k -> new ArrayList<>())
                .add(Long.toString(minutesSinceLastStamp));
        }
        sendAnomalyDetectorValidationResponse();
    }

    private void sendAnomalyDetectorValidationResponse() {
        this.responseValidate.setFailures(failuresMap);
        this.responseValidate.setSuggestedChanges(suggestedChangesMap);
        try {
            BytesRestResponse restResponse = new BytesRestResponse(RestStatus.OK, responseValidate.toXContent(channel.newBuilder()));
            channel.sendResponse(restResponse);
        } catch (Exception e) {
            channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
        }
    }
}
