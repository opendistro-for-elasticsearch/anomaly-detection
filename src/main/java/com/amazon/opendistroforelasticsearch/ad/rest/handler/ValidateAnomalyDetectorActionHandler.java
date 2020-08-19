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
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
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
import com.amazon.opendistroforelasticsearch.ad.model.Feature;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.model.ValidateResponse;
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
    protected static final long MAX_INTERVAL_LENGTH = 2592000000L;
    protected static final long HISTORICAL_CHECK_IN_MS = 7776000000L;
    protected static final String NAME_REGEX = "[a-zA-Z0-9._-]+";
    protected static final double INTERVAL_RECOMMENDATION_MULTIPLIER = 1.2;

    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final AnomalyDetector anomalyDetector;

    private final Logger logger = LogManager.getLogger(IndexAnomalyDetectorActionHandler.class);
    private final Integer maxAnomalyDetectors;
    private final Integer maxAnomalyFeatures;
    private final TimeValue requestTimeout;
    private final NamedXContentRegistry xContent;

    private ValidateResponse responseValidate;
    private Map<String, List<String>> failuresMap;
    private Map<String, List<String>> suggestedChangesMap;
    private Boolean inferringInterval;
    private AtomicBoolean inferAgain;

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
        this.inferringInterval = false;
        this.inferAgain = new AtomicBoolean(true);
        this.failuresMap = new HashMap<>();
        this.suggestedChangesMap = new HashMap<>();
    }

    /**
     * Start function to process validate anomaly detector request.
     * Checks if anomaly detector index exist first, if not, add it as a failure case.
     *
     * @throws IOException IOException from {@link AnomalyDetectionIndices#initAnomalyDetectorIndexIfAbsent(ActionListener)}
     */
    public void startValidation() throws IOException {
        if (!anomalyDetectionIndices.doesAnomalyDetectorIndexExist()) {
            anomalyDetectionIndices
                .initAnomalyDetectorIndex(
                    ActionListener.wrap(response -> onCreateMappingsResponse(response), exception -> onFailure(exception))
                );
        } else {
            preDataValidationSteps();
        }
    }

    public void preDataValidationSteps() {
        List<String> missingFields = new ArrayList<>();
        List<String> formatErrors = new ArrayList<>();
        if (anomalyDetector.getName() == null || anomalyDetector.getName() == "") {
            missingFields.add("name");
        } else if (!anomalyDetector.getName().matches(NAME_REGEX)) {
            formatErrors.add(anomalyDetector.getName());
            failuresMap.put("format", formatErrors);
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
            failuresMap.put("missing", missingFields);
        }
        String error = RestHandlerUtils.validateAnomalyDetector(anomalyDetector, maxAnomalyFeatures);
        if (StringUtils.isNotBlank(error)) {
            List<String> dupErrorsFeatures = new ArrayList<>();
            dupErrorsFeatures.addAll(Arrays.asList(error.split("\\r?\\n")));
            failuresMap.put("duplicates", dupErrorsFeatures);
        }
        if (!failuresMap.isEmpty()) {
            validateAnomalyDetectorResponse();
        } else {
            validateNumberOfDetectors();
        }
    }

    public void validateNumberOfDetectors() {
        try {
            QueryBuilder query = QueryBuilders.matchAllQuery();
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query).size(0).timeout(requestTimeout);
            SearchRequest searchRequest = new SearchRequest(ANOMALY_DETECTORS_INDEX).source(searchSourceBuilder);
            client.search(searchRequest, ActionListener.wrap(response -> onSearchAdResponse(response), exception -> onFailure(exception)));
        } catch (Exception e) {
            onFailure(e);
        }
    }

    private void onSearchAdResponse(SearchResponse response) throws IOException {
        if (response.getHits().getTotalHits().value >= maxAnomalyDetectors) {
            failuresMap
                .computeIfAbsent("others", k -> new ArrayList<>())
                .add("Can't create anomaly detector more than " + maxAnomalyDetectors);
            validateAnomalyDetectorResponse();
        } else {
            searchAdInputIndices(null);
        }
    }

    private void searchAdInputIndices(String detectorId) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery())
            .size(0)
            .timeout(requestTimeout);
        SearchRequest searchRequest = new SearchRequest(anomalyDetector.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        client
            .search(
                searchRequest,
                ActionListener
                    .wrap(searchResponse -> onSearchAdInputIndicesResponse(searchResponse, detectorId), exception -> onFailure(exception))
            );
    }

    private void onSearchAdInputIndicesResponse(SearchResponse response, String detectorId) throws IOException {
        if (response.getHits().getTotalHits().value == 0) {
            String errorMsg = "Can't create anomaly detector as no document found in indices: "
                + Arrays.toString(anomalyDetector.getIndices().toArray(new String[0]));
            failuresMap.computeIfAbsent("others", k -> new ArrayList<>()).add(errorMsg);
            validateAnomalyDetectorResponse();
        } else {
            checkADNameExists(detectorId);
        }
    }

    private void checkADNameExists(String detectorId) throws IOException {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(QueryBuilders.termQuery("name.keyword", anomalyDetector.getName()));
        if (StringUtils.isNotBlank(detectorId)) {
            boolQueryBuilder.mustNot(QueryBuilders.termQuery(RestHandlerUtils._ID, detectorId));
        }
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(boolQueryBuilder).timeout(requestTimeout);
        SearchRequest searchRequest = new SearchRequest(ANOMALY_DETECTORS_INDEX).source(searchSourceBuilder);
        client
            .search(
                searchRequest,
                ActionListener
                    .wrap(
                        searchResponse -> onSearchADNameResponse(searchResponse, anomalyDetector.getName()),
                        exception -> onFailure(exception)
                    )
            );
    }

    private void onSearchADNameResponse(SearchResponse response, String name) throws IOException {
        if (response.getHits().getTotalHits().value > 0) {
            failuresMap.computeIfAbsent("duplicates", k -> new ArrayList<>()).add(name);
            validateAnomalyDetectorResponse();
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
        client
            .search(
                searchRequest,
                ActionListener
                    .wrap(response -> checkIfAnyHistoricalData(getLatestDataTime(response)), exception -> { onFailure(exception); })
            );
    }

    private void checkIfAnyHistoricalData(Optional<Long> lastTimeStamp) {
        if (lastTimeStamp.isPresent() && (Instant.now().toEpochMilli() - HISTORICAL_CHECK_IN_MS > lastTimeStamp.get())) {
            failuresMap.computeIfAbsent("others", k -> new ArrayList<>()).add("No historical data for past 3 months");
            validateAnomalyDetectorResponse();
        } else {
            queryFilterValidation();
        }
    }

    private long[] startEndTimeRangeWithIntervals(int numOfIntervals) {
        long delayMillis = Optional
            .ofNullable((IntervalTimeConfiguration) anomalyDetector.getWindowDelay())
            .map(t -> t.toDuration().toMillis())
            .orElse(0L);
        long dataEndTime = Instant.now().toEpochMilli() - delayMillis;
        long detectorInterval = Optional
            .ofNullable((IntervalTimeConfiguration) anomalyDetector.getDetectionInterval())
            .map(t -> t.toDuration().toMillis())
            .orElse(0L);
        long dataStartTime = dataEndTime - ((long) (numOfIntervals) * detectorInterval);
        return new long[] { dataStartTime, dataEndTime };
    }

    private void queryFilterValidation() {
        long[] startEnd = startEndTimeRangeWithIntervals(NUM_OF_INTERVALS_CHECKED_FILTER);
        long dataEndTime = startEnd[1];
        long dataStartTime = startEnd[0];
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
        client
            .search(
                searchRequest,
                ActionListener.wrap(searchResponse -> onQueryFilterSearch(searchResponse), exception -> { onFailure(exception); })
            );
    }

    private void onQueryFilterSearch(SearchResponse response) throws IOException {
        if (response.getHits().getTotalHits().value <= 0) {
            List<String> filterError = new ArrayList<>();
            filterError
                .add(
                    "query filter is potentially wrong as no hits were found at all or no historical data in last "
                        + NUM_OF_INTERVALS_CHECKED_FILTER
                        + " intervals"
                );
            suggestedChangesMap.put("filter_query", filterError);
            validateAnomalyDetectorResponse();
        } else {
            long timestamp = (long) response.getHits().getHits()[0].getSourceAsMap().get("timestamp");
            featureQueryValidation(timestamp);
        }
    }

    private void featureQueryValidation(long startTimeStamp) throws IOException {
        long delayMillis = Optional
            .ofNullable((IntervalTimeConfiguration) anomalyDetector.getWindowDelay())
            .map(t -> t.toDuration().toMillis())
            .orElse(0L);
        long[] startEnd = startEndTimeRangeWithIntervals(NUM_OF_INTERVALS_CHECKED);
        long dataEndTime = startEnd[1];
        long dataStartTime = startEnd[0];
        if (startEnd[0] > startTimeStamp) {
            dataStartTime = startTimeStamp;
        }
        IntervalTimeConfiguration searchRange = new IntervalTimeConfiguration(FEATURE_VALIDATION_TIME_BACK_MINUTES, ChronoUnit.MINUTES);
        long searchRangeTime = Optional.ofNullable(searchRange).map(t -> t.toDuration().toMillis()).orElse(0L);
        long startTimeWithSetTime = startEnd[1] - (searchRangeTime - delayMillis);
        // Make sure start time includes timestamp seen by filter query check
        if (startEnd[0] > startTimeWithSetTime) {
            dataStartTime = startTimeWithSetTime;
        }
        AtomicInteger featureCounter = new AtomicInteger();
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder(anomalyDetector.getTimeField())
            .from(dataStartTime)
            .to(dataEndTime)
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
                BoolQueryBuilder boolQuery2 = QueryBuilders
                    .boolQuery()
                    .filter(rangeQuery)
                    .filter(anomalyDetector.getFilterQuery())
                    .filter(QueryBuilders.existsQuery(fieldNames.get(0)));
                SearchSourceBuilder internalSearchSourceBuilder = new SearchSourceBuilder().query(boolQuery2).size(1).terminateAfter(1);
                SearchRequest searchRequest = new SearchRequest(anomalyDetector.getIndices().get(0)).source(internalSearchSourceBuilder);
                client.search(searchRequest, ActionListener.wrap(searchResponse -> {
                    featureCounter.incrementAndGet();
                    onFeatureAggregationValidation(searchResponse, feature, featureCounter);
                }, exception -> onFailure(exception)));
            }
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

    private void onFeatureAggregationValidation(SearchResponse response, Feature feature, AtomicInteger counter) throws IOException {
        if (response.getHits().getTotalHits().value <= 0) {
            String errorMsg = feature.getName() + ": feature query is potentially wrong as no hits were found";
            suggestedChangesMap.computeIfAbsent("feature_attributes", k -> new ArrayList<>()).add(errorMsg);
        }
        if (counter.get() == anomalyDetector.getFeatureAttributes().size()) {
            if (!suggestedChangesMap.isEmpty()) {
                validateAnomalyDetectorResponse();
            } else {
                intervalValidation();
            }
        }
    }

    private long[][] createNewTimeRange(long detectorInterval) {
        long timeRanges[][] = new long[MAX_NUM_OF_SAMPLES_VIEWED][2];
        long delayMillis = Optional
            .ofNullable((IntervalTimeConfiguration) anomalyDetector.getWindowDelay())
            .map(t -> t.toDuration().toMillis())
            .orElse(0L);
        long dataEndTime = Instant.now().toEpochMilli() - delayMillis;
        long dataStartTime = dataEndTime - ((long) (MAX_NUM_OF_SAMPLES_VIEWED) * detectorInterval);
        for (int j = 0; j < MAX_NUM_OF_SAMPLES_VIEWED; j++) {
            timeRanges[j][0] = dataStartTime + (j * detectorInterval);
            timeRanges[j][1] = timeRanges[j][0] + detectorInterval;
        }
        return timeRanges;
    }

    private synchronized void intervalValidation() throws IOException {
        long detectorInterval = Optional
            .ofNullable((IntervalTimeConfiguration) anomalyDetector.getDetectionInterval())
            .map(t -> t.toDuration().toMillis())
            .orElse(0L);
        for (long i = detectorInterval; i <= MAX_INTERVAL_LENGTH; i *= INTERVAL_RECOMMENDATION_MULTIPLIER) {
            long timeRanges[][] = createNewTimeRange(i);
            // Need to try and check if the infering logic is done before calling on the method again
            // with a new interval since otherwise the requests get mixed up
            try {
                if (inferAgain.get()) {
                    samplingHelper(timeRanges, i);
                }
                wait();
            } catch (Exception ex) {
                onFailure(ex);
            }
        }
    }

    private List<String> getFeatureFieldNames() throws IOException {
        List<String> featureFields = new ArrayList<>();
        for (Feature feature : anomalyDetector.getFeatureAttributes()) {
            ParseUtils.parseAggregators(feature.getAggregation().toString(), xContent, feature.getId());
            XContentParser parser = XContentType.JSON
                .xContent()
                .createParser(xContent, LoggingDeprecationHandler.INSTANCE, feature.getAggregation().toString());
            parser.nextToken();
            List<String> fieldNames = parseAggregationRequest(parser);
            featureFields.add(fieldNames.get(0));
        }
        return featureFields;
    }

    private synchronized void samplingHelper(long[][] timeRanges, long detectorInterval) throws IOException {
        inferAgain.set(false);
        List<String> featureFields = getFeatureFieldNames();
        MultiSearchRequest sr = new MultiSearchRequest();
        for (int i = 0; i < NUM_OF_INTERVAL_SAMPLES; i++) {
            long rangeStart = timeRanges[i][0];
            long rangeEnd = timeRanges[i][1];
            RangeQueryBuilder rangeQueryRandom = new RangeQueryBuilder(anomalyDetector.getTimeField())
                .from(rangeStart)
                .to(rangeEnd)
                .format("epoch_millis")
                .includeLower(true)
                .includeUpper(false);
            BoolQueryBuilder qb = QueryBuilders.boolQuery().filter(rangeQueryRandom).filter(anomalyDetector.getFilterQuery());
            for (int j = 0; j < featureFields.size(); j++) {
                qb.filter(QueryBuilders.existsQuery(featureFields.get(j)));
            }
            SearchSourceBuilder internalSearchSourceBuilder = new SearchSourceBuilder().query(qb).size(1).terminateAfter(1);
            sr.add(new SearchRequest(anomalyDetector.getIndices().get(0)).source(internalSearchSourceBuilder));
        }
        client.multiSearch(sr, ActionListener.wrap(searchResponse -> {
            if (doneInferring(detectorInterval, searchResponse)) {
                checkWindowDelay();
            }
        }, exception -> onFailure(exception)));
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
                    .computeIfAbsent("detection_interval", k -> new ArrayList<>())
                    .add("detector interval: failed to infer max up too: " + MAX_INTERVAL_LENGTH);
            } else {
                return false;
            }
        } else if (detectorInterval != originalInterval) {
            suggestedChangesMap.computeIfAbsent("detection_interval", k -> new ArrayList<>()).add(Long.toString(detectorInterval));
            inferAgain.set(false);
            return true;
        }
        inferAgain.set(false);
        return true;
    }

    private void checkWindowDelay() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .aggregation(AggregationBuilders.max(AGG_NAME_MAX).field(anomalyDetector.getTimeField()))
            .size(1)
            .sort(new FieldSortBuilder("timestamp").order(SortOrder.DESC));
        SearchRequest searchRequest = new SearchRequest().indices(anomalyDetector.getIndices().get(0)).source(searchSourceBuilder);
        client
            .search(
                searchRequest,
                ActionListener.wrap(response -> checkDelayResponse(getLatestDataTime(response)), exception -> onFailure(exception))
            );
    }

    private Optional<Long> getLatestDataTime(SearchResponse searchResponse) {
        Optional<Long> x = Optional
            .ofNullable(searchResponse)
            .map(SearchResponse::getAggregations)
            .map(aggs -> aggs.asMap())
            .map(map -> (Max) map.get(AGG_NAME_MAX))
            .map(agg -> (long) agg.getValue());
        return x;
    }

    private void checkDelayResponse(Optional<Long> lastTimeStamp) {
        long delayMillis = Optional
            .ofNullable((IntervalTimeConfiguration) anomalyDetector.getWindowDelay())
            .map(t -> t.toDuration().toMillis())
            .orElse(0L);
        if (lastTimeStamp.isPresent() && (Instant.now().toEpochMilli() - lastTimeStamp.get() > delayMillis)) {
            long minutesSinceLastStamp = TimeUnit.MILLISECONDS.toMinutes(Instant.now().toEpochMilli() - lastTimeStamp.get());
            suggestedChangesMap.computeIfAbsent("window_delay", k -> new ArrayList<>()).add(Long.toString(minutesSinceLastStamp));
        }
        validateAnomalyDetectorResponse();
    }

    private void validateAnomalyDetectorResponse() {
        this.responseValidate.setFailures(failuresMap);
        this.responseValidate.setSuggestedChanges(suggestedChangesMap);
        try {
            BytesRestResponse restResponse = new BytesRestResponse(RestStatus.OK, responseValidate.toXContent(channel.newBuilder()));
            channel.sendResponse(restResponse);
        } catch (Exception e) {
            channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
        }
    }

    private void onCreateMappingsResponse(CreateIndexResponse response) throws IOException {
        if (response.isAcknowledged()) {
            logger.info("Created {} with mappings.", ANOMALY_DETECTORS_INDEX);
            preDataValidationSteps();
        } else {
            logger.warn("Created {} with mappings call not acknowledged.", ANOMALY_DETECTORS_INDEX);
            channel
                .sendResponse(
                    new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, response.toXContent(channel.newErrorBuilder(), EMPTY_PARAMS))
                );
        }
    }
}
