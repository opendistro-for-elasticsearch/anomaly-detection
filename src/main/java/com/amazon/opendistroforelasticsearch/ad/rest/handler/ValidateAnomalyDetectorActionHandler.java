package com.amazon.opendistroforelasticsearch.ad.rest.handler;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.*;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.util.MultiResponsesDelegateActionListener;
import com.amazon.opendistroforelasticsearch.ad.util.ParseUtils;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.SamplerAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.Percentile;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;


import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.*;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateRange;
import static org.elasticsearch.search.aggregations.AggregatorFactories.VALID_AGG_NAME;

/**
 * Anomaly detector REST action handler to process POST request.
 * POST request is for validating anomaly detector.
 */
public class ValidateAnomalyDetectorActionHandler extends AbstractActionHandler {

    protected static final String AGG_NAME_MAX = "max_timefield";
    protected static final int NUM_OF_RANDOM_SAMPLES = 128;
    protected static final int MAX_NUM_OF_SAMPLES_VIEWED = 128;
    protected static final int NUM_OF_INTERVALS_CHECKED = 256;
    protected static final double SAMPLE_SUCCESS_RATE = 0.75;
    protected static final int RANDOM_SAMPLING_REPEATS = 12;
    protected static final int FEATURE_VALIDATION_TIME_BACK_MINUTES = 10080;
    protected static final long MAX_INTERVAL_LENGTH = 86400000;
    private final AdminClient adminClient;


    public static final String SUGGESTED_CHANGES = "suggested_changes";
    public static final String FAILURES = "failures";

    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final String detectorId;
    private final AnomalyDetector anomalyDetector;

    private final Logger logger = LogManager.getLogger(IndexAnomalyDetectorActionHandler.class);
    private final Integer maxAnomalyDetectors;
    private final Integer maxAnomalyFeatures;
    private final TimeValue requestTimeout;
    private final NamedXContentRegistry xContent;

    private ValidateResponse responseValidate;
    private final Map<String, MultiSearchResponse> savedResponsesToFeature;
    private final List<MultiSearchResponse> savedMultiResponses;
    private final Map<String, Boolean> featureIntervalValidation;
    private final Map<String, Long> featureValidTimerecommendation;
    private final List<String> failures;
    private final List<String> suggestedChanges;
    private Boolean inferringInterval;
    private AtomicBoolean inferAgain;


    /**
     * Constructor function.
     *
     * @param settings                ES settings
     * @param client                  ES node client that executes actions on the local node
     * @param channel                 ES channel used to construct bytes / builder based outputs, and send responses
     * @param anomalyDetectionIndices anomaly detector index manager
     * @param detectorId              detector identifier
     * @param anomalyDetector         anomaly detector instance
     */
    public ValidateAnomalyDetectorActionHandler(
            Settings settings,
            NodeClient client,
            RestChannel channel,
            AnomalyDetectionIndices anomalyDetectionIndices,
            String detectorId,
            AnomalyDetector anomalyDetector,
            Integer maxAnomalyDetectors,
            Integer maxAnomalyFeatures,
            TimeValue requestTimeout,
            NamedXContentRegistry xContentRegistry
    ) {
        super(client, channel);
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.detectorId = detectorId;
        this.anomalyDetector = anomalyDetector;
        this.maxAnomalyDetectors = maxAnomalyDetectors;
        this.maxAnomalyFeatures = maxAnomalyFeatures;
        this.requestTimeout = requestTimeout;
        this.failures = new ArrayList<>();
        this.suggestedChanges = new ArrayList<>();
        this.responseValidate = new ValidateResponse();
        this.xContent = xContentRegistry;
        this.adminClient = client.admin();
        this.featureIntervalValidation = new HashMap<>();
        this.savedMultiResponses = Collections.synchronizedList(new ArrayList<>());
        this.savedResponsesToFeature = new HashMap<>();
        this.featureValidTimerecommendation = new HashMap<>();
        this.inferringInterval = false;
        this.inferAgain = new AtomicBoolean(true);
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

        if (anomalyDetector.getName() == null) {
            failures.add("name missing");
        }
        if (anomalyDetector.getTimeField() == null) {
            failures.add("time-field missing");
        }
        if (anomalyDetector.getIndices() == null) {
            failures.add("data source indices missing");
        }
        if (anomalyDetector.getWindowDelay() == null) {
            failures.add("window-delay missing");
        }
        if (anomalyDetector.getDetectionInterval() == null) {
            failures.add("detector-interval missing");
        }
        if (anomalyDetector.getFeatureAttributes().isEmpty()) {
            failures.add("feature is missing");
        }

        String error = RestHandlerUtils.validateAnomalyDetector(anomalyDetector, maxAnomalyFeatures);
        if (StringUtils.isNotBlank(error)) {
            failures.add(error);
        }
        if (!failures.isEmpty()) {
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
            String errorMsg = "Can't create anomaly detector more than " + maxAnomalyDetectors;
            logger.error(errorMsg);
            failures.add(errorMsg);
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
            logger.error(errorMsg);
            failures.add(errorMsg);
            return;
        }
        checkADNameExists(detectorId);
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
                                        searchResponse -> onSearchADNameResponse(searchResponse, detectorId, anomalyDetector.getName()),

                                        exception -> onFailure(exception)
                                )
                );
    }

    private void onSearchADNameResponse(SearchResponse response, String detectorId, String name) throws IOException {
        if (response.getHits().getTotalHits().value > 0) {
            String errorMsg = String
                    .format(
                            "Cannot create anomaly detector with name [%s] as it's already used by detector %s",
                            name,
                            Arrays.stream(response.getHits().getHits()).map(hit -> hit.getId()).collect(Collectors.toList())
                    );
            logger.warn(errorMsg);
            failures.add(errorMsg);
            validateAnomalyDetectorResponse();
        } else if (anomalyDetector.getFilterQuery() != null) {
            queryFilterValidation();
        } else {
            featureQueryValidation();
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
        long dataStartTime = dataEndTime - ((long) (numOfIntervals) * detectorInterval - delayMillis);

        return new long[]{dataStartTime, dataEndTime};
    }

    private void queryFilterValidation() {
        long[] startEnd = startEndTimeRangeWithIntervals(NUM_OF_INTERVALS_CHECKED);
        long dataEndTime = startEnd[1];
        long dataStartTime = startEnd[0];
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder(anomalyDetector.getTimeField())
                .from(dataStartTime)
                .to(dataEndTime)
                .format("epoch_millis");
        BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().must(rangeQuery).must(anomalyDetector.getFilterQuery());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(internalFilterQuery).size(1).terminateAfter(1).timeout(requestTimeout);
        SearchRequest searchRequest = new SearchRequest(anomalyDetector.getIndices().get(0)).source(searchSourceBuilder);
       // System.out.println("Query filter request: " + searchRequest.toString());
        client
                .search(
                        searchRequest,
                        ActionListener
                                .wrap(
                                        searchResponse -> onQueryFilterSearch(searchResponse),
                                        exception -> {
                                            System.out.println("queryfilter exception: " + exception.getMessage());
                                            onFailure(exception);
                                        }
                                )
                );
    }

    private void onQueryFilterSearch(SearchResponse response) throws IOException {
        if (response.getHits().getTotalHits().value <= 0) {
            String errorMsg = "query filter is potentially wrong as no hits were found at all. ";
            logger.warn(errorMsg);
            suggestedChanges.add(errorMsg);
            validateAnomalyDetectorResponse();
        } else {
            featureQueryValidation();
            //getFieldMapping();
        }
    }

    private void featureQueryValidation() throws IOException {
        long delayMillis = Optional
                .ofNullable((IntervalTimeConfiguration) anomalyDetector.getWindowDelay())
                .map(t -> t.toDuration().toMillis())
                .orElse(0L);
        long[] startEnd = startEndTimeRangeWithIntervals(NUM_OF_INTERVALS_CHECKED);
        long dataEndTime = startEnd[1];
        long dataStartTime = startEnd[0];
        IntervalTimeConfiguration searchRange = new IntervalTimeConfiguration(FEATURE_VALIDATION_TIME_BACK_MINUTES, ChronoUnit.MINUTES);
        long searchRangeTime = Optional
                .ofNullable(searchRange)
                .map(t -> t.toDuration().toMillis())
                .orElse(0L);
        long startTimeWithSetTime = startEnd[1] - (searchRangeTime - delayMillis);
        if (startEnd[0] > startTimeWithSetTime) {
            dataStartTime = startTimeWithSetTime;
        }

        RangeQueryBuilder rangeQuery = new RangeQueryBuilder(anomalyDetector.getTimeField())
                .from(dataStartTime)
                .to(dataEndTime)
                .format("epoch_millis")
                .includeLower(true)
                .includeUpper(false);
        if (anomalyDetector.getFeatureAttributes() != null) {
            for (Feature feature : anomalyDetector.getFeatureAttributes()) {
                ParseUtils.parseAggregators(feature.getAggregation().toString(), xContent, feature.getId());
                XContentParser parser = XContentType.JSON.xContent().createParser(xContent, LoggingDeprecationHandler.INSTANCE, feature.getAggregation().toString());
                parser.nextToken();
                List<String> fieldNames = parseAggregationRequest(parser, 0, feature.getAggregation().getName());
//                BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().must(rangeQuery).must(anomalyDetector.getFilterQuery())
//                        .must(QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery(fieldNames.get(0))));
                BoolQueryBuilder boolQuery2 = QueryBuilders.boolQuery().filter(rangeQuery).filter(anomalyDetector.getFilterQuery())
                        .filter(QueryBuilders.existsQuery(fieldNames.get(0)));
                SearchSourceBuilder internalSearchSourceBuilder = new SearchSourceBuilder().query(boolQuery2).size(1).terminateAfter(1);
                SearchRequest searchRequest = new SearchRequest(anomalyDetector.getIndices().get(0)).source(internalSearchSourceBuilder);
                //System.out.println("search builder for each feature query: " + searchRequest.toString());
                client
                        .search(
                                searchRequest,
                                ActionListener
                                        .wrap(
                                                searchResponse -> onFeatureAggregationValidation(searchResponse, feature),
                                                exception -> {
                                                    System.out.println(exception.getMessage());
                                                    onFailure(exception);
                                                }
                                        )
                        );
            }
            if (!suggestedChanges.isEmpty()) {
                validateAnomalyDetectorResponse();
                return;
            }
            System.out.println("went into here");
            randomSamplingIntervalValidation();
        }
    }

    private List<String> parseAggregationRequest(XContentParser parser, int level, String aggName) throws IOException {
        List<String> fieldNames = new ArrayList<>();
        XContentParser.Token token = null;
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


    private void onFeatureAggregationValidation(SearchResponse response, Feature feature) throws IOException {
       // System.out.println("response for each feature agg validation " + response.toString());
        if (response.getHits().getTotalHits().value <= 0) {
            String errorMsg = "feature query is potentially wrong as no hits were found at all for feature " + feature.getName();
            logger.warn(errorMsg);
            suggestedChanges.add(errorMsg);
        }
    }


//    private void onFeatureAggregationValidation(SearchResponse response, Feature feature) throws IOException {
//        //System.out.println("response for each feature agg validation " + response.toString());
//        Optional<Double> aggValue = Optional
//                .ofNullable(response)
//                .map(SearchResponse::getAggregations)
//                .map(aggs -> aggs.asMap())
//                .map(map -> map.get(feature.getId()))
//                .map(this::parseAggregation);
//        if (Double.isNaN(aggValue.get()) || Double.isInfinite(aggValue.get())) {
//            String errorMsg = "feature query is potentially wrong as no hits were found at all for feature " + feature.getName();
//            logger.warn(errorMsg);
//            suggestedChanges.add(errorMsg);
//        }
//    }

    private synchronized void randomSamplingIntervalValidation() throws IOException {
        long detectorInterval = Optional
                .ofNullable((IntervalTimeConfiguration) anomalyDetector.getDetectionInterval())
                .map(t -> t.toDuration().toMillis())
                .orElse(0L);
        //CountDownLatch latch = new CountDownLatch(Math.sqrt((double) MAX_INTERVAL_LENGTH / detectorInterval));

        for (long i = detectorInterval; i < MAX_INTERVAL_LENGTH; i *= 1.5) {
            if (inferringInterval) {
                break;
            }
            detectorInterval = i;
            long timeRanges[][] = new long[MAX_NUM_OF_SAMPLES_VIEWED][2];
            long delayMillis = Optional
                    .ofNullable((IntervalTimeConfiguration) anomalyDetector.getWindowDelay())
                    .map(t -> t.toDuration().toMillis())
                    .orElse(0L);
            long dataEndTime = Instant.now().toEpochMilli() - delayMillis;
            long dataStartTime = dataEndTime - ((long) (MAX_NUM_OF_SAMPLES_VIEWED) * detectorInterval - delayMillis);
            for (int j = 0; j < MAX_NUM_OF_SAMPLES_VIEWED; j++) {
                timeRanges[j][0] = dataStartTime + (j * detectorInterval);
                timeRanges[j][1] = timeRanges[j][0] + detectorInterval;
            }
            AtomicInteger listenerCounter = new AtomicInteger();
            try {
                if (inferAgain.get()) {
                    randomSamplingHelper(timeRanges, listenerCounter, i);
                }
                wait();
            } catch (Exception ex) {

            }

            System.out.println("value of i inside loop: " + i);
        }
    }

        private synchronized void randomSamplingHelper(long[][] timeRanges, AtomicInteger listenerCounter, long detectorInterval) throws IOException {
        inferAgain.set(false);
        List<String> featureFields = new ArrayList<>();
            for (Feature feature : anomalyDetector.getFeatureAttributes()) {
                ParseUtils.parseAggregators(feature.getAggregation().toString(), xContent, feature.getId());
                XContentParser parser = XContentType.JSON.xContent().createParser(xContent, LoggingDeprecationHandler.INSTANCE, feature.getAggregation().toString());
                parser.nextToken();
                List<String> fieldNames = parseAggregationRequest(parser, 0, feature.getAggregation().getName());
                featureFields.add(fieldNames.get(0));
            }
            System.out.println("featureFields: " + featureFields);
        MultiSearchRequest sr = new MultiSearchRequest();
        for (int i = 0; i < NUM_OF_RANDOM_SAMPLES; i++) {
            long rangeStart = timeRanges[i][0];
            long rangeEnd = timeRanges[i][1];
            RangeQueryBuilder rangeQueryRandom = new RangeQueryBuilder(anomalyDetector.getTimeField())
                    .from(rangeStart)
                    .to(rangeEnd)
                    .format("epoch_millis")
                    .includeLower(true)
                    .includeUpper(false);
            BoolQueryBuilder qb = QueryBuilders.boolQuery().filter(rangeQueryRandom).filter(anomalyDetector.getFilterQuery());
            for (int j = 0 ; j < featureFields.size(); j++) {
                qb.filter(QueryBuilders.existsQuery(featureFields.get(j) ) ) ;
            }
//            BoolQueryBuilder boolQuery2 = QueryBuilders.boolQuery().filter(rangeQueryRandom).filter(anomalyDetector.getFilterQuery())
//                    .filter(QueryBuilders.existsQuery(fieldNames.get(0)));
            SearchSourceBuilder internalSearchSourceBuilder = new SearchSourceBuilder().query(qb).size(1).terminateAfter(1);
            SearchRequest searchRequest = new SearchRequest(anomalyDetector.getIndices().get(0)).source(internalSearchSourceBuilder);
            if (i == 0) {
                System.out.println("search request: " + searchRequest);
            }
            sr.add(searchRequest);
        }
       // System.out.println("8 requests: " + sr.requests().toString());

            client
                .multiSearch(
                        sr,
                        ActionListener
                                .wrap(
                                        searchResponse -> {
//                                            savedMultiResponses.add(searchResponse);
//                                            listenerCounter.incrementAndGet();

                                            if (doneInferring(detectorInterval, searchResponse)) {
                                                onRandomGetMultiResponse();
                                            }
                                             },
                                        exception -> {
                                            System.out.println(exception.getMessage());
                                            onFailure(exception);
                                        }
                                )
                );
    }
    private synchronized boolean doneInferring(long detectorInterval, MultiSearchResponse searchResponse){
        System.out.println("number of responses in multiresponse: " + searchResponse.getResponses().length);
        System.out.println("detector interval: " + detectorInterval);
        boolean firstCase = false;
        long originalInterval = Optional
                .ofNullable((IntervalTimeConfiguration) anomalyDetector.getDetectionInterval())
                .map(t -> t.toDuration().toMillis())
                .orElse(0L);
        if (detectorInterval == originalInterval) {
            System.out.println("went into first case");
            firstCase = true;
        }
        if (detectorInterval >= MAX_INTERVAL_LENGTH) {
            suggestedChanges.add("detector interval: failed to infer max up too: " + MAX_INTERVAL_LENGTH);
            System.out.println("went into max check");
            inferAgain.set(false);
            return true;
        }
            String errorMsg = "";
            final AtomicInteger hitCounter = new AtomicInteger();
            for (MultiSearchResponse.Item item : searchResponse) {
                SearchResponse response = item.getResponse();
                if (response.getHits().getTotalHits().value > 0) {
                    hitCounter.incrementAndGet();
                }
            }
            inferAgain.set(true);
            notify();
            System.out.println("Hit counter before last validation: " + hitCounter.get());
            System.out.println("successRate: " + hitCounter.doubleValue() / (double) NUM_OF_RANDOM_SAMPLES);
            if (hitCounter.doubleValue() / (double) NUM_OF_RANDOM_SAMPLES < SAMPLE_SUCCESS_RATE) {
                return false;
            } else if (!firstCase){
                String suggestion = "detector interval: " + detectorInterval;
                suggestedChanges.add(suggestion);
                inferAgain.set(false);
                return true;
            }
        return true;
    }

    private void onRandomGetMultiResponse() {
        checkWindowDelay();
    }


//        private void onRandomGetMultiResponse(MultiSearchResponse multiSearchResponse, Feature feature, AtomicInteger listnerCounter, long detectorInterval) {
//            boolean firstCase = false;
//            long originalInterval = Optional
//                    .ofNullable((IntervalTimeConfiguration) anomalyDetector.getDetectionInterval())
//                    .map(t -> t.toDuration().toMillis())
//                    .orElse(0L);
//            if (detectorInterval == originalInterval) {
//                System.out.println("went into first case");
//                firstCase = true;
//            }
//            for (String f: savedResponsesToFeature.keySet()) {
//                String errorMsg = "";
//                final AtomicInteger hitCounter = new AtomicInteger();
//                for (MultiSearchResponse.Item item : savedResponsesToFeature.get(f)) {
//                    SearchResponse response = item.getResponse();
//                    if (response.getHits().getTotalHits().value > 0) {
//                        hitCounter.incrementAndGet();
//                    }
//                }
//                System.out.println("Hit counter before last validation: " + hitCounter.get());
//                System.out.println("successRate: " + hitCounter.doubleValue() / (double) NUM_OF_RANDOM_SAMPLES);
//                if (hitCounter.doubleValue() / (double) NUM_OF_RANDOM_SAMPLES < SAMPLE_SUCCESS_RATE) {
//                    featureIntervalValidation.put(f, false);
////                    errorMsg += "data is too sparse with this interval for feature: " + f;
////                    logger.warn(errorMsg);
////                    suggestedChanges.add(errorMsg);
//
////
//                } else {
//                    if (!firstCase) {
//                        featureValidTimerecommendation.put(f, detectorInterval);
//                    }
//                    featureIntervalValidation.put(f, true);
//                }
//            }
//            System.out.println("1: " + featureIntervalValidation);
//
//            if (!featureIntervalValidation.containsValue((false)) && firstCase && !inferringInterval) {
//                inferringInterval = true;
//                System.out.println("BEFORE WINDOW DELAY CALL 1");
//                System.out.println(featureIntervalValidation);
//                checkWindowDelay();
//            } else if (!featureIntervalValidation.containsValue((false)) && !inferringInterval) {
//                for (String featureName : featureValidTimerecommendation.keySet()) {
//                        String suggestion = featureName + ": " + featureValidTimerecommendation.get(featureName).toString();
//                        suggestedChanges.add(suggestion);
//                }
//                inferringInterval = true;
//                System.out.println("BEFORE WINDOW DELAY CALL 2");
//                System.out.println(featureIntervalValidation);
//                checkWindowDelay();
//            } else if (detectorInterval >= MAX_INTERVAL_LENGTH && featureIntervalValidation.containsValue(false) && !inferringInterval) {
//                for (String featureName : featureIntervalValidation.keySet()) {
//                    if (!featureIntervalValidation.get(featureName)) {
//                        String doneInferring = "failed to infer max up too: " + MAX_INTERVAL_LENGTH + "for feature: " + featureName;
//                        suggestedChanges.add(doneInferring);
//                    } else {
//                        for (String featureNameRecc : featureValidTimerecommendation.keySet()) {
//                            String suggestion = featureName + ": " + featureValidTimerecommendation.get(featureNameRecc).toString();
//                            suggestedChanges.add(suggestion);
//                        }
//                    }
//                }
//                inferringInterval = true;
//                System.out.println("BEFORE WINDOW DELAY CALL 3");
//                checkWindowDelay();
//            } else if (detectorInterval >= MAX_INTERVAL_LENGTH && !inferringInterval) {
//                inferringInterval = true;
//                System.out.println("BEFORE WINDOW DELAY CALL 34");
//                checkWindowDelay();
//            } else {
//                System.out.println("hello");
//                System.out.println(featureIntervalValidation);
//            }
//        }

    //long detectorInterval = intervalTime * 2;

////            long[][] timeRanges = new long[MAX_NUM_OF_SAMPLES_VIEWED][2];
////            long delayMillis = Optional
////                    .ofNullable((IntervalTimeConfiguration) anomalyDetector.getWindowDelay())
////                    .map(t -> t.toDuration().toMillis())
////                    .orElse(0L);
////            long dataEndTime = Instant.now().toEpochMilli() - delayMillis;
////            long dataStartTime = dataEndTime - ((long) (MAX_NUM_OF_SAMPLES_VIEWED) * detectorInterval - delayMillis);
////            for (int j = 0; j < MAX_NUM_OF_SAMPLES_VIEWED; j++) {
////                timeRanges[j][0] = dataStartTime + (j * detectorInterval);
////                timeRanges[j][1] = timeRanges[j][0] + detectorInterval;
////            }

//            boolean valid = false;
//            for (long i = detectorInterval; i < MAX_INTERVAL_LENGTH; i*=2) {
//                detectorInterval = i;
//                timeRanges = new long[MAX_NUM_OF_SAMPLES_VIEWED][2];
//                long delayMillis = Optional
//                        .ofNullable((IntervalTimeConfiguration) anomalyDetector.getWindowDelay())
//                        .map(t -> t.toDuration().toMillis())
//                        .orElse(0L);
//                long dataEndTime = Instant.now().toEpochMilli() - delayMillis;
//                dataStartTime = dataEndTime - ((long) (MAX_NUM_OF_SAMPLES_VIEWED) * detectorInterval - delayMillis);
//                for (int j = 0; j < MAX_NUM_OF_SAMPLES_VIEWED; j++) {
//                    timeRanges[j][0] = dataStartTime + (j * detectorInterval);
//                    timeRanges[j][1] = timeRanges[j][0] + detectorInterval;
//                }
//                randomSamplingHelper(timeRanges, feature, listenerCounter, i);
//                if (featureValidTime.containsKey(feature.getName())) {
//                    break;
//                }
//            }

    //    private void randomSamplingHelper(long[][] timeRanges, Feature feature, AtomicInteger listenerCounter, long intervalTime) throws IOException {
//        ParseUtils.parseAggregators(feature.getAggregation().toString(), xContent, feature.getId());
//        XContentParser parser = XContentType.JSON.xContent().createParser(xContent, LoggingDeprecationHandler.INSTANCE, feature.getAggregation().toString());
//        parser.nextToken();
//        List<String> fieldNames = parseAggregationRequest(parser, 0, feature.getAggregation().getName());
//        //Random rand = new Random();
//        MultiSearchRequest sr = new MultiSearchRequest();
//        for (int i = 0; i < NUM_OF_RANDOM_SAMPLES; i++) {
////            int randIndex = rand.nextInt(MAX_NUM_OF_SAMPLES_VIEWED - 1);
////            long RandomRangeStart = timeRanges[randIndex][0];
////            long RandomRangeEnd = timeRanges[randIndex][1];
//            long rangeStart = timeRanges[i][0];
//            long rangeEnd = timeRanges[i][1];
//            RangeQueryBuilder rangeQueryRandom = new RangeQueryBuilder(anomalyDetector.getTimeField())
//                    .from(rangeStart)
//                    .to(rangeEnd)
//                    .format("epoch_millis")
//                    .includeLower(true)
//                    .includeUpper(false);
//            BoolQueryBuilder boolQuery2 = QueryBuilders.boolQuery().filter(rangeQueryRandom).filter(anomalyDetector.getFilterQuery())
//                    .filter(QueryBuilders.existsQuery(fieldNames.get(0)));
//            SearchSourceBuilder internalSearchSourceBuilder = new SearchSourceBuilder().query(boolQuery2).size(1).terminateAfter(1);
//            SearchRequest searchRequest = new SearchRequest(anomalyDetector.getIndices().get(0)).source(internalSearchSourceBuilder);
//            sr.add(searchRequest);
//        }
//        // System.out.println("8 requests: " + sr.requests().toString());
//        client
//                .multiSearch(
//                        sr,
//                        ActionListener
//                                .wrap(
//                                        searchResponse -> {
//                                            savedResponsesToFeature.put(feature.getName(), searchResponse);
//                                            listenerCounter.incrementAndGet();
//                                            if (listenerCounter.get() >= anomalyDetector.getFeatureAttributes().size()) {
//                                                onRandomGetMultiResponse(searchResponse, feature, listenerCounter, intervalTime);
//                                            }
//                                        },
//                                        exception -> {
//                                            System.out.println(exception.getMessage());
//                                            onFailure(exception);
//                                        }
//                                )
//                );
//    }

//    private void onRandomGetMultiResponse(MultiSearchResponse multiSearchResponse, Feature feature, AtomicInteger listnerCounter, long intervalTime)
//    throws IOException {
//        if (intervalTime >= MAX_INTERVAL_LENGTH) {
//            return;
//        }
//        for (String f: savedResponsesToFeature.keySet()) {
//            String errorMsg = "";
//            final AtomicInteger hitCounter = new AtomicInteger();
//            //System.out.println("feature name out of all feature loop: " + f);
//            for (MultiSearchResponse.Item item : savedResponsesToFeature.get(f)) {
//                SearchResponse response = item.getResponse();
//                //  System.out.println("each response for feature, " + f + ": " + response.toString());
//                if (response.getHits().getTotalHits().value > 0) {
//                    hitCounter.incrementAndGet();
//                }
//            }
////            System.out.println("Hit counter before last validation: " + hitCounter.get());
////            System.out.println("successRate: " + hitCounter.doubleValue() / (double) NUM_OF_RANDOM_SAMPLES);
//
//            if (hitCounter.doubleValue() / (double) NUM_OF_RANDOM_SAMPLES < SAMPLE_SUCCESS_RATE) {
//                featureIntervalValidation.put(f, false);
//                errorMsg += "data is too sparse with this interval for feature: " + f;
//                logger.warn(errorMsg);
//                suggestedChanges.add(errorMsg);
//            } else {
//                featureValidTime.put(f, intervalTime);
//                featureIntervalValidation.put(f, true);
//            }
//        }
//        if (!featureIntervalValidation.containsValue(false)) {
//            return;
//        }
//        System.out.println("valid time" + featureValidTime);
//        if (featureValidTime.keySet().size() == anomalyDetector.getFeatureAttributes().size()) {
//            featureIntervalValidation.put(feature.getName(), false);
//        }
//        if (featureIntervalValidation.containsValue(false)) {
//            long detectorInterval = intervalTime * 2;
//            long[][] timeRanges = new long[MAX_NUM_OF_SAMPLES_VIEWED][2];
//            long delayMillis = Optional
//                    .ofNullable((IntervalTimeConfiguration) anomalyDetector.getWindowDelay())
//                    .map(t -> t.toDuration().toMillis())
//                    .orElse(0L);
//            long dataEndTime = Instant.now().toEpochMilli() - delayMillis;
//            long dataStartTime = dataEndTime - ((long) (MAX_NUM_OF_SAMPLES_VIEWED) * detectorInterval - delayMillis);
//            for (int j = 0; j < MAX_NUM_OF_SAMPLES_VIEWED; j++) {
//                timeRanges[j][0] = dataStartTime + (j * detectorInterval);
//                timeRanges[j][1] = timeRanges[j][0] + detectorInterval;
//            }
//            randomSamplingHelper(timeRanges, feature, listnerCounter, detectorInterval);
//        }
//        if (featureValidTime.keySet().size() != anomalyDetector.getFeatureAttributes().size()) {
//            validateAnomalyDetectorResponse();
//        } else {
//            checkWindowDelay();
//        }
//    }

//    private void randomSamplingHelper(long[][] timeRanges, Feature feature, AtomicInteger listenerCounter) throws IOException {
//        ParseUtils.parseAggregators(feature.getAggregation().toString(), xContent, feature.getId());
//        XContentParser parser = XContentType.JSON.xContent().createParser(xContent, LoggingDeprecationHandler.INSTANCE, feature.getAggregation().toString());
//        parser.nextToken();
//        List<String> fieldNames = parseAggregationRequest(parser, 0, feature.getAggregation().getName());
//        //Random rand = new Random();
//        MultiSearchRequest sr = new MultiSearchRequest();
//        for (int i = 0; i < NUM_OF_RANDOM_SAMPLES; i++) {
////            int randIndex = rand.nextInt(MAX_NUM_OF_SAMPLES_VIEWED - 1);
////            long RandomRangeStart = timeRanges[randIndex][0];
////            long RandomRangeEnd = timeRanges[randIndex][1];
//            long rangeStart = timeRanges[i][0];
//            long rangeEnd = timeRanges[i][1];
//            RangeQueryBuilder rangeQueryRandom = new RangeQueryBuilder(anomalyDetector.getTimeField())
//                    .from(rangeStart)
//                    .to(rangeEnd)
//                    .format("epoch_millis")
//                    .includeLower(true)
//                    .includeUpper(false);
//            BoolQueryBuilder boolQuery2 = QueryBuilders.boolQuery().filter(rangeQueryRandom).filter(anomalyDetector.getFilterQuery())
//                    .filter(QueryBuilders.existsQuery(fieldNames.get(0)));
//            SearchSourceBuilder internalSearchSourceBuilder = new SearchSourceBuilder().query(boolQuery2).size(1).terminateAfter(1);
//            SearchRequest searchRequest = new SearchRequest(anomalyDetector.getIndices().get(0)).source(internalSearchSourceBuilder);
//            sr.add(searchRequest);
//        }
//       // System.out.println("8 requests: " + sr.requests().toString());
//        client
//                .multiSearch(
//                        sr,
//                        ActionListener
//                                .wrap(
//                                        searchResponse -> {
//                                            savedResponsesToFeature.put(feature.getName(), searchResponse);
//                                            listenerCounter.incrementAndGet();
//                                            if (listenerCounter.get() >= anomalyDetector.getFeatureAttributes().size()) {
//                                                onRandomGetMultiResponse(searchResponse, feature, listenerCounter);
//                                            }
//                                             },
//                                        exception -> {
//                                            System.out.println(exception.getMessage());
//                                            onFailure(exception);
//                                        }
//                                )
//                );
//    }
//
//    private void onRandomGetMultiResponse(MultiSearchResponse multiSearchResponse, Feature feature, AtomicInteger listnerCounter) {
//            for (String f: savedResponsesToFeature.keySet()) {
//                String errorMsg = "";
//                final AtomicInteger hitCounter = new AtomicInteger();
//                //System.out.println("feature name out of all feature loop: " + f);
//                for (MultiSearchResponse.Item item : savedResponsesToFeature.get(f)) {
//                    SearchResponse response = item.getResponse();
//                  //  System.out.println("each response for feature, " + f + ": " + response.toString());
//                    if (response.getHits().getTotalHits().value > 0) {
//                        hitCounter.incrementAndGet();
//                    }
//                }
//                System.out.println("Hit counter before last validation: " + hitCounter.get());
//                System.out.println("successRate: " + hitCounter.doubleValue() / (double) NUM_OF_RANDOM_SAMPLES);
//
//                if (hitCounter.doubleValue() / (double) NUM_OF_RANDOM_SAMPLES < SAMPLE_SUCCESS_RATE) {
//
//                    featureIntervalValidation.put(f, false);
//                    errorMsg += "data is too sparse with this interval for feature: " + f;
//                    logger.warn(errorMsg);
//                    suggestedChanges.add(errorMsg);
//                } else {
//                    featureIntervalValidation.put(f, true);
//                }
//            }
//       // System.out.println("validateIntervalMap: " + featureIntervalValidation.toString());
//        if (featureIntervalValidation.containsValue(false)) {
//                validateAnomalyDetectorResponse();
//            } else {
//                checkWindowDelay();
//            }
//    }

    private void checkWindowDelay() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .aggregation(AggregationBuilders.max(AGG_NAME_MAX).field(anomalyDetector.getTimeField()))
                .size(1).sort(new FieldSortBuilder("timestamp").order(SortOrder.DESC));
        SearchRequest searchRequest = new SearchRequest().indices(anomalyDetector.getIndices().get(0)).source(searchSourceBuilder);
        client.search(searchRequest, ActionListener.wrap(response -> checkDelayResponse(getLatestDataTime(response)), exception -> onFailure(exception)));
    }

    private Optional<Long> getLatestDataTime(SearchResponse searchResponse) {
        //System.out.println(searchResponse.toString());
        Optional<Long> x = Optional
                .ofNullable(searchResponse)
                .map(SearchResponse::getAggregations)
                .map(aggs -> aggs.asMap())
                .map(map -> (Max) map.get(AGG_NAME_MAX))
                .map(agg -> (long) agg.getValue());
        //System.out.println("after parsing the max timestamp to long: " + x.get());
        return x;
    }

    private void checkDelayResponse(Optional<Long> lastTimeStamp) {
        long delayMillis = Optional
                .ofNullable((IntervalTimeConfiguration) anomalyDetector.getWindowDelay())
                .map(t -> t.toDuration().toMillis())
                .orElse(0L);
        if (lastTimeStamp.isPresent() && (Instant.now().toEpochMilli() - lastTimeStamp.get() > delayMillis)) {
            long minutesSinceLastStamp = TimeUnit.MILLISECONDS.toMinutes(Instant.now().toEpochMilli() - lastTimeStamp.get());
            long windowDelayMins = TimeUnit.MILLISECONDS.toMinutes(delayMillis);
            String errorMsg = "window-delay given is too short, and last seen timestamp is " + minutesSinceLastStamp +
                    " minutes ago " + "and the window-delay given is only of " + windowDelayMins + " minutes";
            logger.warn(errorMsg);
            suggestedChanges.add(errorMsg);
        }
        validateAnomalyDetectorResponse();
    }

    private void validateAnomalyDetectorResponse() {
        this.responseValidate.setFailures(failures);
        this.responseValidate.setSuggestedChanges(suggestedChanges);
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

    private double parseAggregation(Aggregation aggregation) {
        Double result = null;
        if (aggregation instanceof NumericMetricsAggregation.SingleValue) {
            result = ((NumericMetricsAggregation.SingleValue) aggregation).value();
        } else if (aggregation instanceof InternalTDigestPercentiles) {
            Iterator<Percentile> percentile = ((InternalTDigestPercentiles) aggregation).iterator();
            if (percentile.hasNext()) {
                result = percentile.next().getValue();
            }
        }
        return Optional.ofNullable(result).orElseThrow(() -> new IllegalStateException("Failed to parse aggregation " + aggregation));
    }
    private void getFieldMapping() {
        GetMappingsRequest request = new GetMappingsRequest().indices(anomalyDetector.getIndices().get(0));
        adminClient
                .indices().getMappings(
                request,
                ActionListener
                        .wrap(
                                response -> checkFieldIndex(response),
                                exception -> onFailure(exception)
                        )
        );
    }

    private void checkFieldIndex(GetMappingsResponse response) {
        System.out.println(response.toString());
//        Optional<Long> x = Optional
//                .ofNullable(response)
//                .map(SearchResponse::get)
//                .map(aggs -> aggs.asMap())
//                .map(map -> (Max) map.get(AGG_NAME_MAX))
//                .map(agg -> (long) agg.getValue());
    }
}

