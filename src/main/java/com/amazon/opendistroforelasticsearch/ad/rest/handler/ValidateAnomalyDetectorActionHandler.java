package com.amazon.opendistroforelasticsearch.ad.rest.handler;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.*;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
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
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.Percentile;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;


import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.*;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateRange;

/**
 * Anomaly detector REST action handler to process POST request.
 * POST request is for validating anomaly detector.
 */
public class ValidateAnomalyDetectorActionHandler extends AbstractActionHandler {

    protected static final String AGG_NAME_MAX = "max_timefield";
    protected static final int NUM_OF_RANDOM_SAMPLES = 12;
    protected static final int MAX_NUM_OF_SAMPLES_VIEWED = 128;
    protected static final int NUM_OF_INTERVALS_CHECKED = 256;
    protected static final double SAMPLE_SUCCESS_RATE = 0.75;
    protected static final int RANDOM_SAMPLING_REPEATS = 10;
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

    private final List<String> failures;
    private final List<String> suggestedChanges;


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

    private void queryFilterValidation() {
        long delayMillis = Optional
                .ofNullable((IntervalTimeConfiguration) anomalyDetector.getWindowDelay())
                .map(t -> t.toDuration().toMillis())
                .orElse(0L);
        long dataEndTime = Instant.now().toEpochMilli() - delayMillis;
        long detectorInterval = Optional
                .ofNullable((IntervalTimeConfiguration) anomalyDetector.getDetectionInterval())
                .map(t -> t.toDuration().toMillis())
                .orElse(0L);
        long dataStartTime = dataEndTime - ((long) (NUM_OF_INTERVALS_CHECKED) * detectorInterval - delayMillis);
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder(anomalyDetector.getTimeField())
                .from(dataStartTime)
                .to(dataEndTime)
                .format("epoch_millis");
        BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().must(rangeQuery).must(anomalyDetector.getFilterQuery());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(internalFilterQuery).size(1).timeout(requestTimeout);
        SearchRequest searchRequest = new SearchRequest(anomalyDetector.getIndices().get(0)).source(searchSourceBuilder);
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
            failures.add(errorMsg);
            validateAnomalyDetectorResponse();
        } else {
            featureQueryValidation();
        }
    }

    private void featureQueryValidation() throws IOException {
        long delayMillis = Optional
                .ofNullable((IntervalTimeConfiguration) anomalyDetector.getWindowDelay())
                .map(t -> t.toDuration().toMillis())
                .orElse(0L);
        long dataEndTime = Instant.now().toEpochMilli() - delayMillis;
        IntervalTimeConfiguration searchRange = new IntervalTimeConfiguration(10080, ChronoUnit.MINUTES);
        long searchRangeTime = Optional
                .ofNullable(searchRange)
                .map(t -> t.toDuration().toMillis())
                .orElse(0L);

        long dataStartTime = dataEndTime - (searchRangeTime - delayMillis);
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder(anomalyDetector.getTimeField())
                .from(dataStartTime)
                .to(dataEndTime)
                .format("epoch_millis")
                .includeLower(true)
                .includeUpper(false);
        if (anomalyDetector.getFeatureAttributes() != null) {
            for (Feature feature : anomalyDetector.getFeatureAttributes()) {
                AggregatorFactories.Builder internalAgg = ParseUtils.parseAggregators(
                        feature.getAggregation().toString(),
                        xContent,
                        feature.getId()
                );
                BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().must(rangeQuery).must(anomalyDetector.getFilterQuery());
                SearchSourceBuilder internalSearchSourceBuilder = new SearchSourceBuilder().query(internalFilterQuery);
                internalSearchSourceBuilder.aggregation(internalAgg.getAggregatorFactories().iterator().next());
                SearchRequest searchRequest = new SearchRequest(anomalyDetector.getIndices().get(0)).source(internalSearchSourceBuilder);
                //System.out.println("search builder for each feature query: " + internalSearchSourceBuilder.toString());
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

    private void onFeatureAggregationValidation(SearchResponse response, Feature feature) throws IOException {
        //System.out.println("FEATURE AGG VALIDATION: " + response.toString());
        //System.out.println("feature agg done!!!!");
        Optional<Double> aggValue = Optional
                .ofNullable(response)
                .map(SearchResponse::getAggregations)
                .map(aggs -> aggs.asMap())
                .map(map -> map.get(feature.getId()))
                .map(this::parseAggregation);
        if (Double.isNaN(aggValue.get()) || Double.isInfinite(aggValue.get()) ) {
            String errorMsg = "feature query is potentially wrong as no hits were found at all for feature " + feature.getName();
            logger.warn(errorMsg);
            suggestedChanges.add(errorMsg);
            System.out.println("no hits from feature query over 1 week");
        }
    }

    private void randomSamplingIntervalValidation() throws IOException {
        long delayMillis = Optional
                .ofNullable((IntervalTimeConfiguration) anomalyDetector.getWindowDelay())
                .map(t -> t.toDuration().toMillis())
                .orElse(0L);
        long dataEndTime = Instant.now().toEpochMilli() - delayMillis;
        long detectorInterval = Optional
                .ofNullable((IntervalTimeConfiguration) anomalyDetector.getDetectionInterval())
                .map(t -> t.toDuration().toMillis())
                .orElse(0L);
        long dataStartTime = dataEndTime - ((long) (MAX_NUM_OF_SAMPLES_VIEWED) * detectorInterval - delayMillis);

        long[][] timeRanges = new long[MAX_NUM_OF_SAMPLES_VIEWED][2];
        for (int i = 0; i < MAX_NUM_OF_SAMPLES_VIEWED; i++) {
            timeRanges[i][0] = dataStartTime + (i * detectorInterval);
            timeRanges[i][1] = timeRanges[i][0] + detectorInterval;
        }
        //System.out.println("timeRanges: " + Arrays.deepToString(timeRanges));




        if (anomalyDetector.getFeatureAttributes() != null) {
            for (Feature feature : anomalyDetector.getFeatureAttributes()) {
                AggregatorFactories.Builder internalAgg = ParseUtils.parseAggregators(
                        feature.getAggregation().toString(),
                        xContent,
                        feature.getId()
                );
                Random rand = new Random();
                int[] hitCounter = new int[]{0};
                int sampleCounter = 0;
                for (int i = 0; i < NUM_OF_RANDOM_SAMPLES; i++) {
                    sampleCounter++;
                    int randIndex = rand.nextInt(127);
                    long RandomRangeStart = timeRanges[randIndex][0];
                    long RandomRangeEnd = timeRanges[randIndex][1];
                    RangeQueryBuilder rangeQueryRandom = new RangeQueryBuilder(anomalyDetector.getTimeField())
                            .from(RandomRangeStart)
                            .to(RandomRangeEnd)
                            .format("epoch_millis")
                            .includeLower(true)
                            .includeUpper(false);
                    BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().must(rangeQueryRandom).must(anomalyDetector.getFilterQuery());
                    SearchSourceBuilder internalSearchSourceBuilder = new SearchSourceBuilder().query(internalFilterQuery);
                    internalSearchSourceBuilder.aggregation(internalAgg.getAggregatorFactories().iterator().next());
                    SearchRequest searchRequest = new SearchRequest(anomalyDetector.getIndices().get(0)).source(internalSearchSourceBuilder);
                    //System.out.println("search builder inside counter: " + internalSearchSourceBuilder.toString());
                    client
                            .search(
                                    searchRequest,
                                    ActionListener
                                            .wrap(
                                                    searchResponse -> onRandomSampleResponse(searchResponse, hitCounter, sampleCounter),
                                                    exception -> {
                                                        System.out.println(exception.getMessage());
                                                        onFailure(exception);
                                                    }
                                            )
                            );
                }
                System.out.println(sampleCounter);
                if (sampleCounter >= NUM_OF_RANDOM_SAMPLES - 2) {
                    System.out.println("hitdsad counter: " + Arrays.toString(hitCounter));
                    double successRate = (double) hitCounter[0] / (double) NUM_OF_RANDOM_SAMPLES;
                    System.out.println(successRate);
                    if ((double) hitCounter[0] / (double) NUM_OF_RANDOM_SAMPLES < SAMPLE_SUCCESS_RATE) {
                        String errorMsg = "data is too sparse with this interval for feature " + feature.getName();
                        logger.warn(errorMsg);
                        suggestedChanges.add(errorMsg);
                        validateAnomalyDetectorResponse();
                        return;
                    }
                }
            }
        }
        checkWindowDelay();
        //getFieldMapping();
    }



    private void onRandomSampleResponse(SearchResponse response, int[] hitCounter, int sampleCounter) {
        //System.out.println("response from random sampling: " + response.toString());
        if (response.getHits().getTotalHits().value > 0) {
            hitCounter[0]++;
            System.out.println("hit counter inside if check: " + hitCounter[0]);
        }
    }

    private void checkWindowDelay() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .aggregation(AggregationBuilders.max(AGG_NAME_MAX).field(anomalyDetector.getTimeField()))
                .size(1).sort(new FieldSortBuilder("timestamp").order(SortOrder.DESC));
        SearchRequest searchRequest = new SearchRequest().indices(anomalyDetector.getIndices().get(0)).source(searchSourceBuilder);
        client.search(searchRequest, ActionListener.wrap(response -> checkDelayResponse(getLatestDataTime(response)), exception -> onFailure(exception)));
    }

    private Optional<Long> getLatestDataTime(SearchResponse searchResponse) {
        System.out.println(searchResponse.toString());
        Optional<Long> x = Optional
                .ofNullable(searchResponse)
                .map(SearchResponse::getAggregations)
                .map(aggs -> aggs.asMap())
                .map(map -> (Max) map.get(AGG_NAME_MAX))
                .map(agg -> (long) agg.getValue());

        System.out.println("after parsing the max timestamp to long: " + x.get());
        return x;
    }

    private void checkDelayResponse(Optional<Long> lastTimeStamp) {
        long delayMillis = Optional
                .ofNullable((IntervalTimeConfiguration) anomalyDetector.getWindowDelay())
                .map(t -> t.toDuration().toMillis())
                .orElse(0L);
        System.out.println("Window delay passed in from configs: " + delayMillis);
        System.out.println("Time now: " + Instant.now().toEpochMilli());
        System.out.println("last seen time stamp: " + lastTimeStamp.get());
        if (lastTimeStamp.isPresent() && (Instant.now().toEpochMilli() - lastTimeStamp.get() > delayMillis)) {
            String errorMsg = "window-delay too short:";
            logger.warn(errorMsg);
            suggestedChanges.add(errorMsg);
        }
        validateAnomalyDetectorResponse();
    }

    private void validateAnomalyDetectorResponse() {
        this.responseValidate.setFailures(failures);
        this.responseValidate.setSuggestedChanges(suggestedChanges);
        System.out.println("failure list in response: " + responseValidate.getFailures());
        System.out.println("suggestion list in response: " + responseValidate.getSuggestedChanges());
        System.out.println("inside response building and sending");
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
}
//private void randomSamplingIntervalValidationCombined() throws IOException{
//        System.out.println("inside combined random sampling");
//    long delayMillis = Optional
//            .ofNullable((IntervalTimeConfiguration) anomalyDetector.getWindowDelay())
//            .map(t -> t.toDuration().toMillis())
//            .orElse(0L);
//    long dataStartTime = Instant.now().toEpochMilli() - delayMillis;
//    long detectorInterval = Optional
//            .ofNullable((IntervalTimeConfiguration) anomalyDetector.getDetectionInterval())
//            .map(t -> t.toDuration().toMillis())
//            .orElse(0L);
//    long dataEndTime = dataStartTime + ((long) (MAX_NUM_OF_SAMPLES_VIEWED) * detectorInterval - delayMillis);
//
//    long[][] timeRanges = new long[MAX_NUM_OF_SAMPLES_VIEWED][2];
//    for (int i = 0; i < MAX_NUM_OF_SAMPLES_VIEWED; i++) {
//        timeRanges[i][0] = dataStartTime + (i * detectorInterval);
//        timeRanges[i][1] = timeRanges[i][0] + detectorInterval;
//    }
//    //System.out.println("timeRanges: " + Arrays.deepToString(timeRanges));
//    BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().must(anomalyDetector.getFilterQuery());
//
//    SearchSourceBuilder internalSearchSourceBuilder = new SearchSourceBuilder();
//
//    if (anomalyDetector.getFeatureAttributes() != null) {
//        for (Feature feature : anomalyDetector.getFeatureAttributes()) {
//            AggregatorFactories.Builder internalAgg = ParseUtils.parseAggregators(
//                    feature.getAggregation().toString(),
//                    xContent,
//                    feature.getId()
//            );
//            internalSearchSourceBuilder.aggregation(internalAgg.getAggregatorFactories().iterator().next());
//        }
//        Random rand = new Random();
//        int hitCounter = 0;
//        for (int i = 0; i < NUM_OF_RANDOM_SAMPLES; i++) {
//            int randIndex = rand.nextInt(NUM_OF_RANDOM_SAMPLES);
//            long RandomRangeStart = timeRanges[randIndex][0];
//            long RandomRangeEnd = timeRanges[randIndex][1];
//            RangeQueryBuilder rangeQueryRandom = new RangeQueryBuilder(anomalyDetector.getTimeField())
//                    .from(RandomRangeStart)
//                    .to(RandomRangeEnd)
//                    .format("epoch_millis")
//                    .includeLower(true)
//                    .includeUpper(false);
//            internalFilterQuery.must(rangeQueryRandom);
//            internalSearchSourceBuilder.query(internalFilterQuery);
//            SearchRequest searchRequest = new SearchRequest(ANOMALY_DETECTORS_INDEX).source(internalSearchSourceBuilder);
//            System.out.println("search builder inside counter: " + internalSearchSourceBuilder.toString());
//            client
//                    .search(
//                            searchRequest,
//                            ActionListener
//                                    .wrap(
//                                            searchResponse -> onRandomSampleResponse(searchResponse, hitCounter),
//                                            exception -> {
//                                                System.out.println(exception.getMessage());
//                                                onFailure(exception);
//                                            }
//                                    )
//                    );
//        }
//        System.out.println("hit counter: " + hitCounter);
//        if (hitCounter < 6) {
//            String errorMsg = "data is too sparse with this interval for feature ";
//            logger.warn(errorMsg);
//            suggestedChanges.add(errorMsg);
//            validateAnomalyDetectorResponse();
//        } else {
//            checkWindowDelay();
//        }
//    }
//}
//        private Optional<double[]> parseAggregations(Optional<Aggregations> aggregations, String featureIds) {
//        return aggregations
//                .map(aggs -> aggs.asMap())
//                .map(
//                        map -> featureIds
//                                .stream()
//                                .mapToDouble(id -> Optional.ofNullable(map.get(id)).map(this::parseAggregation).orElse(Double.NaN))
//                                .toArray()
//                )
//                .filter(result -> Arrays.stream(result).noneMatch(d -> Double.isNaN(d) || Double.isInfinite(d)));
//    }

//    private void getFieldMapping() {
//        GetMappingsRequest request = new GetMappingsRequest().indices(anomalyDetector.getIndices().get(0));
//        adminClient
//                .indices().getMappings(
//                request,
//                ActionListener
//                        .wrap(
//                                response -> checkFieldIndex(response),
//                                exception -> onFailure(exception)
//                        )
//        );
//    }
//
//    private void checkFieldIndex(GetMappingsResponse response) {
//        System.out.println(response.toString());
//        Optional<Long> x = Optional
//                .ofNullable(response)
//                .map(SearchResponse::get)
//                .map(aggs -> aggs.asMap())
//                .map(map -> (Max) map.get(AGG_NAME_MAX))
//                .map(agg -> (long) agg.getValue());
//    }
//}


//    private void checkIfMissingFeature() throws IOException{
//            if (anomalyDetector.getFeatureAttributes() != null) {
//                for (Feature feature : anomalyDetector.getFeatureAttributes()) {
//                    AggregatorFactories.Builder internalAgg = ParseUtils.parseAggregators(
//                            feature.getAggregation().toString(),
//                            xContent,
//                            feature.getId()
//                    );
//                }
//            }
//        if (anomalyDetector.getFeatureAttributes() != null) {
//            for (Feature feature : anomalyDetector.getFeatureAttributes()) {
//                AggregatorFactories.Builder internalAgg = ParseUtils.parseAggregators(
//                        feature.getAggregation().toString(),
//                        xContent,
//                        feature.getId()
//                );
//                internalSearchSourceBuilder.aggregation(internalAgg.getAggregatorFactories().iterator().next());
//            }
//        }
//
//        try {
//            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(internalAgg)
//            return new SearchRequest(detector.getIndices().toArray(new String[0]), searchSourceBuilder);
//        } catch (IOException e) {
//            logger.warn("Failed to create feature search request for " + detector.getDetectorId() + " for preview", e);
//            throw e;
//        }
//
//            if (anomalyDetector.getFeatureAttributes() != null) {
//                for (Feature feature : anomalyDetector.getFeatureAttributes()) {
//                    parseAggregations(Optional.ofNullable(feature).map(f -> f.getAggregation()), anomalyDetector.getEnabledFeatureIds());
//                }
//            }
//        }