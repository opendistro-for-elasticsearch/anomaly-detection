package com.amazon.opendistroforelasticsearch.ad.rest.handler;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.*;
import com.amazon.opendistroforelasticsearch.ad.util.ParseUtils;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
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
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.common.xcontent.XContentParser;


import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;

/**
 * Anomaly detector REST action handler to process POST request.
 * POST request is for validating anomaly detector.
 */
public class ValidateAnomalyDetectorActionHandler extends AbstractActionHandler {

    protected static final String AGG_NAME_MAX = "max_timefield";

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
            if (!failures.isEmpty()) {
                validateAnomalyDetectorResponse();
                return;
            }
            validateNumberOfDetectors();
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
            failures.add("indices missing");
        }

        if (anomalyDetector.getWindowDelay() == null) {
            failures.add("window-delay missing");
        }
        if (anomalyDetector.getDetectionInterval() == null) {
            failures.add("detector-interval missing");
        }

        String error = RestHandlerUtils.validateAnomalyDetector(anomalyDetector, maxAnomalyFeatures);
        if (StringUtils.isNotBlank(error)) {
            failures.add(error);
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
            return;
        }
        searchAdInputIndices(null);
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
            return;
        }
        if (anomalyDetector.getFilterQuery() != null) {
            queryFilterValidation();
        } else {
            //featureQueryValidation();
        }

    }

    private void queryFilterValidation() {

//        long delayMillis = Optional
//                .ofNullable((IntervalTimeConfiguration) anomalyDetector.getWindowDelay())
//                .map(t -> t.toDuration().toMillis())
//                .orElse(0L);
//        long dataStartTime = Instant.now().toEpochMilli() - delayMillis;
//        long dataEndTime = 256 * anomalyDetector.getDetectionInterval(). - delayMillis;
//
//
//        RangeQueryBuilder rangeQuery = new RangeQueryBuilder(anomalyDetector.getTimeField())
//                .from(dataStartTime)
//                .to(endTime)
//                .format("epoch_millis")
//                .includeLower(true)
//                .includeUpper(false);
        BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().must(rangeQuery).must(anomalyDetector.getFilterQuery());

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(internalFilterQuery).size(1).timeout(requestTimeout);
        SearchRequest searchRequest = new SearchRequest(ANOMALY_DETECTORS_INDEX).source(searchSourceBuilder);

        client
                .search(
                        searchRequest,
                        ActionListener
                                .wrap(
                                        searchResponse -> onQueryFilterSearch(searchResponse),
                                        exception -> onFailure(exception)
                                )
                );
    }

    private void onQueryFilterSearch(SearchResponse response) {
        if (response.getHits().getTotalHits().value < 0) {
            String errorMsg = "query filter is potentially wrong as no hits were found at all. ";
            logger.warn(errorMsg);
            failures.add(errorMsg);
            return;
        }
        //checkIfMissingFeature(,anomalyDetector.getEnabledFeatureIds())
        featureQueryValidation();
    }


    private void checkIfMissingFeature() throws IOException{
        SearchRequest request = createPreviewSearchRequest(detector, ranges);

        try {
            SearchSourceBuilder searchSourceBuilder = ParseUtils.generatePreviewQuery(detector, ranges, xContent);
            return new SearchRequest(detector.getIndices().toArray(new String[0]), searchSourceBuilder);
        } catch (IOException e) {
            logger.warn("Failed to create feature search request for " + detector.getDetectorId() + " for preview", e);
            throw e;
        }

        client.search(request, ActionListener.wrap(response -> {
            Aggregations aggs = response.getAggregations();
            if (aggs == null) {
                listener.onResponse(Collections.emptyList());
                return;
            }
         if (anomalyDetector.getFeatureAttributes() != null) {
            for (Feature feature : anomalyDetector.getFeatureAttributes()) {
                AggregatorFactories.Builder internalAgg = ParseUtils.parseAggregators(
                        feature.getAggregation().toString(),
                        xContent,
                        feature.getId()
                );
            }
        }

        private Optional<double[]> parseAggregations(Optional< Aggregations > aggregations, List<String> featureIds) {
            return aggregations
                    .map(aggs -> aggs.asMap())
                    .map(
                            map -> featureIds
                                    .stream()
                                    .mapToDouble(id -> Optional.ofNullable(map.get(id)).map(this::parseAggregation).orElse(Double.NaN))
                                    .toArray()
                    )
                    .filter(result -> Arrays.stream(result).noneMatch(d -> Double.isNaN(d) || Double.isInfinite(d)));
        }
    }

    private void featureQueryValidation() throws IOException {
        BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().must(rangeQuery).must(anomalyDetector.getFilterQuery());
        SearchSourceBuilder internalSearchSourceBuilder = new SearchSourceBuilder().query(internalFilterQuery);

        if (anomalyDetector.getFeatureAttributes() != null) {
            for (Feature feature : anomalyDetector.getFeatureAttributes()) {
                AggregatorFactories.Builder internalAgg = ParseUtils.parseAggregators(
                        feature.getAggregation().toString(),
                        xContent,
                        feature.getId()
                );
                internalSearchSourceBuilder.aggregation(internalAgg.getAggregatorFactories().iterator().next());
            }
        }


    }

    private void randomSamplingIntervalValidation() {

    }

    private void checkWindowDelay() {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                    .aggregation(AggregationBuilders.max(AGG_NAME_MAX).field(anomalyDetector.getTimeField()))
                    .size(0);
            SearchRequest searchRequest = new SearchRequest().indices(anomalyDetector.getIndices().toArray(new String[0])).source(searchSourceBuilder);
            client
                    .search(searchRequest, ActionListener.wrap(response -> checkDelayResponse(getLatestDataTime(response)), exception -> onFailure(exception)));
        }

    private Optional<Long> getLatestDataTime(SearchResponse searchResponse) {
        return Optional
                .ofNullable(searchResponse)
                .map(SearchResponse::getAggregations)
                .map(aggs -> aggs.asMap())
                .map(map -> (Max) map.get(AGG_NAME_MAX))
                .map(agg -> (long) agg.getValue());
    }

    private void checkDelayResponse(Optional<Long> windowDelay) {
        if (windowDelay.isPresent()) {
            System.out.println(windowDelay.toString());
        }
    }






    private void validateAnomalyDetectorResponse() throws IOException {
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
}
