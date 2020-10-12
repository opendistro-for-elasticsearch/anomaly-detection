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
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.XCONTENT_WITH_TYPE;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.transport.IndexAnomalyDetectorResponse;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;

/**
 * Anomaly detector REST action handler to process POST/PUT request.
 * POST request is for creating anomaly detector.
 * PUT request is for updating anomaly detector.
 */
public class IndexAnomalyDetectorActionHandler {

    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final String detectorId;
    private final Long seqNo;
    private final Long primaryTerm;
    private final WriteRequest.RefreshPolicy refreshPolicy;
    private final AnomalyDetector anomalyDetector;
    private final ClusterService clusterService;

    private final Logger logger = LogManager.getLogger(IndexAnomalyDetectorActionHandler.class);
    private final TimeValue requestTimeout;
    private final Integer maxAnomalyDetectors;
    private final Integer maxAnomalyFeatures;
    private final AnomalyDetectorActionHandler handler = new AnomalyDetectorActionHandler();
    private final RestRequest.Method method;
    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    private final Settings settings;
    private final ActionListener<IndexAnomalyDetectorResponse> listener;

    /**
     * Constructor function.
     *
     * @param settings                ES settings
     * @param clusterService          ClusterService
     * @param client                  ES node client that executes actions on the local node
     * @param listener                 ES channel used to construct bytes / builder based outputs, and send responses
     * @param anomalyDetectionIndices anomaly detector index manager
     * @param detectorId              detector identifier
     * @param seqNo                   sequence number of last modification
     * @param primaryTerm             primary term of last modification
     * @param refreshPolicy           refresh policy
     * @param anomalyDetector         anomaly detector instance
     * @param requestTimeout          request time out configuration
     * @param maxAnomalyDetectors     max anomaly detector allowed
     * @param maxAnomalyFeatures      max features allowed per detector
     * @param method                  Rest Method type
     * @param xContentRegistry        Registry which is used for XContentParser
     */
    public IndexAnomalyDetectorActionHandler(
        Settings settings,
        ClusterService clusterService,
        Client client,
        ActionListener<IndexAnomalyDetectorResponse> listener,
        AnomalyDetectionIndices anomalyDetectionIndices,
        String detectorId,
        Long seqNo,
        Long primaryTerm,
        WriteRequest.RefreshPolicy refreshPolicy,
        AnomalyDetector anomalyDetector,
        TimeValue requestTimeout,
        Integer maxAnomalyDetectors,
        Integer maxAnomalyFeatures,
        RestRequest.Method method,
        NamedXContentRegistry xContentRegistry
    ) {
        this.settings = settings;
        this.clusterService = clusterService;
        this.client = client;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.listener = listener;
        this.detectorId = detectorId;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.refreshPolicy = refreshPolicy;
        this.anomalyDetector = anomalyDetector;
        this.requestTimeout = requestTimeout;
        this.maxAnomalyDetectors = maxAnomalyDetectors;
        this.maxAnomalyFeatures = maxAnomalyFeatures;
        this.method = method;
        this.xContentRegistry = xContentRegistry;
    }

    /**
     * Start function to process create/update anomaly detector request.
     * Check if anomaly detector index exist first, if not, will create first.
     *
     * @throws IOException IOException from {@link AnomalyDetectionIndices#initAnomalyDetectorIndexIfAbsent(ActionListener)}
     */
    public void start() throws IOException {
        if (!anomalyDetectionIndices.doesAnomalyDetectorIndexExist()) {
            logger.info("AnomalyDetector Indices do not exist");
            anomalyDetectionIndices
                .initAnomalyDetectorIndex(
                    ActionListener.wrap(response -> onCreateMappingsResponse(response), exception -> listener.onFailure(exception))
                );
        } else {
            logger.info("AnomalyDetector Indices do exist, calling prepareAnomalyDetectorIndexing");
            prepareAnomalyDetectorIndexing();
        }
    }

    /**
     * Prepare for indexing a new anomaly detector.
     */
    private void prepareAnomalyDetectorIndexing() {
        // TODO: check if aggregation query will return only one number. Not easy to validate,
        // 1).If index has only one document
        // 2).If filter will only return one document,
        // 3).If custom expression has specific logic to return one number for some case,
        // but multiple for others, like some if/else branch
        logger.info("prepareAnomalyDetectorIndexing called after creating indices");
        String error = RestHandlerUtils.validateAnomalyDetector(anomalyDetector, maxAnomalyFeatures);
        if (StringUtils.isNotBlank(error)) {
            listener.onFailure(new ElasticsearchStatusException(error, RestStatus.BAD_REQUEST));
            return;
        }
        if (method == RestRequest.Method.PUT) {
            handler.getDetectorJob(clusterService, client, detectorId, listener, () -> updateAnomalyDetector(detectorId), xContentRegistry);
        } else {
            createAnomalyDetector();
        }
    }

    private void updateAnomalyDetector(String detectorId) {
        GetRequest request = new GetRequest(ANOMALY_DETECTORS_INDEX, detectorId);
        client
            .get(
                request,
                ActionListener.wrap(response -> onGetAnomalyDetectorResponse(response), exception -> listener.onFailure(exception))
            );
    }

    private void onGetAnomalyDetectorResponse(GetResponse response) throws IOException {
        if (!response.isExists()) {
            listener
                .onFailure(new ElasticsearchStatusException("AnomalyDetector is not found with id: " + detectorId, RestStatus.NOT_FOUND));
            return;
        }

        searchAdInputIndices(detectorId);
    }

    private void createAnomalyDetector() {
        try {
            QueryBuilder query = QueryBuilders.matchAllQuery();
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query).size(0).timeout(requestTimeout);

            SearchRequest searchRequest = new SearchRequest(ANOMALY_DETECTORS_INDEX).source(searchSourceBuilder);

            client
                .search(
                    searchRequest,
                    ActionListener.wrap(response -> onSearchAdResponse(response), exception -> listener.onFailure(exception))
                );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void onSearchAdResponse(SearchResponse response) throws IOException {
        if (response.getHits().getTotalHits().value >= maxAnomalyDetectors) {
            String errorMsg = "Can't create anomaly detector more than " + maxAnomalyDetectors;
            logger.error(errorMsg);
            listener.onFailure(new IllegalArgumentException(errorMsg));
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
                    .wrap(
                        searchResponse -> onSearchAdInputIndicesResponse(searchResponse, detectorId),
                        exception -> listener.onFailure(exception)
                    )
            );
    }

    private void onSearchAdInputIndicesResponse(SearchResponse response, String detectorId) throws IOException {
        if (response.getHits().getTotalHits().value == 0) {
            String errorMsg = "Can't create anomaly detector as no document found in indices: "
                + Arrays.toString(anomalyDetector.getIndices().toArray(new String[0]));
            logger.error(errorMsg);
            listener.onFailure(new IllegalArgumentException(errorMsg));
        } else {
            checkADNameExists(detectorId);
        }
    }

    private void checkADNameExists(String detectorId) throws IOException {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        // src/main/resources/mappings/anomaly-detectors.json#L14
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
                        exception -> listener.onFailure(exception)
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
            listener.onFailure(new IllegalArgumentException(errorMsg));
        } else {
            indexAnomalyDetector(detectorId);
        }
    }

    private void indexAnomalyDetector(String detectorId) throws IOException {
        AnomalyDetector detector = new AnomalyDetector(
            anomalyDetector.getDetectorId(),
            anomalyDetector.getVersion(),
            anomalyDetector.getName(),
            anomalyDetector.getDescription(),
            anomalyDetector.getTimeField(),
            anomalyDetector.getIndices(),
            anomalyDetector.getFeatureAttributes(),
            anomalyDetector.getFilterQuery(),
            anomalyDetector.getDetectionInterval(),
            anomalyDetector.getWindowDelay(),
            anomalyDetector.getShingleSize(),
            anomalyDetector.getUiMetadata(),
            anomalyDetector.getSchemaVersion(),
            Instant.now()
        );
        IndexRequest indexRequest = new IndexRequest(ANOMALY_DETECTORS_INDEX)
            .setRefreshPolicy(refreshPolicy)
            .source(detector.toXContent(XContentFactory.jsonBuilder(), XCONTENT_WITH_TYPE))
            .setIfSeqNo(seqNo)
            .setIfPrimaryTerm(primaryTerm)
            .timeout(requestTimeout);
        if (detectorId != null) {
            indexRequest.id(detectorId);
        }
        client.index(indexRequest, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                String errorMsg = checkShardsFailure(indexResponse);
                if (errorMsg != null) {
                    listener.onFailure(new ElasticsearchStatusException(errorMsg, indexResponse.status()));
                    return;
                }
                listener
                    .onResponse(
                        new IndexAnomalyDetectorResponse(
                            indexResponse.getId(),
                            indexResponse.getVersion(),
                            indexResponse.getSeqNo(),
                            indexResponse.getPrimaryTerm(),
                            RestStatus.CREATED
                        )
                    );
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void onCreateMappingsResponse(CreateIndexResponse response) throws IOException {
        if (response.isAcknowledged()) {
            logger.info("Created {} with mappings.", ANOMALY_DETECTORS_INDEX);
            prepareAnomalyDetectorIndexing();
        } else {
            logger.warn("Created {} with mappings call not acknowledged.", ANOMALY_DETECTORS_INDEX);
            listener
                .onFailure(
                    new ElasticsearchStatusException(
                        "Created " + ANOMALY_DETECTORS_INDEX + "with mappings call not acknowledged.",
                        RestStatus.INTERNAL_SERVER_ERROR
                    )
                );
        }
    }

    private String checkShardsFailure(IndexResponse response) {
        StringBuilder failureReasons = new StringBuilder();
        if (response.getShardInfo().getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : response.getShardInfo().getFailures()) {
                failureReasons.append(failure);
            }
            return failureReasons.toString();
        }
        return null;
    }
}
