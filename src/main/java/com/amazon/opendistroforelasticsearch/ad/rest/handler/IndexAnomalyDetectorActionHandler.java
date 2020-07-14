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
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;

/**
 * Anomaly detector REST action handler to process POST/PUT request.
 * POST request is for creating anomaly detector.
 * PUT request is for updating anomaly detector.
 */
public class IndexAnomalyDetectorActionHandler extends AbstractActionHandler {

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

    /**
     * Constructor function.
     *
     * @param settings                ES settings
     * @param clusterService          ClusterService
     * @param client                  ES node client that executes actions on the local node
     * @param channel                 ES channel used to construct bytes / builder based outputs, and send responses
     * @param anomalyDetectionIndices anomaly detector index manager
     * @param detectorId              detector identifier
     * @param seqNo                   sequence number of last modification
     * @param primaryTerm             primary term of last modification
     * @param refreshPolicy           refresh policy
     * @param anomalyDetector         anomaly detector instance
     * @param requestTimeout          request time out configuration
     * @param maxAnomalyDetectors     max anomaly detector allowed
     * @param maxAnomalyFeatures      max features allowed per detector
     */
    public IndexAnomalyDetectorActionHandler(
        Settings settings,
        ClusterService clusterService,
        NodeClient client,
        RestChannel channel,
        AnomalyDetectionIndices anomalyDetectionIndices,
        String detectorId,
        Long seqNo,
        Long primaryTerm,
        WriteRequest.RefreshPolicy refreshPolicy,
        AnomalyDetector anomalyDetector,
        TimeValue requestTimeout,
        Integer maxAnomalyDetectors,
        Integer maxAnomalyFeatures
    ) {
        super(client, channel);
        this.clusterService = clusterService;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.detectorId = detectorId;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.refreshPolicy = refreshPolicy;
        this.anomalyDetector = anomalyDetector;
        this.requestTimeout = requestTimeout;
        this.maxAnomalyDetectors = maxAnomalyDetectors;
        this.maxAnomalyFeatures = maxAnomalyFeatures;
    }

    /**
     * Start function to process create/update anomaly detector request.
     * Check if anomaly detector index exist first, if not, will create first.
     *
     * @throws IOException IOException from {@link AnomalyDetectionIndices#initAnomalyDetectorIndexIfAbsent(ActionListener)}
     */
    public void start() throws IOException {
        if (!anomalyDetectionIndices.doesAnomalyDetectorIndexExist()) {
            anomalyDetectionIndices
                .initAnomalyDetectorIndex(
                    ActionListener.wrap(response -> onCreateMappingsResponse(response), exception -> onFailure(exception))
                );
        } else {
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
        String error = RestHandlerUtils.validateAnomalyDetector(anomalyDetector, maxAnomalyFeatures);
        if (StringUtils.isNotBlank(error)) {
            channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, error));
            return;
        }
        if (channel.request().method() == RestRequest.Method.PUT) {
            handler.getDetectorJob(clusterService, client, detectorId, channel, () -> updateAnomalyDetector(client, detectorId));
        } else {
            createAnomalyDetector();
        }
    }

    private void updateAnomalyDetector(NodeClient client, String detectorId) {
        GetRequest request = new GetRequest(ANOMALY_DETECTORS_INDEX, detectorId);
        client.get(request, ActionListener.wrap(response -> onGetAnomalyDetectorResponse(response), exception -> onFailure(exception)));
    }

    private void onGetAnomalyDetectorResponse(GetResponse response) throws IOException {
        if (!response.isExists()) {
            XContentBuilder builder = channel
                .newErrorBuilder()
                .startObject()
                .field("Message", "AnomalyDetector is not found with id: " + detectorId)
                .endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.NOT_FOUND, response.toXContent(builder, EMPTY_PARAMS)));
            return;
        }

        searchAdInputIndices(detectorId);
    }

    private void createAnomalyDetector() {
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
            onFailure(new IllegalArgumentException(errorMsg));
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
            onFailure(new IllegalArgumentException(errorMsg));
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
            onFailure(new IllegalArgumentException(errorMsg));
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
            anomalyDetector.getUiMetadata(),
            anomalyDetector.getSchemaVersion(),
            Instant.now()
        );
        IndexRequest indexRequest = new IndexRequest(ANOMALY_DETECTORS_INDEX)
            .setRefreshPolicy(refreshPolicy)
            .source(detector.toXContent(channel.newBuilder(), XCONTENT_WITH_TYPE))
            .setIfSeqNo(seqNo)
            .setIfPrimaryTerm(primaryTerm)
            .timeout(requestTimeout);
        if (detectorId != null) {
            indexRequest.id(detectorId);
        }
        client.index(indexRequest, indexAnomalyDetectorResponse());
    }

    private ActionListener<IndexResponse> indexAnomalyDetectorResponse() {
        return new RestResponseListener<IndexResponse>(channel) {
            @Override
            public RestResponse buildResponse(IndexResponse response) throws Exception {
                if (response.getShardInfo().getSuccessful() < 1) {
                    return new BytesRestResponse(response.status(), response.toXContent(channel.newErrorBuilder(), EMPTY_PARAMS));
                }

                XContentBuilder builder = channel
                    .newBuilder()
                    .startObject()
                    .field(RestHandlerUtils._ID, response.getId())
                    .field(RestHandlerUtils._VERSION, response.getVersion())
                    .field(RestHandlerUtils._SEQ_NO, response.getSeqNo())
                    .field(RestHandlerUtils._PRIMARY_TERM, response.getPrimaryTerm())
                    .field("anomaly_detector", anomalyDetector)
                    .endObject();

                BytesRestResponse restResponse = new BytesRestResponse(response.status(), builder);
                if (response.status() == RestStatus.CREATED) {
                    String location = String.format(Locale.ROOT, "%s/%s", AnomalyDetectorPlugin.AD_BASE_URI, response.getId());
                    restResponse.addHeader("Location", location);
                }
                return restResponse;
            }
        };
    }

    private void onCreateMappingsResponse(CreateIndexResponse response) throws IOException {
        if (response.isAcknowledged()) {
            logger.info("Created {} with mappings.", ANOMALY_DETECTORS_INDEX);
            prepareAnomalyDetectorIndexing();
        } else {
            logger.warn("Created {} with mappings call not acknowledged.", ANOMALY_DETECTORS_INDEX);
            channel
                .sendResponse(
                    new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, response.toXContent(channel.newErrorBuilder(), EMPTY_PARAMS))
                );
        }
    }

}
