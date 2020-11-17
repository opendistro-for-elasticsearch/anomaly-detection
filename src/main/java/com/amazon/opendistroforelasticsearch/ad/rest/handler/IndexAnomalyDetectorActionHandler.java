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
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.transport.IndexAnomalyDetectorResponse;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import com.amazon.opendistroforelasticsearch.commons.authuser.User;

/**
 * Anomaly detector REST action handler to process POST/PUT request.
 * POST request is for creating anomaly detector.
 * PUT request is for updating anomaly detector.
 */
public class IndexAnomalyDetectorActionHandler {
    public static final String EXCEEDED_MAX_MULTI_ENTITY_DETECTORS_PREFIX_MSG = "Can't create multi-entity anomaly detectors more than ";
    public static final String EXCEEDED_MAX_SINGLE_ENTITY_DETECTORS_PREFIX_MSG = "Can't create single-entity anomaly detectors more than ";
    public static final String NO_DOCS_IN_USER_INDEX_MSG = "Can't create anomaly detector as no document found in indices: ";
    public static final String ONLY_ONE_CATEGORICAL_FIELD_ERR_MSG = "We can have only one categorical field.";
    public static final String CATEGORICAL_FIELD_TYPE_ERR_MSG = "A categorical field must be of type keyword or ip.";
    public static final String NOT_FOUND_ERR_MSG = "Cannot found the categorical field %s";

    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final String detectorId;
    private final Long seqNo;
    private final Long primaryTerm;
    private final WriteRequest.RefreshPolicy refreshPolicy;
    private final AnomalyDetector anomalyDetector;
    private final ClusterService clusterService;

    private final Logger logger = LogManager.getLogger(IndexAnomalyDetectorActionHandler.class);
    private final TimeValue requestTimeout;
    private final Integer maxSingleEntityAnomalyDetectors;
    private final Integer maxMultiEntityAnomalyDetectors;
    private final Integer maxAnomalyFeatures;
    private final AnomalyDetectorActionHandler handler = new AnomalyDetectorActionHandler();
    private final RestRequest.Method method;
    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    private final ActionListener<IndexAnomalyDetectorResponse> listener;
    private final User user;

    /**
     * Constructor function.
     *
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
     * @param maxSingleEntityAnomalyDetectors     max single-entity anomaly detectors allowed
     * @param maxMultiEntityAnomalyDetectors      max multi-entity detectors allowed
     * @param maxAnomalyFeatures      max features allowed per detector
     * @param method                  Rest Method type
     * @param xContentRegistry        Registry which is used for XContentParser
     * @param user                    User context
     */
    public IndexAnomalyDetectorActionHandler(
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
        Integer maxSingleEntityAnomalyDetectors,
        Integer maxMultiEntityAnomalyDetectors,
        Integer maxAnomalyFeatures,
        RestRequest.Method method,
        NamedXContentRegistry xContentRegistry,
        User user
    ) {
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
        this.maxSingleEntityAnomalyDetectors = maxSingleEntityAnomalyDetectors;
        this.maxMultiEntityAnomalyDetectors = maxMultiEntityAnomalyDetectors;
        this.maxAnomalyFeatures = maxAnomalyFeatures;
        this.method = method;
        this.xContentRegistry = xContentRegistry;
        this.user = user;
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

    private void onGetAnomalyDetectorResponse(GetResponse response) {
        if (!response.isExists()) {
            listener
                .onFailure(new ElasticsearchStatusException("AnomalyDetector is not found with id: " + detectorId, RestStatus.NOT_FOUND));
            return;
        }
        try (XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            AnomalyDetector existingDetector = AnomalyDetector.parse(parser, response.getId(), response.getVersion());
            if (!hasCategoryField(existingDetector) && hasCategoryField(this.anomalyDetector)) {
                validateAgainstExistingMultiEntityAnomalyDetector(detectorId);
            } else {
                validateCategoricalField(detectorId);
            }
        } catch (IOException e) {
            String message = "Failed to parse anomaly detector " + detectorId;
            logger.error(message, e);
            listener.onFailure(new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
        }

    }

    private boolean hasCategoryField(AnomalyDetector detector) {
        return detector.getCategoryField() != null && !detector.getCategoryField().isEmpty();
    }

    private void validateAgainstExistingMultiEntityAnomalyDetector(String detectorId) {
        QueryBuilder query = QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery(AnomalyDetector.CATEGORY_FIELD));

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query).size(0).timeout(requestTimeout);

        SearchRequest searchRequest = new SearchRequest(ANOMALY_DETECTORS_INDEX).source(searchSourceBuilder);

        client
            .search(
                searchRequest,
                ActionListener
                    .wrap(response -> onSearchMultiEntityAdResponse(response, detectorId), exception -> listener.onFailure(exception))
            );
    }

    private void createAnomalyDetector() {
        try {
            List<String> categoricalFields = anomalyDetector.getCategoryField();
            if (categoricalFields != null && categoricalFields.size() > 0) {
                validateAgainstExistingMultiEntityAnomalyDetector(null);
            } else {
                QueryBuilder query = QueryBuilders.matchAllQuery();
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query).size(0).timeout(requestTimeout);

                SearchRequest searchRequest = new SearchRequest(ANOMALY_DETECTORS_INDEX).source(searchSourceBuilder);

                client
                    .search(
                        searchRequest,
                        ActionListener
                            .wrap(response -> onSearchSingleEntityAdResponse(response), exception -> listener.onFailure(exception))
                    );
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void onSearchSingleEntityAdResponse(SearchResponse response) throws IOException {
        if (response.getHits().getTotalHits().value >= maxSingleEntityAnomalyDetectors) {
            String errorMsg = EXCEEDED_MAX_SINGLE_ENTITY_DETECTORS_PREFIX_MSG + maxSingleEntityAnomalyDetectors;
            logger.error(errorMsg);
            listener.onFailure(new IllegalArgumentException(errorMsg));
        } else {
            searchAdInputIndices(null);
        }
    }

    private void onSearchMultiEntityAdResponse(SearchResponse response, String detectorId) throws IOException {
        if (response.getHits().getTotalHits().value >= maxMultiEntityAnomalyDetectors) {
            String errorMsg = EXCEEDED_MAX_MULTI_ENTITY_DETECTORS_PREFIX_MSG + maxMultiEntityAnomalyDetectors;
            logger.error(errorMsg);
            listener.onFailure(new IllegalArgumentException(errorMsg));
        } else {
            validateCategoricalField(detectorId);
        }
    }

    @SuppressWarnings("unchecked")
    private void validateCategoricalField(String detectorId) {
        List<String> categoryField = anomalyDetector.getCategoryField();

        if (categoryField == null) {
            searchAdInputIndices(detectorId);
            return;
        }

        // we only support one categorical field
        // If there is more than 1 field or none, AnomalyDetector's constructor
        // throws IllegalArgumentException before reaching this line
        if (categoryField.size() != 1) {
            listener.onFailure(new IllegalArgumentException(ONLY_ONE_CATEGORICAL_FIELD_ERR_MSG));
            return;
        }

        String categoryField0 = categoryField.get(0);

        GetFieldMappingsRequest getMappingsRequest = new GetFieldMappingsRequest();
        getMappingsRequest.indices(anomalyDetector.getIndices().toArray(new String[0])).fields(categoryField.toArray(new String[0]));
        getMappingsRequest.indicesOptions(IndicesOptions.strictExpand());

        ActionListener<GetFieldMappingsResponse> mappingsListener = ActionListener.wrap(getMappingsResponse -> {
            // example getMappingsResponse:
            // GetFieldMappingsResponse{mappings={server-metrics={_doc={service=FieldMappingMetadata{fullName='service',
            // source=org.elasticsearch.common.bytes.BytesArray@7ba87dbd}}}}}
            // for nested field, it would be
            // GetFieldMappingsResponse{mappings={server-metrics={_doc={host_nest.host2=FieldMappingMetadata{fullName='host_nest.host2',
            // source=org.elasticsearch.common.bytes.BytesArray@8fb4de08}}}}}
            boolean foundField = false;
            Map<String, Map<String, Map<String, FieldMappingMetadata>>> mappingsByIndex = getMappingsResponse.mappings();

            for (Map<String, Map<String, FieldMappingMetadata>> mappingsByType : mappingsByIndex.values()) {
                for (Map<String, FieldMappingMetadata> mappingsByField : mappingsByType.values()) {
                    for (Map.Entry<String, FieldMappingMetadata> field2Metadata : mappingsByField.entrySet()) {
                        // example output:
                        // host_nest.host2=FieldMappingMetadata{fullName='host_nest.host2',
                        // source=org.elasticsearch.common.bytes.BytesArray@8fb4de08}
                        FieldMappingMetadata fieldMetadata = field2Metadata.getValue();

                        if (fieldMetadata != null) {
                            // sourceAsMap returns sth like {host2={type=keyword}} with host2 being a nested field
                            Map<String, Object> fieldMap = fieldMetadata.sourceAsMap();
                            if (fieldMap != null) {
                                for (Object type : fieldMap.values()) {
                                    if (type != null && type instanceof Map) {
                                        foundField = true;
                                        Map<String, Object> metadataMap = (Map<String, Object>) type;
                                        String typeName = (String) metadataMap.get(CommonName.TYPE);
                                        if (!typeName.equals(CommonName.KEYWORD_TYPE) && !typeName.equals(CommonName.IP_TYPE)) {
                                            listener.onFailure(new IllegalArgumentException(CATEGORICAL_FIELD_TYPE_ERR_MSG));
                                            return;
                                        }
                                    }
                                }
                            }

                        }
                    }
                }
            }

            if (foundField == false) {
                listener.onFailure(new IllegalArgumentException(String.format(NOT_FOUND_ERR_MSG, categoryField0)));
                return;
            }

            searchAdInputIndices(detectorId);
        }, error -> {
            String message = String.format("Fail to get the index mapping of %s", anomalyDetector.getIndices());
            logger.error(message, error);
            listener.onFailure(new IllegalArgumentException(message));
        });

        client.execute(GetFieldMappingsAction.INSTANCE, getMappingsRequest, mappingsListener);
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
            String errorMsg = NO_DOCS_IN_USER_INDEX_MSG + Arrays.toString(anomalyDetector.getIndices().toArray(new String[0]));
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
            Instant.now(),
            anomalyDetector.getCategoryField(),
            user
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
                            detector,
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
