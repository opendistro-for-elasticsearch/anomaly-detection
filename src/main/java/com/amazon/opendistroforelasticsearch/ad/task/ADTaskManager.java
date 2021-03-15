/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.ad.task;

import static com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages.DETECTOR_IS_RUNNING;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.DETECTOR_ID_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.ERROR_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.EXECUTION_END_TIME_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.EXECUTION_START_TIME_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.IS_LATEST_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.LAST_UPDATE_TIME_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.STATE_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.STOPPED_BY_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.TASK_TYPE_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.REQUEST_TIMEOUT;
import static com.amazon.opendistroforelasticsearch.ad.util.ExceptionUtil.getErrorMessage;
import static com.amazon.opendistroforelasticsearch.ad.util.ExceptionUtil.getShardsFailure;
import static org.elasticsearch.action.DocWriteResponse.Result.CREATED;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.amazon.opendistroforelasticsearch.ad.caching.PriorityTracker;
import com.amazon.opendistroforelasticsearch.ad.model.DetectionDateRange;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.cluster.HashRing;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ADTaskCancelledException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.DuplicateTaskException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.InternalFailure;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ResourceNotFoundException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskAction;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskProfile;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskState;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskType;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfile;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.AnomalyDetectorFunction;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.IndexAnomalyDetectorJobActionHandler;
import com.amazon.opendistroforelasticsearch.ad.transport.ADBatchAnomalyResultAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADBatchAnomalyResultRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.ADCancelTaskAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADCancelTaskRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.ADTaskProfileAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADTaskProfileNodeResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.ADTaskProfileRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyDetectorJobResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.ForwardADTaskAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ForwardADTaskRequest;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import com.amazon.opendistroforelasticsearch.commons.authuser.User;

/**
 * Manage AD task.
 */
public class ADTaskManager {
    private final Logger logger = LogManager.getLogger(this.getClass());

    private final Client client;
    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;
    private final AnomalyDetectionIndices detectionIndices;
    private final DiscoveryNodeFilterer nodeFilter;
    private final ADTaskCacheManager adTaskCacheManager;

    private final HashRing hashRing;
    private volatile Integer maxAdTaskDocsPerDetector;
    private volatile Integer pieceIntervalSeconds;
    private volatile TimeValue requestTimeout;

    public ADTaskManager(
        Settings settings,
        ClusterService clusterService,
        Client client,
        NamedXContentRegistry xContentRegistry,
        AnomalyDetectionIndices detectionIndices,
        DiscoveryNodeFilterer nodeFilter,
        HashRing hashRing,
        ADTaskCacheManager adTaskCacheManager
    ) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.detectionIndices = detectionIndices;
        this.nodeFilter = nodeFilter;
        this.clusterService = clusterService;
        this.adTaskCacheManager = adTaskCacheManager;
        this.hashRing = hashRing;

        this.maxAdTaskDocsPerDetector = MAX_OLD_AD_TASK_DOCS_PER_DETECTOR.get(settings);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(MAX_OLD_AD_TASK_DOCS_PER_DETECTOR, it -> maxAdTaskDocsPerDetector = it);

        this.pieceIntervalSeconds = BATCH_TASK_PIECE_INTERVAL_SECONDS.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(BATCH_TASK_PIECE_INTERVAL_SECONDS, it -> pieceIntervalSeconds = it);

        this.requestTimeout = REQUEST_TIMEOUT.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(REQUEST_TIMEOUT, it -> requestTimeout = it);
    }

    /**
     * Start detector. Will create schedule job for realtime detector,
     * and start AD task for historical detector.
     *
     * @param detectorId detector id
     * @param detectionDateRange historical analysis date range
     * @param handler anomaly detector job action handler
     * @param user user
     * @param transportService transport service
     * @param listener action listener
     */
    public void startDetector(
        String detectorId,
        DetectionDateRange detectionDateRange,
        IndexAnomalyDetectorJobActionHandler handler,
        User user,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
//        getDetector(detectorId, (detector) -> {
//            if (validateDetector(detector, listener)) {
//                // run realtime detector
//                handler.startAnomalyDetectorJob(detector);
//            }
//        }, (detector) -> {
//            if (validateDetector(detector, listener)) {
//                // run historical detector
//                Optional<DiscoveryNode> owningNode = hashRing.getOwningNode(detector.getDetectorId());
//                if (!owningNode.isPresent()) {
//                    logger.debug("Can't find eligible node to run as AD task's coordinating node");
//                    listener
//                        .onFailure(new ElasticsearchStatusException("No eligible node to run detector", RestStatus.INTERNAL_SERVER_ERROR));
//                    return;
//                }
//                forwardToCoordinatingNode(detector, user, ADTaskAction.START, transportService, owningNode.get(), listener);
//            }
//        }, listener);

        getDetector(detectorId, (detector) -> {
            if (validateDetector(detector, listener)) { // validate if detector is ready to start
                if (detectionDateRange == null) {
                    // run realtime detector
                    // create AD task
                    handler.startAnomalyDetectorJob(detector);
                } else {
                    // run historical detector
                    Optional<DiscoveryNode> owningNode = hashRing.getOwningNode(detector.getDetectorId());
                    if (!owningNode.isPresent()) {
                        logger.debug("Can't find eligible node to run as AD task's coordinating node");
                        listener
                                .onFailure(new ElasticsearchStatusException("No eligible node to run detector", RestStatus.INTERNAL_SERVER_ERROR));
                        return;
                    }
                    logger.info("ylwudebug: coordinating node is : {}", owningNode.get().getId());
                    forwardToCoordinatingNode(detector, null, detectionDateRange, user, ADTaskAction.START, transportService, owningNode.get(), listener);
                }
            }
        }, listener);
    }

    /**
     * We have three types of nodes in AD task process.
     *
     * 1.Forwarding node which receives external request. The request will \
     *   be sent to coordinating node first.
     * 2.Coordinating node which maintains running historical detector set.\
     *   We use hash ring to find coordinating node with detector id. \
     *   Coordinating node will find a worker node with least load and \
     *   dispatch AD task to that worker node.
     * 3.Worker node which will run AD task.
     *
     * This function is to forward the request to coordinating node.
     *
     * @param detector anomaly detector
     * @param user user
     * @param adTaskAction AD task action
     * @param transportService transport service
     * @param node ES node
     * @param listener action listener
     */
    protected void forwardToCoordinatingNode(
        AnomalyDetector detector,
        ADTask adTask,
        DetectionDateRange detectionDateRange,
        User user,
        ADTaskAction adTaskAction,
        TransportService transportService,
        DiscoveryNode node,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        TransportRequestOptions option = TransportRequestOptions
            .builder()
            .withType(TransportRequestOptions.Type.REG)
            .withTimeout(requestTimeout)
            .build();
        transportService
            .sendRequest(
                node,
                ForwardADTaskAction.NAME,
                new ForwardADTaskRequest(detector, adTask, detectionDateRange, user, adTaskAction),
                option,
                new ActionListenerResponseHandler<>(listener, AnomalyDetectorJobResponse::new)
            );
    }

    /**
     * Stop detector.
     * For realtime detector, will set detector job as disabled.
     * For historical detector, will set its AD task as cancelled.
     *
     * @param detectorId detector id
     * @param handler AD job action handler
     * @param user user
     * @param transportService transport service
     * @param listener action listener
     */
    public void stopDetector(
        String detectorId,
        boolean historical,
        IndexAnomalyDetectorJobActionHandler handler,
        User user,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        //TODO: change to get latest AD task to
//        getDetector(
//            detectorId,
//            // stop realtime detector job
//            (detector) -> handler.stopAnomalyDetectorJob(detectorId),
//            // stop historical detector AD task
//            (detector) -> getLatestADTask(
//                detectorId,
//                (task) -> stopHistoricalDetector(detectorId, task, user, listener),
//                transportService,
//                listener
//            ),
//            listener
//        );

        getDetector(
                detectorId,
                detector -> {
                  if (historical) {
                      getLatestADTask(
                              detectorId,
                              ImmutableList.of(ADTaskType.HISTORICAL_HC_DETECTOR, ADTaskType.HISTORICAL_SINGLE_ENTITY),
                              (task) -> stopHistoricalDetector(detectorId, task, user, listener),
                              transportService,
                              listener
                      );
                  } else {
                      handler.stopAnomalyDetectorJob(detectorId);
                  }
                },
                listener
        );
    }

    public <T> void getDetector(
            String detectorId,
            Consumer<AnomalyDetector> consumer,
            ActionListener<T> listener
    ) {
        GetRequest getRequest = new GetRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX).id(detectorId);
        client.get(getRequest, ActionListener.wrap(response -> {
            if (!response.isExists()) {
                listener.onFailure(new ElasticsearchStatusException("AnomalyDetector is not found", RestStatus.NOT_FOUND));
                return;
            }
            try (
                    XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())
            ) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                AnomalyDetector detector = AnomalyDetector.parse(parser, response.getId(), response.getVersion());

                consumer.accept(detector);
//                if (detector.isRealTimeDetector()) {
//                    // run realtime detector
//                    realTimeDetectorConsumer.accept(detector);
//                } else {
//                    // run historical detector
//                    historicalDetectorConsumer.accept(detector);
//                }
            } catch (Exception e) {
                String message = "Failed to start anomaly detector " + detectorId;
                logger.error(message, e);
                listener.onFailure(new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, exception -> listener.onFailure(exception)));
    }

    /*public <T> void getDetector(
        String detectorId,
        Consumer<AnomalyDetector> realTimeDetectorConsumer,
        Consumer<AnomalyDetector> historicalDetectorConsumer,
        ActionListener<T> listener
    ) {
        GetRequest getRequest = new GetRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX).id(detectorId);
        client.get(getRequest, ActionListener.wrap(response -> {
            if (!response.isExists()) {
                listener.onFailure(new ElasticsearchStatusException("AnomalyDetector is not found", RestStatus.NOT_FOUND));
                return;
            }
            try (
                XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())
            ) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                AnomalyDetector detector = AnomalyDetector.parse(parser, response.getId(), response.getVersion());

                if (detector.isRealTimeDetector()) {
                    // run realtime detector
                    realTimeDetectorConsumer.accept(detector);
                } else {
                    // run historical detector
                    historicalDetectorConsumer.accept(detector);
                }
            } catch (Exception e) {
                String message = "Failed to start anomaly detector " + detectorId;
                logger.error(message, e);
                listener.onFailure(new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, exception -> listener.onFailure(exception)));
    }*/

    /**
     * Get latest AD task and execute consumer function.
     *
     * @param detectorId detector id
     * @param function consumer function
     * @param transportService transport service
     * @param listener action listener
     * @param <T> action listerner response
     */
    public <T> void getLatestADTask(
        String detectorId,
        List<ADTaskType> adTaskTypes,
        Consumer<Optional<ADTask>> function,
        TransportService transportService,
        ActionListener<T> listener
    ) {
        getLatestADTask(detectorId, null, adTaskTypes, function,transportService, true, listener);
        /*BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, detectorId));
        query.filter(new TermQueryBuilder(IS_LATEST_FIELD, true));
        if (adTaskTypes != null && adTaskTypes.size() > 0) {
            query.filter(new TermsQueryBuilder(TASK_TYPE_FIELD, adTaskTypes.stream().map(type -> type.name()).collect(Collectors.toList())));
        }
        query.mustNot(new ExistsQueryBuilder("parent_task_id"));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(query);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);
        searchRequest.indices(CommonName.DETECTION_STATE_INDEX);
//        System.out.println("00000000000000000000000000000000000000000000000");
//        System.out.println(sourceBuilder.toString());
//        System.out.println("00000000000000000000000000000000000000000000000");

        client.search(searchRequest, ActionListener.wrap(r -> {
            // https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/359#discussion_r558653132
            // getTotalHits will be null when we track_total_hits is false in the query request.
            // Add more checking here to cover some unknown cases.
            if (r == null || r.getHits().getTotalHits() == null || r.getHits().getTotalHits().value == 0) {
                // don't throw exception here as consumer functions need to handle missing task
                // in different way.
                function.accept(Optional.empty());
                return;
            }
            SearchHit searchHit = r.getHits().getAt(0);
            try (XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, searchHit.getSourceRef())) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                ADTask adTask = ADTask.parse(parser, searchHit.getId());

                //TODO: check realtime detector job and reset realtime task as stopped.
                if (adTask.getDetectionDateRange() != null && !isADTaskEnded(adTask) && lastUpdateTimeExpired(adTask)) {
                    // If AD task is still running, but its last updated time not refreshed
                    // for 2 pieces intervals, we will get task profile to check if it's
                    // really running and reset state as STOPPED if not running.
                    // For example, ES process crashes, then all tasks running on it will stay
                    // as running. We can reset the task state when next read happen.
                    getADTaskProfile(adTask, ActionListener.wrap(taskProfile -> {
                        if (taskProfile.getNodeId() == null) {
                            // If no node is running this task, reset it as STOPPED.
                            resetTaskStateAsStopped(adTask, transportService); //TODO: reset realtime task state
                            adTask.setState(ADTaskState.STOPPED.name());
                        }
                        function.accept(Optional.of(adTask));
                    }, e -> {
                        logger.error("Failed to get AD task profile for task " + adTask.getTaskId(), e);
                        listener.onFailure(e);
                    }));
                } else {
                    function.accept(Optional.of(adTask));
                }
            } catch (Exception e) {
                String message = "Failed to parse AD task for detector " + detectorId;
                logger.error(message, e);
                listener.onFailure(new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                function.accept(Optional.empty());
            } else {
                logger.error("Failed to search AD task for detector " + detectorId, e);
                listener.onFailure(e);
            }
        }));*/
    }

    public <T> void getLatestADTask(
            String detectorId,
            String entityValue,
            List<ADTaskType> adTaskTypes,
            Consumer<Optional<ADTask>> function,
            TransportService transportService,
            boolean resetTaskState,
            ActionListener<T> listener
    ) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, detectorId));
        query.filter(new TermQueryBuilder(IS_LATEST_FIELD, true));
        if (adTaskTypes != null && adTaskTypes.size() > 0) {
            query.filter(new TermsQueryBuilder(TASK_TYPE_FIELD, adTaskTypes.stream().map(type -> type.name()).collect(Collectors.toList())));
        }
        if (Strings.isNotEmpty(entityValue)) {
            String path = "entity";
            String entityValueFieldName = path + ".value";
            TermQueryBuilder entityValueFilterQuery = QueryBuilders.termQuery(entityValueFieldName, entityValue);
            NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(path, entityValueFilterQuery, ScoreMode.None);
            query.filter(nestedQueryBuilder);
        }
//        query.mustNot(new ExistsQueryBuilder("parent_task_id"));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(query);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);
        searchRequest.indices(CommonName.DETECTION_STATE_INDEX);
        logger.info("00000000000000000000000000000000000000000000000 entityValue: {}", entityValue);
        logger.info(sourceBuilder.toString());
        logger.info("00000000000000000000000000000000000000000000000");

        client.search(searchRequest, ActionListener.wrap(r -> {
            // https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/359#discussion_r558653132
            // getTotalHits will be null when we track_total_hits is false in the query request.
            // Add more checking here to cover some unknown cases.
            if (r == null || r.getHits().getTotalHits() == null || r.getHits().getTotalHits().value == 0) {
                // don't throw exception here as consumer functions need to handle missing task
                // in different way.
                function.accept(Optional.empty());
                return;
            }
            SearchHit searchHit = r.getHits().getAt(0);
            try (XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, searchHit.getSourceRef())) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                ADTask adTask = ADTask.parse(parser, searchHit.getId());

                //TODO: check realtime detector job and reset realtime task as stopped.
                if (resetTaskState && adTask.getDetectionDateRange() != null && !isADTaskEnded(adTask) && lastUpdateTimeExpired(adTask)) {
                    // If AD task is still running, but its last updated time not refreshed
                    // for 2 pieces intervals, we will get task profile to check if it's
                    // really running and reset state as STOPPED if not running.
                    // For example, ES process crashes, then all tasks running on it will stay
                    // as running. We can reset the task state when next read happen.
                    getADTaskProfile(adTask, ActionListener.wrap(taskProfile -> {
                        if (taskProfile.getNodeId() == null) {
                            // If no node is running this task, reset it as STOPPED.
                            resetTaskStateAsStopped(adTask, transportService); //TODO: reset realtime task state
                            adTask.setState(ADTaskState.STOPPED.name());
                        }
                        function.accept(Optional.of(adTask));
                    }, e -> {
                        logger.error("Failed to get AD task profile for task " + adTask.getTaskId(), e);
                        listener.onFailure(e);
                    }));
                } else {
                    function.accept(Optional.of(adTask));
                }
            } catch (Exception e) {
                String message = "Failed to parse AD task for detector " + detectorId;
                logger.error(message, e);
                listener.onFailure(new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                function.accept(Optional.empty());
            } else {
                logger.error("Failed to search AD task for detector " + detectorId, e);
                listener.onFailure(e);
            }
        }));
    }

    private void stopHistoricalDetector(
        String detectorId,
        Optional<ADTask> adTask,
        User user,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        if (!adTask.isPresent()) {
            listener.onFailure(new ResourceNotFoundException(detectorId, "Detector not started"));
            return;
        }

        if (isADTaskEnded(adTask.get())) {
            listener.onFailure(new ResourceNotFoundException(detectorId, "No running task found"));
            return;
        }

        String taskId = adTask.get().getTaskId();
        DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
        String userName = user == null ? null : user.getName();

        ADCancelTaskRequest cancelTaskRequest = new ADCancelTaskRequest(detectorId, userName, dataNodes);
        client
            .execute(
                ADCancelTaskAction.INSTANCE,
                cancelTaskRequest,
                ActionListener
                    .wrap(response -> { listener.onResponse(new AnomalyDetectorJobResponse(taskId, 0, 0, 0, RestStatus.OK)); }, e -> {
                        logger.error("Failed to cancel AD task " + taskId + ", detector id: " + detectorId, e);
                        listener.onFailure(e);
                    })
            );
    }

    private boolean lastUpdateTimeExpired(ADTask adTask) {
        return adTask.getLastUpdateTime().plus(2 * pieceIntervalSeconds, ChronoUnit.SECONDS).isBefore(Instant.now());
    }

    public boolean isADTaskEnded(ADTask adTask) {
        return ADTaskState.STOPPED.name().equals(adTask.getState())
            || ADTaskState.FINISHED.name().equals(adTask.getState())
            || ADTaskState.FAILED.name().equals(adTask.getState());
    }

    private void resetTaskStateAsStopped(ADTask adTask, TransportService transportService) {
        if (!isADTaskEnded(adTask)) {
            cleanDetectorCache(adTask, transportService, () -> {
                Map<String, Object> updatedFields = new HashMap<>();
                updatedFields.put(STATE_FIELD, ADTaskState.STOPPED.name());
                updateADTask(adTask.getTaskId(), updatedFields);
                logger.debug("reset task as stopped, task id " + adTask.getTaskId());
            });
        }
    }

    /**
     * Clean detector cache on coordinating node.
     * If task's coordinating node is still in cluster, will forward stop
     * task request to coordinating node, then coordinating node will
     * remove detector from cache.
     * If task's coordinating node is not in cluster, we don't need to
     * forward stop task request to coordinating node.
     *
     * @param adTask AD task
     * @param transportService transport service
     * @param function will execute it when detector cache cleaned successfully or coordinating node left cluster
     */
    protected void cleanDetectorCache(ADTask adTask, TransportService transportService, AnomalyDetectorFunction function) {
        String coordinatingNode = adTask.getCoordinatingNode();
        DiscoveryNode[] eligibleDataNodes = nodeFilter.getEligibleDataNodes();
        logger.debug("coordinatingNode is: " + coordinatingNode + " for task " + adTask.getTaskId());
        DiscoveryNode targetNode = null;
        for (DiscoveryNode node : eligibleDataNodes) {
            if (node.getId().equals(coordinatingNode)) {
                targetNode = node;
                break;
            }
        }
        if (targetNode != null) {
            logger.debug("coordinatingNode found, will clean detector cache on it, detectorId: " + adTask.getDetectorId());
            forwardToCoordinatingNode(
                adTask.getDetector(), null,
                adTask.getDetectionDateRange(),
                null,
                ADTaskAction.STOP,
                transportService,
                targetNode,
                ActionListener
                    .wrap(
                        r -> {
                            function.execute();
                            },
                        e -> {
                            logger.error("Failed to clear detector cache on coordinating node " + coordinatingNode, e);
                        }
                    )
            );
        } else {
            logger
                .warn(
                    "coordinating node"
                        + coordinatingNode
                        + " left cluster for detector "
                        + adTask.getDetectorId()
                        + ", task id "
                        + adTask.getTaskId()
                );
            function.execute();
        }
    }


    protected void entityTaskDone(ADTask adTask, TransportService transportService, AnomalyDetectorFunction function) {
        String coordinatingNode = adTask.getCoordinatingNode();
        DiscoveryNode[] eligibleDataNodes = nodeFilter.getEligibleDataNodes();
        logger.debug("coordinatingNode is: " + coordinatingNode + " for entity task " + adTask.getTaskId());
        DiscoveryNode targetNode = null;
        for (DiscoveryNode node : eligibleDataNodes) {
            if (node.getId().equals(coordinatingNode)) {
                targetNode = node;
                break;
            }
        }
        if (targetNode != null) {
            logger.debug("coordinatingNode found, will clean detector cache on it, detectorId: " + adTask.getDetectorId());
            forwardToCoordinatingNode(
                    adTask.getDetector(),
                    adTask,
                    adTask.getDetectionDateRange(),
                    null,
                    ADTaskAction.NEXT_ENTITY,
                    transportService,
                    targetNode,
                    ActionListener
                            .wrap(
                                    r -> {
                                        function.execute();
                                    },
                                    e -> {
                                        logger.error("Failed to clear detector cache on coordinating node " + coordinatingNode, e);
                                    }
                            )
            );
        } else {
            logger
                    .warn(
                            "coordinating node"
                                    + coordinatingNode
                                    + " left cluster for detector "
                                    + adTask.getDetectorId()
                                    + ", task id "
                                    + adTask.getTaskId()
                    );
            function.execute();
        }
    }

    /**
     * Get AD task profile data.
     *
     * @param detectorId detector id
     * @param transportService transport service
     * @param listener action listener
     */
    public void getLatestADTaskProfile(String detectorId, TransportService transportService, ActionListener<DetectorProfile> listener) {
        getLatestADTask(detectorId, null, adTask -> {
            if (adTask.isPresent()) {
                getADTaskProfile(adTask.get(), ActionListener.wrap(adTaskProfile -> {
                    DetectorProfile.Builder profileBuilder = new DetectorProfile.Builder();
                    profileBuilder.adTaskProfile(adTaskProfile);
                    listener.onResponse(profileBuilder.build());
                }, e -> {
                    logger.error("Failed to get AD task profile for task " + adTask.get().getTaskId(), e);
                    listener.onFailure(e);
                }));
            } else {
                listener.onFailure(new ResourceNotFoundException(detectorId, "Can't find latest task for detector"));
            }
        }, transportService, listener);
    }

    private void getADTaskProfile(ADTask adTask, ActionListener<ADTaskProfile> listener) {
        String detectorId = adTask.getDetectorId();

        DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
        ADTaskProfileRequest adTaskProfileRequest = new ADTaskProfileRequest(detectorId, dataNodes);
        client.execute(ADTaskProfileAction.INSTANCE, adTaskProfileRequest, ActionListener.wrap(response -> {
            if (response.hasFailures()) {
                listener.onFailure(response.failures().get(0));
                return;
            }

            List<ADTaskProfile> nodeResponses = response
                .getNodes()
                .stream()
                .filter(r -> r.getAdTaskProfile() != null)
                .map(ADTaskProfileNodeResponse::getAdTaskProfile)
                .collect(Collectors.toList());

            if (nodeResponses.size() > 1) {
                String error = nodeResponses.size()
                    + " tasks running for detector "
                    + adTask.getDetectorId()
                    + ". Please stop detector to kill all running tasks.";
                logger.error(error);
                listener.onFailure(new InternalFailure(adTask.getDetectorId(), error));
                return;
            }
            if (nodeResponses.size() == 0) {
                ADTaskProfile adTaskProfile = new ADTaskProfile(adTask, null, null, null, null, null, null);
                listener.onResponse(adTaskProfile);
            } else {
                ADTaskProfile nodeResponse = nodeResponses.get(0);
                ADTaskProfile adTaskProfile = new ADTaskProfile(
                    adTask,
                    nodeResponse.getShingleSize(),
                    nodeResponse.getRcfTotalUpdates(),
                    nodeResponse.getThresholdModelTrained(),
                    nodeResponse.getThresholdModelTrainingDataSize(),
                    nodeResponse.getModelSizeInBytes(),
                    nodeResponse.getNodeId()
                );
                listener.onResponse(adTaskProfile);
            }
        }, e -> {
            logger.error("Failed to get task profile for task " + adTask.getTaskId(), e);
            listener.onFailure(e);
        }));
    }

    /**
     * Get task profile for detector.
     *
     * @param detectorId detector id
     * @return AD task profile
     * @throws LimitExceededException if there are multiple tasks for the detector
     */
    public ADTaskProfile getLocalADTaskProfileByDetectorId(String detectorId) {
        ADTaskProfile adTaskProfile = null;
        List<String> tasksOfDetector = adTaskCacheManager.getTasksOfDetector(detectorId);
        if (tasksOfDetector.size() > 1) {
            String error = "Multiple tasks are running for detector: " + detectorId + ". You can stop detector to kill all running tasks.";
            logger.warn(error);
            throw new LimitExceededException(error);
        }
        if (tasksOfDetector.size() == 1) {
            String taskId = tasksOfDetector.get(0);
            adTaskProfile = new ADTaskProfile(
                adTaskCacheManager.getShingle(taskId).size(),
                adTaskCacheManager.getRcfModel(taskId).getTotalUpdates(),
                adTaskCacheManager.isThresholdModelTrained(taskId),
                adTaskCacheManager.getThresholdModelTrainingDataSize(taskId),
                adTaskCacheManager.getModelSize(taskId),
                clusterService.localNode().getId()
            );
        }
        return adTaskProfile;
    }

    private boolean validateDetector(AnomalyDetector detector, ActionListener<AnomalyDetectorJobResponse> listener) {
        String error = null;
        if (detector.getFeatureAttributes().size() == 0) {
            error = "Can't start detector job as no features configured";
        } else if (detector.getEnabledFeatureIds().size() == 0) {
            error = "Can't start detector job as no enabled features configured";
        }
        if (error != null) {
            listener.onFailure(new ElasticsearchStatusException(error, RestStatus.BAD_REQUEST));
            return false;
        }
        return true;
    }

    /**
     * Start historical detector on coordinating node.
     * Will init task index if not exist and write new AD task to index. If task index
     * exists, will check if there is task running. If no running task, reset old task
     * as not latest and clean old tasks which exceeds limitation. Then find out node
     * with least load and dispatch task to that node(worker node).
     *
     * @param detector anomaly detector
     * @param user user
     * @param transportService transport service
     * @param listener action listener
     */
    public void startHistoricalDetector(
        AnomalyDetector detector,
        DetectionDateRange detectionDateRange,
        User user,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        logger.info("ylwudebug : coordinate node is : {}", clusterService.localNode().getId());
        try {
            if (detectionIndices.doesDetectorStateIndexExist()) {
                // If detection index exist, check if latest AD task is running
                getLatestADTask(detector.getDetectorId(), ImmutableList.of(ADTaskType.HISTORICAL_HC_DETECTOR, ADTaskType.HISTORICAL_SINGLE_ENTITY),(adTask) -> {
                    if (!adTask.isPresent() || isADTaskEnded(adTask.get())) {
                        executeHistoricalDetector(detector, detectionDateRange, user, listener);
                    } else {
                        listener.onFailure(new ElasticsearchStatusException(DETECTOR_IS_RUNNING, RestStatus.BAD_REQUEST));
                    }
                }, transportService, listener);
            } else {
                // If detection index doesn't exist, create index and execute historical detector.
                detectionIndices.initDetectionStateIndex(ActionListener.wrap(r -> {
                    if (r.isAcknowledged()) {
                        logger.info("Created {} with mappings.", CommonName.DETECTION_STATE_INDEX);
                        executeHistoricalDetector(detector, detectionDateRange, user, listener);
                    } else {
                        String error = "Create index " + CommonName.DETECTION_STATE_INDEX + " with mappings not acknowledged";
                        logger.warn(error);
                        listener.onFailure(new ElasticsearchStatusException(error, RestStatus.INTERNAL_SERVER_ERROR));
                    }
                }, e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        executeHistoricalDetector(detector, detectionDateRange, user, listener);
                    } else {
                        logger.error("Failed to init anomaly detection state index", e);
                        listener.onFailure(e);
                    }
                }));
            }
        } catch (Exception e) {
            logger.error("Failed to start historical detector " + detector.getDetectorId(), e);
            listener.onFailure(e);
        }
    }


    public void startAnomalyDetector(
            AnomalyDetector detector,
            DetectionDateRange detectionDateRange,
            User user,
            TransportService transportService,
            ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        logger.info("ylwudebug : coordinate node is : {}", clusterService.localNode().getId());
        try {
            if (detectionIndices.doesDetectorStateIndexExist()) {
                // If detection index exist, check if latest AD task is running
                getLatestADTask(detector.getDetectorId(), ImmutableList.of(getADTaskType(detector, detectionDateRange)), (adTask) -> {
                    if (!adTask.isPresent() || isADTaskEnded(adTask.get())) {
                        executeAnomalyDetector(detector, detectionDateRange, user, listener);
                    } else {
                        listener.onFailure(new ElasticsearchStatusException(DETECTOR_IS_RUNNING, RestStatus.BAD_REQUEST));
                    }
                }, transportService, listener);
            } else {
                // If detection index doesn't exist, create index and execute historical detector.
                detectionIndices.initDetectionStateIndex(ActionListener.wrap(r -> {
                    if (r.isAcknowledged()) {
                        logger.info("Created {} with mappings.", CommonName.DETECTION_STATE_INDEX);
                        executeAnomalyDetector(detector, detectionDateRange, user, listener);
                    } else {
                        String error = "Create index " + CommonName.DETECTION_STATE_INDEX + " with mappings not acknowledged";
                        logger.warn(error);
                        listener.onFailure(new ElasticsearchStatusException(error, RestStatus.INTERNAL_SERVER_ERROR));
                    }
                }, e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        executeAnomalyDetector(detector, detectionDateRange, user, listener);
                    } else {
                        logger.error("Failed to init anomaly detection state index", e);
                        listener.onFailure(e);
                    }
                }));
            }
        } catch (Exception e) {
            logger.error("Failed to start historical detector " + detector.getDetectorId(), e);
            listener.onFailure(e);
        }
    }

    private void executeAnomalyDetector(AnomalyDetector detector, DetectionDateRange detectionDateRange, User user, ActionListener<AnomalyDetectorJobResponse> listener) {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
        updateByQueryRequest.indices(CommonName.DETECTION_STATE_INDEX);
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, detector.getDetectorId()));
        query.filter(new TermQueryBuilder(IS_LATEST_FIELD, true));
        updateByQueryRequest.setQuery(query);
        updateByQueryRequest.setRefresh(true);
        updateByQueryRequest.setScript(new Script("ctx._source.is_latest = false;"));

        client.execute(UpdateByQueryAction.INSTANCE, updateByQueryRequest, ActionListener.wrap(r -> {
            List<BulkItemResponse.Failure> bulkFailures = r.getBulkFailures();
            if (bulkFailures.isEmpty()) {
                createADTaskDoc(detector, detectionDateRange, user, listener);
            } else {
                logger.error("Failed to update old task's state for detector: {}, response: {} ", detector.getDetectorId(), r.toString());
                listener.onFailure(bulkFailures.get(0).getCause());
            }
        }, e -> {
            logger.error("Failed to reset old tasks as not latest for detector " + detector.getDetectorId(), e);
            listener.onFailure(e);
        }));
    }

    private void createADTaskDoc(AnomalyDetector detector, DetectionDateRange detectionDateRange, User user, ActionListener<AnomalyDetectorJobResponse> listener) {
        String userName = user == null ? null : user.getName();
        Instant now = Instant.now();
        String taskTYpe = getADTaskType(detector, detectionDateRange).name();
        ADTask adTask = new ADTask.Builder()
                .detectorId(detector.getDetectorId())
                .detector(detector)
                .isLatest(true)
                .taskType(taskTYpe)
                .executionStartTime(now)
                .taskProgress(0.0f)
                .initProgress(0.0f)
                .state(ADTaskState.CREATED.name())
                .lastUpdateTime(now)
                .startedBy(userName)
                .coordinatingNode(clusterService.localNode().getId())
                .detectionDateRange(detectionDateRange)
                .user(user)
                .build();

        IndexRequest request = new IndexRequest(CommonName.DETECTION_STATE_INDEX);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            request
                    .source(adTask.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE))
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            client
                    .index(
                            request,
                            ActionListener
                                    .wrap(
                                            r -> onIndexADTaskResponse(
                                                    r,
                                                    adTask,
                                                    (response, delegatedListener) -> {
//                                                        if (adTask.getDetector().isMultientityDetector() && detectionDateRange != null) {
//                                                            getTopEntities(adTask, () -> cleanOldAdTasks(response, adTask, delegatedListener), delegatedListener);
//                                                        } else {
//                                                            cleanOldAdTasks(response, adTask, delegatedListener);
//                                                        }
                                                        cleanOldAdTasks(response, adTask, delegatedListener);
                                                    },
                                                    listener
                                            ),
                                            e -> {
                                                logger.error("Failed to create AD task for detector " + detector.getDetectorId(), e);
                                                listener.onFailure(e);
                                            }
                                    )
                    );
        } catch (Exception e) {
            logger.error("Failed to create AD task for detector " + detector.getDetectorId(), e);
            listener.onFailure(e);
        }
    }

    private ADTaskType getADTaskType(AnomalyDetector detector, DetectionDateRange detectionDateRange) {
        if (detectionDateRange == null) {
            return detector.isMultientityDetector()? ADTaskType.REALTIME_HC_DETECTOR : ADTaskType.REALTIME_SINGLE_ENTITY;
        } else {
            return detector.isMultientityDetector()? ADTaskType.HISTORICAL_HC_DETECTOR : ADTaskType.HISTORICAL_SINGLE_ENTITY;
        }
    }

    private void cleanOldAdTasks(IndexResponse response, ADTask adTask,
                                 ActionListener<AnomalyDetectorJobResponse> listener) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, adTask.getDetectorId()));
        query.filter(new TermQueryBuilder(IS_LATEST_FIELD, false));
        if (adTask.getDetectionDateRange() != null) {
            query.filter(new TermsQueryBuilder(TASK_TYPE_FIELD, ADTaskType.HISTORICAL_SINGLE_ENTITY.name(), ADTaskType.HISTORICAL_HC_DETECTOR.name()));
        } else {
            query.filter(new TermsQueryBuilder(TASK_TYPE_FIELD, ADTaskType.REALTIME_SINGLE_ENTITY.name(), ADTaskType.REALTIME_HC_DETECTOR.name()));
        }
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder
                .query(query)
                .sort(EXECUTION_START_TIME_FIELD, SortOrder.DESC)
                // Search query "from" starts from 0.
                .from(maxAdTaskDocsPerDetector - 1)
                .trackTotalHits(true)
                .size(1);
        searchRequest.source(sourceBuilder).indices(CommonName.DETECTION_STATE_INDEX);
        String detectorId = adTask.getDetectorId();

        client.search(searchRequest, ActionListener.wrap(r -> {
            Iterator<SearchHit> iterator = r.getHits().iterator();
            if (iterator.hasNext()) {
                logger
                        .debug(
                                "AD tasks count for detector {} is {}, exceeds limit of {}",
                                detectorId,
                                r.getHits().getTotalHits().value,
                                maxAdTaskDocsPerDetector
                        );
                SearchHit searchHit = r.getHits().getAt(0);
                try (
                        XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, searchHit.getSourceRef())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    ADTask task = ADTask.parse(parser, searchHit.getId());

                    DeleteByQueryRequest request = new DeleteByQueryRequest(CommonName.DETECTION_STATE_INDEX);
                    RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(EXECUTION_START_TIME_FIELD);
                    rangeQueryBuilder.lt(task.getExecutionStartTime().toEpochMilli()).format("epoch_millis");
                    request.setQuery(rangeQueryBuilder);
                    client.execute(DeleteByQueryAction.INSTANCE, request, ActionListener.wrap(res -> {
                        logger
                                .debug(
                                        "Deleted {} old AD tasks started equals or before {} for detector {}",
                                        res.getDeleted(),
                                        adTask.getExecutionStartTime().toEpochMilli(),
                                        detectorId
                                );

                        //TODO: create new method to avoid duplicate code
                        if (adTask.getDetectionDateRange() != null) {
                            /*if (adTask.getDetector().isMultientityDetector() && adTask.getDetectionDateRange() != null) {
                                getTopEntities(adTask, () -> runBatchResultAction(response, adTask, listener), listener);
                            } else {
                                runBatchResultAction(response, adTask, listener);
                            }*/
                            runBatchResultAction(response, adTask, listener);
                        } else {
                            AnomalyDetectorJobResponse anomalyDetectorJobResponse = new AnomalyDetectorJobResponse(
                                    response.getId(),
                                    response.getVersion(),
                                    response.getSeqNo(),
                                    response.getPrimaryTerm(),
                                    RestStatus.OK
                            );
                            listener.onResponse(anomalyDetectorJobResponse);
                        }
                    }, e -> {
                        logger.warn("Failed to clean AD tasks for detector " + detectorId, e);
                        listener.onFailure(e);
                    }));
                } catch (Exception e) {
                    logger.warn("Failed to parse AD tasks for detector " + detectorId, e);
                    listener.onFailure(e);
                }
            } else {
                //TODO: create new method to avoid duplicate code
                if (adTask.getDetectionDateRange() != null) {
                    /*if (adTask.getDetector().isMultientityDetector() && adTask.getDetectionDateRange() != null) {
                        getTopEntities(adTask, () -> runBatchResultAction(response, adTask, listener), listener);
                    } else {
                        runBatchResultAction(response, adTask, listener);
                    }*/
                    runBatchResultAction(response, adTask, listener);
                } else {
                    AnomalyDetectorJobResponse anomalyDetectorJobResponse = new AnomalyDetectorJobResponse(
                            response.getId(), //TODO: change to detector id? Add new task id field
                            response.getVersion(),
                            response.getSeqNo(),
                            response.getPrimaryTerm(),
                            RestStatus.OK
                    );
                    listener.onResponse(anomalyDetectorJobResponse);
                }
            }
        }, e -> {
            logger.warn("Failed to search AD tasks for detector " + detectorId, e);
            listener.onFailure(e);
        }));
    }

    private void executeHistoricalDetector(AnomalyDetector detector, DetectionDateRange detectionDateRange, User user, ActionListener<AnomalyDetectorJobResponse> listener) {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
        updateByQueryRequest.indices(CommonName.DETECTION_STATE_INDEX);
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, detector.getDetectorId()));
        query.filter(new TermQueryBuilder(IS_LATEST_FIELD, true));
        updateByQueryRequest.setQuery(query);
        updateByQueryRequest.setRefresh(true);
        updateByQueryRequest.setScript(new Script("ctx._source.is_latest = false;"));

        client.execute(UpdateByQueryAction.INSTANCE, updateByQueryRequest, ActionListener.wrap(r -> {
            List<BulkItemResponse.Failure> bulkFailures = r.getBulkFailures();
            if (bulkFailures.isEmpty()) {
                createNewADTask(detector, detectionDateRange, user, listener);
            } else {
                logger.error("Failed to update old task's state for detector: {}, response: {} ", detector.getDetectorId(), r.toString());
                listener.onFailure(bulkFailures.get(0).getCause());
            }
        }, e -> {
            logger.error("Failed to reset old tasks as not latest for detector " + detector.getDetectorId(), e);
            listener.onFailure(e);
        }));
    }

    private void createNewADTask(AnomalyDetector detector, DetectionDateRange detectionDateRange, User user, ActionListener<AnomalyDetectorJobResponse> listener) {
        String userName = user == null ? null : user.getName();
        Instant now = Instant.now();
        String taskTYpe = detector.isMultientityDetector()? ADTaskType.HISTORICAL_HC_DETECTOR.name() : ADTaskType.HISTORICAL_SINGLE_ENTITY.name();
        ADTask adTask = new ADTask.Builder()
            .detectorId(detector.getDetectorId())
            .detector(detector)
            .isLatest(true)
            .taskType(taskTYpe)
            .executionStartTime(now)
            .taskProgress(0.0f)
            .initProgress(0.0f)
            .state(ADTaskState.CREATED.name())
            .lastUpdateTime(now)
            .startedBy(userName)
            .coordinatingNode(clusterService.localNode().getId())
            .detectionDateRange(detectionDateRange)
            .user(user)
            .build();

        IndexRequest request = new IndexRequest(CommonName.DETECTION_STATE_INDEX);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            request
                .source(adTask.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            client
                .index(
                    request,
                    ActionListener
                        .wrap(
                            r -> onIndexADTaskResponse(
                                r,
                                adTask,
                                (response, delegatedListener) -> {
                                    /*if (adTask.getDetector().isMultientityDetector()){
                                        getTopEntities(adTask, () -> cleanOldAdTaskDocs(response, adTask, delegatedListener), delegatedListener);
                                    } else {
                                        cleanOldAdTaskDocs(response, adTask, delegatedListener);
                                    }*/
                                    cleanOldAdTaskDocs(response, adTask, delegatedListener);
                                },
                                listener
                            ),
                            e -> {
                                logger.error("Failed to create AD task for detector " + detector.getDetectorId(), e);
                                listener.onFailure(e);
                            }
                        )
                );
        } catch (Exception e) {
            logger.error("Failed to create AD task for detector " + detector.getDetectorId(), e);
            listener.onFailure(e);
        }
    }


    public <T> void createADTask(ADTask adTask, Consumer<IndexResponse> function, ActionListener<T> listener) {
        IndexRequest request = new IndexRequest(CommonName.DETECTION_STATE_INDEX);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            request
                    .source(adTask.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE))
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            client
                    .index(
                            request,
                            ActionListener
                                    .wrap(
                                            r -> function.accept(r),
//                                                    onIndexADTaskResponse(
//                                                    r,
//                                                    adTask,
//                                                    (response, delegatedListener) -> cleanOldAdTaskDocs(response, adTask, delegatedListener),
//                                                    listener
//                                            ),
                                            e -> {
                                                logger.error("Failed to create AD task for detector " + adTask.getDetectorId(), e);
                                                listener.onFailure(e);
                                            }
                                    )
                    );
        } catch (Exception e) {
            logger.error("Failed to create AD task for detector " + adTask.getDetectorId(), e);
            listener.onFailure(e);
        }
    }

    private void onIndexADTaskResponse(
        IndexResponse response,
        ADTask adTask,
        BiConsumer<IndexResponse, ActionListener<AnomalyDetectorJobResponse>> function,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        if (response == null || response.getResult() != CREATED) {
            String errorMsg = getShardsFailure(response);
            listener.onFailure(new ElasticsearchStatusException(errorMsg, response.status()));
            return;
        }
        adTask.setTaskId(response.getId());
        ActionListener<AnomalyDetectorJobResponse> delegatedListener = ActionListener.wrap(r -> { listener.onResponse(r); }, e -> {
            handleADTaskException(adTask, e);
            if (e instanceof DuplicateTaskException) {
                listener.onFailure(new ElasticsearchStatusException(DETECTOR_IS_RUNNING, RestStatus.BAD_REQUEST));
            } else {
                listener.onFailure(e);
                if (!adTask.getTaskType().startsWith("HISTORICAL_HC_")) {
                    adTaskCacheManager.removeDetector(adTask.getDetectorId());
                }

            }
        });
        try {
            // Put detector id in cache. If detector id already in cache, will throw
            // DuplicateTaskException. This is to solve race condition when user send
            // multiple start request for one historical detector.
            if (adTask.getDetectionDateRange() != null) {
                adTaskCacheManager.add(adTask.getDetectorId());
            }
        } catch (Exception e) {
            delegatedListener.onFailure(e);
            return;
        }
        if (function != null) {
            function.accept(response, delegatedListener);
        }
    }

    //TODO: calculate top entities with the same algorithm of realtime detector
//    private void getTopEntities(ADTask adTask, AnomalyDetectorFunction consumer, ActionListener<AnomalyDetectorJobResponse> delegatedListener) {
//        PriorityTracker priorityTracker = new PriorityTracker(Clock.systemUTC(),
//                adTask.getDetector().getDetectorIntervalInSeconds(),
//                adTask.getDetectionDateRange().getStartTime().toEpochMilli(),
//                10_000);
//
//        long dataStartTime = adTask.getDetectionDateRange().getStartTime().toEpochMilli();
//        long dataEndTime = adTask.getDetectionDateRange().getEndTime().toEpochMilli();
//        long interval = adTask.getDetector().getDetectorIntervalInMilliseconds();
//        dataStartTime = dataStartTime - dataStartTime % interval;
//        dataEndTime = dataEndTime - dataEndTime % interval;
//
//        final CountDownLatch latch = new CountDownLatch(1);
//        long s = System.currentTimeMillis();
//        long c = 0;
//        for (long t = dataStartTime; t < dataEndTime; t += interval) {
//            c++;
//            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
//            RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(adTask.getDetector().getTimeField()).gte(dataStartTime).lte(dataEndTime).format("epoch_millis");;
//            boolQueryBuilder.filter(rangeQueryBuilder);
//            boolQueryBuilder.filter(adTask.getDetector().getFilterQuery());
//            sourceBuilder.query(boolQueryBuilder);
//
//            String topEntitiesAgg = "topEntities";
//            AggregationBuilder aggregation = new TermsAggregationBuilder(topEntitiesAgg).field(adTask.getDetector().getCategoryField().get(0)).size(1000);
//            sourceBuilder.aggregation(aggregation).size(0);
//            //TODO: add historical date range
//            SearchRequest searchRequest = new SearchRequest();
//            searchRequest.source(sourceBuilder);
//            searchRequest.indices(adTask.getDetector().getIndices().toArray(new String[0]));
////            logger.info("++++++++++++++++++++++++++++++++");
////            logger.info(sourceBuilder.toString());
////            logger.info("++++++++++++++++++++++++++++++++ start search top entities");
//
//            client.search(searchRequest, new LatchedActionListener<>(
//                    ActionListener.wrap(r -> {
//                        StringTerms a = r.getAggregations().get(topEntitiesAgg);
//                        List<StringTerms.Bucket> buckets = a.getBuckets();
//                        List<String> topEntities = new ArrayList<>();
//                        long total = r.getHits().getTotalHits().value;
////                        logger.info("++++++++++++++++++++++++++++++++ finish search top entities");
//                        for (StringTerms.Bucket b : buckets) {
//                            //TODO: fix stats
//                            String key = b.getKeyAsString();
//                            long count = b.getDocCount();
//                            topEntities.add(key);
////                            logger.info(key + ", " + count);
//                        }
////                        logger.info("++++++++++++++++++++++++++++++++ entities");
//
//                        topEntities.forEach(e -> priorityTracker.updatePriority(e));
//
//
////                adTaskCacheManager.addEntities(adTask.getDetectorId(), topEntities);
////                consumer.execute();
//                    }, e -> {
//                        logger.error("Failed to get top entities for detector " + adTask.getDetectorId(), e);
////                        delegatedListener.onFailure(e);
//                    }), latch));
//        }
//        try {
//            if (!latch.await(10, TimeUnit.SECONDS)) {
//                throw new ElasticsearchTimeoutException("Timeout when get top entities for detector " + adTask.getDetectorId());
//            }
//
//        } catch (Exception e) {
//            delegatedListener.onFailure(e);
//        }
//        long e = System.currentTimeMillis();
//        System.out.println("00000000000000000000000000000000000011111111 , totaltimeUsed: " + (e - s));
//        List<String> topEntities = priorityTracker.getTopNEntities(1000);
//        adTaskCacheManager.addEntities(adTask.getDetectorId(), topEntities);
//        consumer.execute();
//    }

//    public void getTopEntities(ADTask adTask, AnomalyDetectorFunction consumer, ActionListener<AnomalyDetectorJobResponse> delegatedListener) {
//        PriorityTracker priorityTracker = new PriorityTracker(Clock.systemUTC(),
//                adTask.getDetector().getDetectorIntervalInSeconds(),
//                adTask.getDetectionDateRange().getStartTime().toEpochMilli(),
//                10_000);
//
//        long startTime = adTask.getDetectionDateRange().getStartTime().toEpochMilli();
//        long endTime = adTask.getDetectionDateRange().getEndTime().toEpochMilli();
//        long interval = adTask.getDetector().getDetectorIntervalInMilliseconds();
//        startTime = startTime - startTime % interval;
//        endTime = endTime - endTime % interval;
//        logger.info("start to search top entities at " + System.currentTimeMillis());
//        searchTopEntities(adTask, priorityTracker, endTime, (endTime - startTime) / 1000,
//                startTime,startTime + interval,
//                consumer, delegatedListener);
//    }
//
//    private void searchTopEntities(ADTask adTask, PriorityTracker priorityTracker, long detectionEndTime, long interval,
//                                   long dataStartTime, long dataEndTime,
//                                   AnomalyDetectorFunction consumer,
//                                   ActionListener<AnomalyDetectorJobResponse> delegatedListener) {
//        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
//        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(adTask.getDetector().getTimeField()).gte(dataStartTime).lte(dataEndTime).format("epoch_millis");;
//        boolQueryBuilder.filter(rangeQueryBuilder);
//        boolQueryBuilder.filter(adTask.getDetector().getFilterQuery());
//        sourceBuilder.query(boolQueryBuilder);
//
//        String topEntitiesAgg = "topEntities";
//        AggregationBuilder aggregation = new TermsAggregationBuilder(topEntitiesAgg).field(adTask.getDetector().getCategoryField().get(0)).size(1000);
//        sourceBuilder.aggregation(aggregation).size(0);
//        //TODO: add historical date range
//        SearchRequest searchRequest = new SearchRequest();
//        searchRequest.source(sourceBuilder);
//        searchRequest.indices(adTask.getDetector().getIndices().toArray(new String[0]));
//        client.search(searchRequest, ActionListener.wrap(r -> {
//            StringTerms a = r.getAggregations().get(topEntitiesAgg);
//            List<StringTerms.Bucket> buckets = a.getBuckets();
//            List<String> topEntities = new ArrayList<>();
//            for (StringTerms.Bucket b : buckets) {
//                //TODO: fix stats
//                String key = b.getKeyAsString();
//                topEntities.add(key);
//            }
//
//            topEntities.forEach(e -> priorityTracker.updatePriority(e));
//            if (dataEndTime < detectionEndTime) {
////                System.out.println("dataEndTimeYlwuDebug: " + dataEndTime);
//                searchTopEntities(adTask, priorityTracker, detectionEndTime, interval,
//                        dataEndTime, dataEndTime + interval,
//                        consumer, delegatedListener);
//            } else {
//                logger.info("finish to search top entities at " + System.currentTimeMillis());
//                List<String> topNEntities = priorityTracker.getTopNEntities(1000);
//                adTaskCacheManager.addEntities(adTask.getDetectorId(), topNEntities);
//                adTaskCacheManager.setEntityCount(adTask.getDetectorId(), topNEntities.size());
//                consumer.execute();
//            }
//        }, e -> {
//            logger.error("Failed to get top entities for detector " + adTask.getDetectorId(), e);
//                        delegatedListener.onFailure(e);
//        }));
//    }
    //TODO: change this for HC detector as every entity will have one task. When delete old detector level task, delete
    // all entity task with "parent task id = detector task id", delete with cron job?
    private void cleanOldAdTaskDocs(IndexResponse response, ADTask adTask, ActionListener<AnomalyDetectorJobResponse> listener) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, adTask.getDetectorId()));
        query.filter(new TermQueryBuilder(IS_LATEST_FIELD, false));
        if (adTask.getDetector().isMultientityDetector()) {
            query.filter(new TermQueryBuilder(TASK_TYPE_FIELD, ADTaskType.HISTORICAL_HC_DETECTOR.name()));
        }
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder
            .query(query)
            .sort(EXECUTION_START_TIME_FIELD, SortOrder.DESC)
            // Search query "from" starts from 0.
            .from(maxAdTaskDocsPerDetector - 1)
            .trackTotalHits(true)
            .size(1);
        searchRequest.source(sourceBuilder).indices(CommonName.DETECTION_STATE_INDEX);
        String detectorId = adTask.getDetectorId();

        client.search(searchRequest, ActionListener.wrap(r -> {
            Iterator<SearchHit> iterator = r.getHits().iterator();
            if (iterator.hasNext()) {
                logger
                    .debug(
                        "AD tasks count for detector {} is {}, exceeds limit of {}",
                        detectorId,
                        r.getHits().getTotalHits().value,
                        maxAdTaskDocsPerDetector
                    );
                SearchHit searchHit = r.getHits().getAt(0);
                try (
                    XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, searchHit.getSourceRef())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    ADTask task = ADTask.parse(parser, searchHit.getId());

                    DeleteByQueryRequest request = new DeleteByQueryRequest(CommonName.DETECTION_STATE_INDEX);
                    RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(EXECUTION_START_TIME_FIELD);
                    rangeQueryBuilder.lt(task.getExecutionStartTime().toEpochMilli()).format("epoch_millis");
                    request.setQuery(rangeQueryBuilder);
                    client.execute(DeleteByQueryAction.INSTANCE, request, ActionListener.wrap(res -> {
                        logger
                            .debug(
                                "Deleted {} old AD tasks started equals or before {} for detector {}",
                                res.getDeleted(),
                                adTask.getExecutionStartTime().toEpochMilli(),
                                detectorId
                            );
                        runBatchResultAction(response, adTask, listener);
                    }, e -> {
                        logger.warn("Failed to clean AD tasks for detector " + detectorId, e);
                        listener.onFailure(e);
                    }));
                } catch (Exception e) {
                    logger.warn("Failed to parse AD tasks for detector " + detectorId, e);
                    listener.onFailure(e);
                }
            } else {
                runBatchResultAction(response, adTask, listener);
            }
        }, e -> {
            logger.warn("Failed to search AD tasks for detector " + detectorId, e);
            listener.onFailure(e);
        }));
    }

    private void runBatchResultAction(IndexResponse response, ADTask adTask, ActionListener<AnomalyDetectorJobResponse> listener) {
        client.execute(ADBatchAnomalyResultAction.INSTANCE, new ADBatchAnomalyResultRequest(adTask), ActionListener.wrap(r -> {
            String remoteOrLocal = r.isRunTaskRemotely() ? "remote" : "local";
            logger
                .info(
                    "AD task {} of detector {} dispatched to {} node {}",
                    adTask.getTaskId(),
                    adTask.getDetectorId(),
                    remoteOrLocal,
                    r.getNodeId()
                );
            AnomalyDetectorJobResponse anomalyDetectorJobResponse = new AnomalyDetectorJobResponse(
                response.getId(),
                response.getVersion(),
                response.getSeqNo(),
                response.getPrimaryTerm(),
                RestStatus.OK
            );
            listener.onResponse(anomalyDetectorJobResponse);
        }, e -> listener.onFailure(e)));
    }

    public void runBatchResultActionForEntity(ADTask adTask, ActionListener<AnomalyDetectorJobResponse> listener) {
        client.execute(ADBatchAnomalyResultAction.INSTANCE, new ADBatchAnomalyResultRequest(adTask), ActionListener.wrap(r -> {
            String remoteOrLocal = r.isRunTaskRemotely() ? "remote" : "local";
            logger
                    .info(
                            "AD entity task {} of detector {} dispatched to {} node {}",
                            adTask.getTaskId(),
                            adTask.getDetectorId(),
                            remoteOrLocal,
                            r.getNodeId()
                    );
            AnomalyDetectorJobResponse anomalyDetectorJobResponse = new AnomalyDetectorJobResponse(
                    adTask.getDetectorId(),//TODO: tune this
                    0,
                    0,
                    0,
                    RestStatus.OK
            );
            listener.onResponse(anomalyDetectorJobResponse);
        }, e -> listener.onFailure(e)));
    }

    /**
     * Handle exceptions for AD task. Update task state and record error message.
     *
     * @param adTask AD task
     * @param e exception
     */
    public void handleADTaskException(ADTask adTask, Exception e) {
        // TODO: handle timeout exception
        String state = ADTaskState.FAILED.name();
        Map<String, Object> updatedFields = new HashMap<>();
        if (e instanceof DuplicateTaskException) {
            // If user send multiple start detector request, we will meet race condition.
            // Cache manager will put first request in cache and throw DuplicateTaskException
            // for the second request. We will delete the second task.
            logger
                .warn(
                    "There is already one running task for detector, detectorId:"
                        + adTask.getDetectorId()
                        + ". Will delete task "
                        + adTask.getTaskId()
                );
            deleteADTask(adTask.getTaskId());
            return;
        }
        if (e instanceof ADTaskCancelledException) {
            logger.info("AD task cancelled, taskId: {}, detectorId: {}", adTask.getTaskId(), adTask.getDetectorId());
            state = ADTaskState.STOPPED.name();
            String stoppedBy = ((ADTaskCancelledException) e).getCancelledBy();
            if (stoppedBy != null) {
                updatedFields.put(STOPPED_BY_FIELD, stoppedBy);
            }
        } else {
            logger.error("Failed to execute AD batch task, task id: " + adTask.getTaskId() + ", detector id: " + adTask.getDetectorId(), e);
        }
        updatedFields.put(ERROR_FIELD, getErrorMessage(e));
        updatedFields.put(STATE_FIELD, state);
        updatedFields.put(EXECUTION_END_TIME_FIELD, Instant.now().toEpochMilli());
        updateADTask(adTask.getTaskId(), updatedFields);
        if (adTask.getParentTaskId() != null) {
            updateADTask(adTask.getParentTaskId(), updatedFields);
        }
    }

    public void updateADTask(String taskId, Map<String, Object> updatedFields) {
        updateADTask(taskId, updatedFields, ActionListener.wrap(response -> {
            if (response.status() == RestStatus.OK) {
                logger.debug("Updated AD task successfully: {}", response.status());
            } else {
                logger.error("Failed to update AD task {}, status: {}", taskId, response.status());
            }
        }, e -> { logger.error("Failed to update task: " + taskId, e); }));
    }

    /**
     * Update AD task for specific fields.
     *
     * @param taskId task id
     * @param updatedFields updated fields, key: filed name, value: new value
     * @param listener action listener
     */
    public void updateADTask(String taskId, Map<String, Object> updatedFields, ActionListener<UpdateResponse> listener) {
        UpdateRequest updateRequest = new UpdateRequest(CommonName.DETECTION_STATE_INDEX, taskId);
        Map<String, Object> updatedContent = new HashMap<>();
        updatedContent.putAll(updatedFields);
        updatedContent.put(LAST_UPDATE_TIME_FIELD, Instant.now().toEpochMilli());
        updateRequest.doc(updatedContent);
        updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client.update(updateRequest, listener);
    }

    public void deleteADTask(String taskId) {
        deleteADTask(
            taskId,
            ActionListener
                .wrap(
                    r -> { logger.info("Deleted AD task {} with status: {}", taskId, r.status()); },
                    e -> { logger.error("Failed to delete AD task " + taskId, e); }
                )
        );
    }

    public void deleteADTask(String taskId, ActionListener<DeleteResponse> listener) {
        DeleteRequest deleteRequest = new DeleteRequest(CommonName.DETECTION_STATE_INDEX, taskId);
        client.delete(deleteRequest, listener);
    }

    /**
     * Cancel running task by detector id.
     *
     * @param detectorId detector id
     * @param reason reason to cancel AD task
     * @param userName which user cancel the AD task
     * @return AD task cancellation state
     */
    public ADTaskCancellationState cancelLocalTaskByDetectorId(String detectorId, String reason, String userName) {
        ADTaskCancellationState cancellationState = adTaskCacheManager.cancelByDetectorId(detectorId, reason, userName);
        logger
            .debug(
                "Cancelled AD task for detector: {}, state: {}, cancelled by: {}, reason: {}",
                detectorId,
                cancellationState,
                userName,
                reason
            );
        return cancellationState;
    }

    /**
     * Delete AD tasks docs.
     *
     * @param detectorId detector id
     * @param function AD function
     * @param listener action listener
     */
    public void deleteADTasks(String detectorId, AnomalyDetectorFunction function, ActionListener<DeleteResponse> listener) {
        DeleteByQueryRequest request = new DeleteByQueryRequest(CommonName.DETECTION_STATE_INDEX);

        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, detectorId));

        request.setQuery(query);
        client.execute(DeleteByQueryAction.INSTANCE, request, ActionListener.wrap(r -> {
            logger.info("AD tasks deleted for detector {}", detectorId);
            function.execute();
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                function.execute();
            } else {
                listener.onFailure(e);
            }
        }));
    }

    /**
     * Remove detector from cache on coordinating node.
     *
     * @param detectorId detector id
     */
    public void removeDetectorFromCache(String detectorId) {
        adTaskCacheManager.removeDetector(detectorId);
    }

    public boolean hcDetectorDone(String detectorId) {
        return adTaskCacheManager.hasEntity(detectorId);
    }

    public void updateLatestADTask(String detectorId, ImmutableList<ADTaskType> taskTypes, Map<String, Object> updatedFields) {
        updateLatestADTask(detectorId, taskTypes, updatedFields,
                ActionListener.wrap(r -> logger.info("updated"), e -> logger.warn("failed to update latest task for detector {}", detectorId)));
    }

    public void updateLatestADTask(String detectorId, ImmutableList<ADTaskType> taskTypes, Map<String, Object> updatedFields,
                                   ActionListener listener) {
        getLatestADTask(detectorId, taskTypes,
                (adTask) -> {
                  if (adTask.isPresent()) {
                      updateADTask(adTask.get().getTaskId(), updatedFields);
                  }
                },
                null,
                listener);
    }
}
