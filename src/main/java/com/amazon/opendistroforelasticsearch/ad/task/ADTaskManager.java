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

import static com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin.AD_BATCH_TASK_THREAD_POOL_NAME;
import static com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages.DETECTOR_IS_RUNNING;
import static com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages.EXCEED_HISTORICAL_ANALYSIS_LIMIT;
import static com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages.NO_ELIGIBLE_NODE_TO_RUN_DETECTOR;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.DETECTOR_ID_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.ERROR_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.EXECUTION_END_TIME_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.EXECUTION_START_TIME_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.IS_LATEST_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.LAST_UPDATE_TIME_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.PARENT_TASK_ID_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.STATE_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.STOPPED_BY_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.TASK_PROGRESS_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.TASK_TYPE_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult.TASK_ID_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_RUNNING_ENTITIES_PER_DETECTOR;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.REQUEST_TIMEOUT;
import static com.amazon.opendistroforelasticsearch.ad.util.ExceptionUtil.getErrorMessage;
import static com.amazon.opendistroforelasticsearch.ad.util.ExceptionUtil.getShardsFailure;
import static org.elasticsearch.action.DocWriteResponse.Result.CREATED;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
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
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.cluster.HashRing;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ADTaskCancelledException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.DuplicateTaskException;
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
import com.amazon.opendistroforelasticsearch.ad.model.DetectionDateRange;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfile;
import com.amazon.opendistroforelasticsearch.ad.model.Entity;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.AnomalyDetectorFunction;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.IndexAnomalyDetectorJobActionHandler;
import com.amazon.opendistroforelasticsearch.ad.transport.ADBatchAnomalyResultAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADBatchAnomalyResultRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.ADBatchAnomalyResultResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.ADCancelTaskAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADCancelTaskNodeResponse;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Manage AD task.
 */
public class ADTaskManager {
    private final Logger logger = LogManager.getLogger(this.getClass());
    private final Set<String> retryableErrors = ImmutableSet.of(EXCEED_HISTORICAL_ANALYSIS_LIMIT, NO_ELIGIBLE_NODE_TO_RUN_DETECTOR);
    private final Client client;
    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;
    private final AnomalyDetectionIndices detectionIndices;
    private final DiscoveryNodeFilterer nodeFilter;
    private final ADTaskCacheManager adTaskCacheManager;
    private final ThreadPool threadPool;

    private final HashRing hashRing;
    private volatile Integer maxOldAdTaskDocsPerDetector;
    private volatile Integer pieceIntervalSeconds;
    private volatile TimeValue requestTimeout;
    private volatile Integer maxAdBatchTaskPerNode;
    private volatile Integer maxRunningEntitiesPerDetector;

    public ADTaskManager(
        Settings settings,
        ClusterService clusterService,
        Client client,
        NamedXContentRegistry xContentRegistry,
        AnomalyDetectionIndices detectionIndices,
        DiscoveryNodeFilterer nodeFilter,
        HashRing hashRing,
        ADTaskCacheManager adTaskCacheManager,
        ThreadPool threadPool
    ) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.detectionIndices = detectionIndices;
        this.nodeFilter = nodeFilter;
        this.clusterService = clusterService;
        this.adTaskCacheManager = adTaskCacheManager;
        this.hashRing = hashRing;

        this.maxOldAdTaskDocsPerDetector = MAX_OLD_AD_TASK_DOCS_PER_DETECTOR.get(settings);
        this.threadPool = threadPool;
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(MAX_OLD_AD_TASK_DOCS_PER_DETECTOR, it -> maxOldAdTaskDocsPerDetector = it);

        this.pieceIntervalSeconds = BATCH_TASK_PIECE_INTERVAL_SECONDS.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(BATCH_TASK_PIECE_INTERVAL_SECONDS, it -> pieceIntervalSeconds = it);

        this.requestTimeout = REQUEST_TIMEOUT.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(REQUEST_TIMEOUT, it -> requestTimeout = it);

        this.maxAdBatchTaskPerNode = MAX_BATCH_TASK_PER_NODE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_BATCH_TASK_PER_NODE, it -> maxAdBatchTaskPerNode = it);

        this.maxRunningEntitiesPerDetector = MAX_RUNNING_ENTITIES_PER_DETECTOR.get(settings);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(MAX_RUNNING_ENTITIES_PER_DETECTOR, it -> maxRunningEntitiesPerDetector = it);
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
        getDetector(detectorId, (detector) -> {
            if (validateDetector(detector, listener)) { // validate if detector is ready to start
                if (detectionDateRange == null) {
                    // run realtime job
                    // create AD task
                    handler.startAnomalyDetectorJob(detector);
                } else {
                    // run historical analysis
                    Optional<DiscoveryNode> owningNode = hashRing.getOwningNode(detector.getDetectorId());
                    if (!owningNode.isPresent()) {
                        logger.debug("Can't find eligible node to run as AD task's coordinating node");
                        listener
                            .onFailure(
                                new ElasticsearchStatusException("No eligible node to run detector", RestStatus.INTERNAL_SERVER_ERROR)
                            );
                        return;
                    }
                    logger.info("coordinating node is : {} for detector: {}", owningNode.get().getId(), detectorId);
                    forwardToCoordinatingNode(
                        detector,
                        null,
                        detectionDateRange,
                        user,
                        ADTaskAction.START,
                        transportService,
                        owningNode.get(),
                        listener
                    );
                }
            }
        }, listener);
    }

    protected void forwardToCoordinatingNode(
        ADTask adTask,
        ADTaskAction adTaskAction,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        DiscoveryNode coordinatingNode = getCoordinatingNode(adTask);
        logger.debug("4444444444 coordinatingNode found, will clean detector cache on it, detectorId: " + adTask.getDetectorId());
        forwardToCoordinatingNode(
            adTask.getDetector(),
            adTask,
            adTask.getDetectionDateRange(),
            null,
            adTaskAction,
            transportService,
            coordinatingNode,
            listener
        );
    }

    protected void forwardToCoordinatingNode(
        ADTask adTask,
        ADTaskAction adTaskAction,
        TransportService transportService,
        List<String> staleRunningEntity,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        DiscoveryNode coordinatingNode = getCoordinatingNode(adTask);
        logger.debug("4444444444 coordinatingNode found, will clean detector cache on it, detectorId: " + adTask.getDetectorId());
        forwardToCoordinatingNode(
            adTask.getDetector(),
            adTask,
            adTask.getDetectionDateRange(),
            null,
            adTaskAction,
            transportService,
            coordinatingNode,
            staleRunningEntity,
            listener
        );
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
     * @param adTask AD task
     * @param detectionDateRange detection date range
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
        forwardToCoordinatingNode(detector, adTask, detectionDateRange, user, adTaskAction, transportService, node, null, listener);
    }

    protected void forwardToCoordinatingNode(
        AnomalyDetector detector,
        ADTask adTask,
        DetectionDateRange detectionDateRange,
        User user,
        ADTaskAction adTaskAction,
        TransportService transportService,
        DiscoveryNode node,
        List<String> staleRunningEntities,
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
                new ForwardADTaskRequest(detector, adTask, detectionDateRange, staleRunningEntities, user, adTaskAction),
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
     * @param historical stop historical analysis or not
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
        getDetector(detectorId, detector -> {
            if (historical) {
                getLatestADTask(
                    detectorId,
                    ADTaskType.getHistoricalDetectorTaskTypes(),
                    // ImmutableList.of(ADTaskType.HISTORICAL_HC_DETECTOR, ADTaskType.HISTORICAL_SINGLE_ENTITY),
                    (task) -> stopHistoricalDetector(detectorId, task, user, listener),
                    transportService,
                    listener
                );
            } else {
                handler.stopAnomalyDetectorJob(detectorId);
            }
        }, listener);
    }

    public <T> void getDetector(String detectorId, Consumer<AnomalyDetector> consumer, ActionListener<T> listener) {
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
            } catch (Exception e) {
                String message = "Failed to start anomaly detector " + detectorId;
                logger.error(message, e);
                listener.onFailure(new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, exception -> listener.onFailure(exception)));
    }

    /**
     * Get latest AD task and execute consumer function.
     *
     * @param detectorId detector id
     * @param adTaskTypes AD task types
     * @param function consumer function
     * @param transportService transport service
     * @param listener action listener
     * @param <T> action listener response
     */
    public <T> void getLatestADTask(
        String detectorId,
        List<ADTaskType> adTaskTypes,
        Consumer<Optional<ADTask>> function,
        TransportService transportService,
        ActionListener<T> listener
    ) {
        getLatestADTask(detectorId, null, adTaskTypes, function, transportService, true, listener);
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
            query.filter(new TermsQueryBuilder(TASK_TYPE_FIELD, taskTypeToString(adTaskTypes)));
        }
        if (Strings.isNotEmpty(entityValue)) {
            String path = "entity";
            String entityValueFieldName = path + ".value";
            TermQueryBuilder entityValueFilterQuery = QueryBuilders.termQuery(entityValueFieldName, entityValue);
            NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(path, entityValueFilterQuery, ScoreMode.None);
            query.filter(nestedQueryBuilder);
        }
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(query);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);
        searchRequest.indices(CommonName.DETECTION_STATE_INDEX);

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
                logger.info("------------ latest task id is {}, for detector {}", adTask.getTaskId(), adTask.getDetectorId());

                // TODO: check realtime detector job and reset realtime task as stopped.
                if (resetTaskState && adTask.isHistoricalTask() && !isADTaskEnded(adTask) && lastUpdateTimeExpired(adTask)
                // && !adTask.getDetector().isMultientityDetector()
                ) {
                    String taskId = adTask.getTaskId();
                    // If AD task is still running, but its last updated time not refreshed
                    // for 2 pieces intervals, we will get task profile to check if it's
                    // really running and reset state as STOPPED if not running.
                    // For example, ES process crashes, then all tasks running on it will stay
                    // as running. We can reset the task state when next read happen.
                    getADTaskProfile(adTask, ActionListener.wrap(taskProfiles -> {
                        logger
                            .debug(
                                "++++++++++113355 taskProfiles {}, size: {},  {}, {}",
                                taskProfiles,
                                taskProfiles != null ? taskProfiles.size() : 0,
                                !taskProfiles.containsKey(adTask.getTaskId()),
                                !taskProfiles.containsKey(adTask.getTaskId()) || taskProfiles.get(adTask.getTaskId()).getNodeId() == null
                            );
                        if (!taskProfiles.containsKey(adTask.getTaskId()) || taskProfiles.get(adTask.getTaskId()).getNodeId() == null) {
                            logger.debug("4444444444 reset task state as stopped");
                            // If no node is running this task, reset it as STOPPED.
                            resetTaskStateAsStopped(adTask, transportService); // TODO: reset realtime task state
                            adTask.setState(ADTaskState.STOPPED.name());
                        } else if (ADTaskType.HISTORICAL_HC_DETECTOR.name().equals(adTask.getTaskType())) {
                            logger.debug("4444444444 check running/pending entities");
                            // Check if any running entity not run on worker node. If yes, we need to remove it
                            // and poll next entity from pending entity queue and run it.
                            // TODO: If running entity is empty, but pending entities exists, need to poll next entity and run it.?
                            ADTaskProfile detectorTaskProfile = taskProfiles.get(taskId);
                            if (detectorTaskProfile.getRunningEntitiesCount() > 0) {
                                List<String> runningTasksOnCoordinatingNode = Arrays.asList(detectorTaskProfile.getRunningEntities());
                                List<String> runningTasksOnWorkerNode = new ArrayList<>();
                                for (Map.Entry<String, ADTaskProfile> entry : taskProfiles.entrySet()) {
                                    if (!taskId.equals(entry.getKey())) {
                                        runningTasksOnWorkerNode.add(entry.getValue().getEntity().get(0).getValue());
                                    }
                                }
                                logger
                                    .debug(
                                        "4444444444 runningTasksOnCoordinatingNode: {}, runningTasksOnWorkerNode: {}",
                                        Arrays.toString(runningTasksOnCoordinatingNode.toArray(new String[0])),
                                        Arrays.toString(runningTasksOnWorkerNode.toArray(new String[0]))
                                    );
                                if (runningTasksOnCoordinatingNode.size() > runningTasksOnWorkerNode.size()) {
                                    runningTasksOnCoordinatingNode.removeAll(runningTasksOnWorkerNode);
                                    logger
                                        .debug(
                                            "4444444444-2 runningTasksOnCoordinatingNode: {}",
                                            Arrays.toString(runningTasksOnCoordinatingNode.toArray(new String[0]))
                                        );
                                    forwardToCoordinatingNode(
                                        adTask,
                                        ADTaskAction.CLEAN_RUNNING_ENTITY,
                                        transportService,
                                        runningTasksOnCoordinatingNode,
                                        ActionListener
                                            .wrap(
                                                res -> {
                                                    logger.debug("4444444444 forwared task to clean running entity, task id {}", taskId);
                                                },
                                                ex -> {
                                                    logger
                                                        .debug(
                                                            "4444444444 failed to forwared task to clean running entity, task id {}",
                                                            taskId
                                                        );
                                                }
                                            )
                                    );
                                }
                            }
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

    public <T> void getLatestADTasks(
        String detectorId,
        String entityValue,
        List<ADTaskType> adTaskTypes,
        Consumer<Map<String, ADTask>> function,
        TransportService transportService,
        boolean resetTaskState,
        ActionListener<T> listener
    ) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, detectorId));
        query.filter(new TermQueryBuilder(IS_LATEST_FIELD, true));
        int size = 1;
        if (adTaskTypes != null && adTaskTypes.size() > 0) {
            size = adTaskTypes.size();
            query.filter(new TermsQueryBuilder(TASK_TYPE_FIELD, taskTypeToString(adTaskTypes)));
        }
        if (Strings.isNotEmpty(entityValue)) {
            String path = "entity";
            String entityValueFieldName = path + ".value";
            TermQueryBuilder entityValueFilterQuery = QueryBuilders.termQuery(entityValueFieldName, entityValue);
            NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(path, entityValueFilterQuery, ScoreMode.None);
            query.filter(nestedQueryBuilder);
        }
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.query(query).size(size);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);
        searchRequest.indices(CommonName.DETECTION_STATE_INDEX);
        logger.debug("6666666666 query {}", sourceBuilder);

        client.search(searchRequest, ActionListener.wrap(r -> {
            // https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/359#discussion_r558653132
            // getTotalHits will be null when we track_total_hits is false in the query request.
            // Add more checking here to cover some unknown cases.
            Map<String, ADTask> adTasks = new HashMap<>();
            logger.debug("66666666666666666 {}", r.getHits().getTotalHits());
            if (r == null || r.getHits().getTotalHits() == null || r.getHits().getTotalHits().value == 0) {
                // don't throw exception here as consumer functions need to handle missing task
                // in different way.
                function.accept(adTasks);
                return;
            }

            Iterator<SearchHit> iterator = r.getHits().iterator();

            while (iterator.hasNext()) {
                SearchHit searchHit = iterator.next();
                try (
                    XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, searchHit.getSourceRef())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    ADTask adTask = ADTask.parse(parser, searchHit.getId());
                    logger.debug("66666666666666666 adTask: {}", adTask.toString());
                    adTasks.put(adTask.getTaskType(), adTask);
                    // TODO: check realtime detector job and reset realtime task as stopped.
                    if (resetTaskState) {
                        resetTaskState(adTask, transportService);
                    }
                } catch (Exception e) {
                    String message = "Failed to parse AD task for detector " + detectorId;
                    logger.error(message, e);
                    listener.onFailure(new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
                }
            }
            function.accept(adTasks);

        }, e -> {
            if (e instanceof IndexNotFoundException) {
                function.accept(new HashMap<>());
            } else {
                logger.error("Failed to search AD task for detector " + detectorId, e);
                listener.onFailure(e);
            }
        }));
    }

    public void maintainRunningDetector(TransportService transportService) {
        logger.info("Start to maintain running detectors");
        String[] detectors = adTaskCacheManager.getRunningDetectors();
        for (String detectorId : detectors) {
            getLatestADTasks(
                detectorId,
                null,
                ADTaskType.getHistoricalDetectorTaskTypes(),
                adTask -> {},
                transportService,
                true,
                ActionListener
                    .wrap(
                        r -> { logger.info("Finished maintaining running detector {}", detectorId); },
                        e -> { logger.error("Failed maintaining running detector " + detectorId, e); }
                    )
            );
        }
    }

    private void resetTaskState(ADTask adTask, TransportService transportService) {
        if (adTask.isHistoricalTask() && !isADTaskEnded(adTask) && lastUpdateTimeExpired(adTask)) {
            // If AD task is still running, but its last updated time not refreshed
            // for 2 pieces intervals, we will get task profile to check if it's
            // really running and reset state as STOPPED if not running.
            // For example, ES process crashes, then all tasks running on it will stay
            // as running. We can reset the task state when next read happen.
            String taskId = adTask.getTaskId();
            getADTaskProfile(adTask, ActionListener.wrap(taskProfiles -> {
                logger
                    .debug(
                        "++++++++++113355 taskProfiles {}, size: {},  {}, {}",
                        taskProfiles,
                        taskProfiles != null ? taskProfiles.size() : 0,
                        !taskProfiles.containsKey(adTask.getTaskId()),
                        !taskProfiles.containsKey(adTask.getTaskId()) || taskProfiles.get(adTask.getTaskId()).getNodeId() == null
                    );
                if (!taskProfiles.containsKey(adTask.getTaskId()) || taskProfiles.get(adTask.getTaskId()).getNodeId() == null) {
                    logger.debug("4444444444 reset task state as stopped");
                    // If no node is running this task, reset it as STOPPED.
                    resetTaskStateAsStopped(adTask, transportService); // TODO: reset realtime task state
                    adTask.setState(ADTaskState.STOPPED.name());
                } else if (ADTaskType.HISTORICAL_HC_DETECTOR.name().equals(adTask.getTaskType())) {
                    logger.debug("4444444444 check running/pending entities");
                    // Check if any running entity not run on worker node. If yes, we need to remove it
                    // and poll next entity from pending entity queue and run it.
                    // TODO: If running entity is empty, but pending entities exists, need to poll next entity and run it.?
                    ADTaskProfile detectorTaskProfile = taskProfiles.get(taskId);
                    if (detectorTaskProfile.getRunningEntitiesCount() > 0) {
                        List<String> runningTasksOnCoordinatingNode = Arrays.asList(detectorTaskProfile.getRunningEntities());
                        List<String> runningTasksOnWorkerNode = new ArrayList<>();
                        for (Map.Entry<String, ADTaskProfile> entry : taskProfiles.entrySet()) {
                            if (!taskId.equals(entry.getKey())) {
                                runningTasksOnWorkerNode.add(entry.getValue().getEntity().get(0).getValue());
                            }
                        }
                        logger
                            .debug(
                                "4444444444 runningTasksOnCoordinatingNode: {}, runningTasksOnWorkerNode: {}",
                                Arrays.toString(runningTasksOnCoordinatingNode.toArray(new String[0])),
                                Arrays.toString(runningTasksOnWorkerNode.toArray(new String[0]))
                            );
                        if (runningTasksOnCoordinatingNode.size() > runningTasksOnWorkerNode.size()) {
                            runningTasksOnCoordinatingNode.removeAll(runningTasksOnWorkerNode);
                            logger
                                .debug(
                                    "4444444444-2 runningTasksOnCoordinatingNode: {}",
                                    Arrays.toString(runningTasksOnCoordinatingNode.toArray(new String[0]))
                                );
                            forwardToCoordinatingNode(
                                adTask,
                                ADTaskAction.CLEAN_RUNNING_ENTITY,
                                transportService,
                                runningTasksOnCoordinatingNode,
                                ActionListener
                                    .wrap(
                                        res -> { logger.debug("4444444444 forwared task to clean running entity, task id {}", taskId); },
                                        ex -> {
                                            logger.debug("4444444444 failed to forwared task to clean running entity, task id {}", taskId);
                                        }
                                    )
                            );
                        }
                    }
                }
            }, e -> { logger.error("Failed to get AD task profile for task " + adTask.getTaskId(), e); }));
        }
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
        logger.info("------------------- task id is: " + taskId);
        DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
        String userName = user == null ? null : user.getName();

        ADCancelTaskRequest cancelTaskRequest = new ADCancelTaskRequest(detectorId, userName, dataNodes);
        client.execute(ADCancelTaskAction.INSTANCE, cancelTaskRequest, ActionListener.wrap(response -> {
            List<ADTaskCancellationState> nodeResponses = response
                .getNodes()
                .stream()
                .filter(r -> r.getState() != null)
                .map(ADCancelTaskNodeResponse::getState)
                .collect(Collectors.toList());
            listener.onResponse(new AnomalyDetectorJobResponse(taskId, 0, 0, 0, RestStatus.OK));
        }, e -> {
            logger.error("Failed to cancel AD task " + taskId + ", detector id: " + detectorId, e);
            listener.onFailure(e);
        }));
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
                adTask.getDetector(),
                null,
                adTask.getDetectionDateRange(),
                null,
                ADTaskAction.FINISHED,
                transportService,
                targetNode,
                ActionListener
                    .wrap(
                        r -> { function.execute(); },
                        e -> { logger.error("++++++++++ Failed to clear detector cache on coordinating node " + coordinatingNode, e); }
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

    protected void entityTaskDone(ADTask adTask, TransportService transportService) {
        entityTaskDone(adTask, null, transportService);
    }

    protected void entityTaskDone(ADTask adTask, Exception exception, TransportService transportService) {
        entityTaskDone(
            adTask,
            exception,
            transportService,
            ActionListener
                .wrap(
                    r -> { logger.debug("AD task forwarded to coordinating node, task id {}", adTask.getTaskId()); },
                    e -> { logger.debug("AD task failed to forward to coordinating node, task id {}", adTask.getTaskId()); }
                )
        );
    }

    private void entityTaskDone(
        ADTask adTask,
        Exception exception,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {

        try {
            ADTaskAction action = getAdEntityTaskAction(adTask, exception);
            logger
                .debug(
                    "4444444444 Forward entity task to coordinating node, detector id:{} task id:{}, action:{}",
                    adTask.getDetectorId(),
                    adTask.getTaskId(),
                    action.name()
                );
            forwardToCoordinatingNode(adTask, action, transportService, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private DiscoveryNode getCoordinatingNode(ADTask adTask) {
        String coordinatingNode = adTask.getCoordinatingNode();
        DiscoveryNode[] eligibleDataNodes = nodeFilter.getEligibleDataNodes();
        DiscoveryNode targetNode = null;
        for (DiscoveryNode node : eligibleDataNodes) {
            if (node.getId().equals(coordinatingNode)) {
                targetNode = node;
                break;
            }
        }
        if (targetNode == null) {
            throw new ResourceNotFoundException(adTask.getDetectorId(), "AD task coordinating node not found");
        }
        return targetNode;
    }

    private ADTaskAction getAdEntityTaskAction(ADTask adTask, Exception exception) {
        ADTaskAction action = ADTaskAction.NEXT_ENTITY;
        if (exception != null) {
            adTask.setError(getErrorMessage(exception));
            if (exception instanceof LimitExceededException && exception.getMessage().contains(NO_ELIGIBLE_NODE_TO_RUN_DETECTOR)) {
                action = ADTaskAction.PUSH_BACK_ENTITY;
            } else if (exception instanceof ADTaskCancelledException) {
                action = ADTaskAction.CANCEL;
            }
        }
        return action;
    }

    /**
     * Get AD task profile data.
     *  @param detectorId detector id
     * @param transportService transport service
     * @param profile detector profile
     * @param listener action listener
     */
    public void getLatestADTaskProfile(
        String detectorId,
        TransportService transportService,
        DetectorProfile profile,
        ActionListener<DetectorProfile> listener
    ) {
        getLatestADTask(detectorId, null, ADTaskType.getHistoricalDetectorTaskTypes(), adTask -> {
            if (adTask.isPresent()) {
                getADTaskProfile(adTask.get(), ActionListener.wrap(adTaskProfiles -> {
                    DetectorProfile.Builder profileBuilder = new DetectorProfile.Builder();
                    profileBuilder.adTaskProfiles(adTaskProfiles);
                    DetectorProfile detectorProfile = profileBuilder.build();
                    detectorProfile.merge(profile);
                    // logger.info("yyyywwww88: merged profiles {}", detectorProfile.getAdTaskProfiles().size());
                    listener.onResponse(detectorProfile);
                }, e -> {
                    logger.error("Failed to get AD task profile for task " + adTask.get().getTaskId(), e);
                    listener.onFailure(e);
                }));
            } else {
                // logger.info("--------- can't find latest AD task ");
                // listener.onFailure(new ResourceNotFoundException(detectorId, "Can't find latest task for detector"));
                DetectorProfile.Builder profileBuilder = new DetectorProfile.Builder();
                listener.onResponse(profileBuilder.build());
            }
        }, transportService, false, listener);
    }

    private void getADTaskProfile(ADTask adTask, ActionListener<Map<String, ADTaskProfile>> listener) {
        String detectorId = adTask.getDetectorId();

        DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
        ADTaskProfileRequest adTaskProfileRequest = new ADTaskProfileRequest(detectorId, dataNodes);
        client.execute(ADTaskProfileAction.INSTANCE, adTaskProfileRequest, ActionListener.wrap(response -> {
            if (response.hasFailures()) {
                listener.onFailure(response.failures().get(0));
                return;
            }

            Map<String, ADTaskProfile> adTaskProfileMap = new HashMap<>();
            for (ADTaskProfileNodeResponse node : response.getNodes()) {
                List<ADTaskProfile> profiles = node.getAdTaskProfiles();
                if (profiles != null) {
                    profiles.forEach(p -> {
                        if (p.getTaskId() == null) {
                            p.setTaskId(adTask.getTaskId());
                        }
                        if (!ADTaskType.HISTORICAL_HC_ENTITY.name().equals(p.getAdTaskType())) {
                            p.setAdTask(adTask);
                        }
                        if (adTaskProfileMap.containsKey(p.getTaskId())) {
                            logger.warn("Find duplicate task profile for task " + p.getTaskId());
                        }
                        adTaskProfileMap.put(p.getTaskId(), p);
                    });
                }
                logger
                    .info(
                        "AD task profile: nodeId: {}, profile size: {}, node.getAdTaskProfiles: {}",
                        node.getNode().getId(),
                        profiles == null ? 0 : profiles.size(),
                        profiles
                    );
            }

            logger.info("AD task profile: total running entity count: {}", adTaskProfileMap.size());
            listener.onResponse(adTaskProfileMap);
        }, e -> {
            logger.error("Failed to get task profile for task " + adTask.getTaskId(), e);
            listener.onFailure(e);
        }));
    }

    public List<ADTaskProfile> getLocalADTaskProfilesByDetectorId(String detectorId) {
        List<ADTaskProfile> adTaskProfiles = new ArrayList<>();
        List<String> tasksOfDetector = adTaskCacheManager.getTasksOfDetector(detectorId);

        if (tasksOfDetector.size() > 0) {
            tasksOfDetector.forEach(taskId -> {
                ADTaskProfile adTaskProfile = new ADTaskProfile(
                    adTaskCacheManager.getShingle(taskId).size(),
                    adTaskCacheManager.getRcfModel(taskId).getTotalUpdates(),
                    adTaskCacheManager.isThresholdModelTrained(taskId),
                    adTaskCacheManager.getThresholdModelTrainingDataSize(taskId),
                    adTaskCacheManager.getModelSize(taskId),
                    clusterService.localNode().getId(),
                    adTaskCacheManager.getEntity(taskId),
                    taskId
                );
                adTaskProfiles.add(adTaskProfile);
            });
        }
        if (adTaskCacheManager.hasEntity(detectorId)) {
            ADTaskProfile adTaskProfile = new ADTaskProfile(
                clusterService.localNode().getId(),
                adTaskCacheManager.getTopEntityCount(detectorId),
                adTaskCacheManager.getPendingEntityCount(detectorId),
                adTaskCacheManager.getRunningEntityCount(detectorId),
                adTaskCacheManager.getRunningEntities(detectorId)
            );
            logger
                .info(
                    "AD task profile: coordinating node running entity is {}",
                    Arrays.toString(adTaskCacheManager.getRunningEntities(detectorId))
                );

            adTaskProfiles.add(adTaskProfile);
        } else {
            logger
                .info(
                    "AD task profile: worker node running entity is {}",
                    Arrays.toString(adTaskCacheManager.getRunningEntities(detectorId))
                );
        }
        logger.info("AD task profile: node adTaskProfile size is {}", adTaskProfiles.size());
        return adTaskProfiles;
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
     * Start anomaly detector on coordinating node.
     * Will init task index if not exist and write new AD task to index. If task index
     * exists, will check if there is task running. If no running task, reset old task
     * as not latest and clean old tasks which exceeds limitation. Then find out node
     * with least load and dispatch task to that node(worker node).
     *
     * @param detector anomaly detector
     * @param detectionDateRange detection date range
     * @param user user
     * @param transportService transport service
     * @param listener action listener
     */
    public void startAnomalyDetector(
        AnomalyDetector detector,
        DetectionDateRange detectionDateRange,
        User user,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        // logger.info("ylwudebug : coordinate node is : {}", clusterService.localNode().getId());
        try {
            if (detectionIndices.doesDetectorStateIndexExist()) {
                // If detection index exist, check if latest AD task is running
                getLatestADTask(
                    detector.getDetectorId(),
                    // ImmutableList.of(getADTaskType(detector, detectionDateRange)),
                    getADTaskTypes(detectionDateRange),
                    (adTask) -> {
                        if (!adTask.isPresent() || isADTaskEnded(adTask.get())) {
                            executeAnomalyDetector(detector, detectionDateRange, user, listener);
                        } else {
                            listener.onFailure(new ElasticsearchStatusException(DETECTOR_IS_RUNNING, RestStatus.BAD_REQUEST));
                        }
                    },
                    transportService,
                    listener
                );
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

    private void executeAnomalyDetector(
        AnomalyDetector detector,
        DetectionDateRange detectionDateRange,
        User user,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
        updateByQueryRequest.indices(CommonName.DETECTION_STATE_INDEX);
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, detector.getDetectorId()));
        query.filter(new TermQueryBuilder(IS_LATEST_FIELD, true));
        // String taskType = getADTaskType(detector, detectionDateRange).name();
        // make sure we reset all latest task as false when user switch from single entity to HC, vice versa.
        query.filter(new TermsQueryBuilder(TASK_TYPE_FIELD, taskTypeToString(getADTaskTypes(detectionDateRange, true))));
        updateByQueryRequest.setQuery(query);
        updateByQueryRequest.setRefresh(true);
        updateByQueryRequest.setScript(new Script("ctx._source.is_latest = false;"));

        logger.debug("000111222333: reset latest task : {}", query);
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

    private ADTaskType getADTaskType(AnomalyDetector detector, DetectionDateRange detectionDateRange) {
        if (detectionDateRange == null) {
            return detector.isMultientityDetector() ? ADTaskType.REALTIME_HC_DETECTOR : ADTaskType.REALTIME_SINGLE_ENTITY;
        } else {
            return detector.isMultientityDetector() ? ADTaskType.HISTORICAL_HC_DETECTOR : ADTaskType.HISTORICAL_SINGLE_ENTITY;
        }
    }

    private List<ADTaskType> getADTaskTypes(DetectionDateRange detectionDateRange) {
        return getADTaskTypes(detectionDateRange, false);
    }

    private List<ADTaskType> getADTaskTypes(DetectionDateRange detectionDateRange, boolean resetLatestFlag) {
        if (detectionDateRange == null) {
            return ADTaskType.getRealtimeTaskTypes();
        } else {
            if (resetLatestFlag) {
                return ADTaskType.getAllHistoricalTaskTypes();
            } else {
                return ADTaskType.getHistoricalDetectorTaskTypes();
            }

        }
    }

    private List<String> taskTypeToString(List<ADTaskType> adTaskTypes) {
        return adTaskTypes.stream().map(type -> type.name()).collect(Collectors.toList());
    }

    private void createNewADTask(
        AnomalyDetector detector,
        DetectionDateRange detectionDateRange,
        User user,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        String userName = user == null ? null : user.getName();
        Instant now = Instant.now();
        String taskType = getADTaskType(detector, detectionDateRange).name();
        ADTask adTask = new ADTask.Builder()
            .detectorId(detector.getDetectorId())
            .detector(detector)
            .isLatest(true)
            .taskType(taskType)
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
                                (response, delegatedListener) -> { cleanOldAdTasks(response, adTask, delegatedListener); },
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

    public <T> void createADTaskDirectly(ADTask adTask, Consumer<IndexResponse> function, ActionListener<T> listener) {
        IndexRequest request = new IndexRequest(CommonName.DETECTION_STATE_INDEX);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            request
                .source(adTask.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            client.index(request, ActionListener.wrap(r -> function.accept(r), e -> {
                logger.error("Failed to create AD task for detector " + adTask.getDetectorId(), e);
                listener.onFailure(e);
            }));
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
            handleADTaskException(adTask, e); // TODO: handle HC detector task here?
            if (e instanceof DuplicateTaskException) {
                listener.onFailure(new ElasticsearchStatusException(DETECTOR_IS_RUNNING, RestStatus.BAD_REQUEST));
            } else {
                listener.onFailure(e);
                // if (!adTask.getTaskType().startsWith("HISTORICAL_HC_")) {
                // adTaskCacheManager.removeDetector(adTask.getDetectorId());
                // }

            }
        });
        try {
            // Put detector id in cache. If detector id already in cache, will throw
            // DuplicateTaskException. This is to solve race condition when user send
            // multiple start request for one historical detector.
            if (adTask.getDetectionDateRange() != null) {
                adTaskCacheManager.add(adTask.getDetectorId(), adTask.getTaskType());
            }
        } catch (Exception e) {
            delegatedListener.onFailure(e);
            return;
        }
        if (function != null) {
            function.accept(response, delegatedListener);
        }
    }

    // TODO: change this for HC detector as every entity will have one task. When delete old detector level task, delete
    // all entity task with "parent task id = detector task id", delete with cron job?
    private void cleanOldAdTasks(IndexResponse response, ADTask adTask, ActionListener<AnomalyDetectorJobResponse> delegatedListener) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, adTask.getDetectorId()));
        query.filter(new TermQueryBuilder(IS_LATEST_FIELD, false));

        if (adTask.isHistoricalTask()) {
            query.filter(new TermsQueryBuilder(TASK_TYPE_FIELD, taskTypeToString(ADTaskType.getHistoricalDetectorTaskTypes())));
        } else {
            query.filter(new TermsQueryBuilder(TASK_TYPE_FIELD, taskTypeToString(ADTaskType.getRealtimeTaskTypes())));
        }

        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder
            .query(query)
            .sort(EXECUTION_START_TIME_FIELD, SortOrder.DESC)
            // Search query "from" starts from 0.
            .from(maxOldAdTaskDocsPerDetector)
            .trackTotalHits(true)
            .size(MAX_OLD_AD_TASK_DOCS);
        logger.debug("000111222333 {}", sourceBuilder);
        searchRequest.source(sourceBuilder).indices(CommonName.DETECTION_STATE_INDEX);
        String detectorId = adTask.getDetectorId();

        deleteTaskDocs(detectorId, searchRequest, () -> {
            if (adTask.isHistoricalTask()) {
                runBatchResultAction(response, adTask, delegatedListener);
            } else {
                AnomalyDetectorJobResponse anomalyDetectorJobResponse = new AnomalyDetectorJobResponse(
                    response.getId(),
                    response.getVersion(),
                    response.getSeqNo(),
                    response.getPrimaryTerm(),
                    RestStatus.OK
                );
                delegatedListener.onResponse(anomalyDetectorJobResponse);
            }
        }, delegatedListener);
    }

    private void runBatchResultAction(IndexResponse response, ADTask adTask, ActionListener<AnomalyDetectorJobResponse> delegatedListener) {
        logger.info("runBatchResultAction for task {}, {}", adTask.getTaskId(), adTask.getTaskType());

        ActionListener<ADBatchAnomalyResultResponse> actionListener = ActionListener.wrap(r -> {
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
            delegatedListener.onResponse(anomalyDetectorJobResponse);
        },
            e -> {
                // dedicated listener which handle exceptions
                delegatedListener.onFailure(e);
            }
        );
        client.execute(ADBatchAnomalyResultAction.INSTANCE, new ADBatchAnomalyResultRequest(adTask), actionListener);
    }

    /**
     * Called by forwarding action
     * @param adTask ad entity task
     * @param listener action listerner
     */
    public void runBatchResultActionForEntity(ADTask adTask, ActionListener<AnomalyDetectorJobResponse> listener) {
        logger.info("runBatchResultActionForEntity for task {}, {}", adTask.getTaskId(), adTask.getTaskType());
        logger
            .debug(
                "3333333333 running entities {}, {}, pending entities count: {}",
                adTaskCacheManager.getRunningEntities(adTask.getDetectorId()),
                adTaskCacheManager.getRunningEntityCount(adTask.getDetectorId()),
                adTaskCacheManager.getPendingEntityCount(adTask.getDetectorId())
            );
        client.execute(ADBatchAnomalyResultAction.INSTANCE, new ADBatchAnomalyResultRequest(adTask), ActionListener.wrap(r -> {
            String remoteOrLocal = r.isRunTaskRemotely() ? "remote" : "local";
            logger
                .debug(
                    "AD entity task {} of detector {} dispatched to {} node {}",
                    adTask.getTaskId(),
                    adTask.getDetectorId(),
                    remoteOrLocal,
                    r.getNodeId()
                );
            AnomalyDetectorJobResponse anomalyDetectorJobResponse = new AnomalyDetectorJobResponse(
                adTask.getDetectorId(),// TODO: tune this
                0,
                0,
                0,
                RestStatus.OK
            );
            listener.onResponse(anomalyDetectorJobResponse);
        },
            e -> {
                // dedicated listener? this listener is from ForwardADTaskTransportAction, and it does't handle exceptions.
                listener.onFailure(e);
            }
        ));
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
        if (adTask.getTaskType().equals(ADTaskType.HISTORICAL_HC_DETECTOR.name())) {
            updateADHCDetectorTask(adTask.getDetectorId(), adTask.getTaskId(), updatedFields);
        } else {
            updateADTask(adTask.getTaskId(), updatedFields);
        }
    }

    public void updateADHCDetectorTask(String detectorId, String taskId, Map<String, Object> updatedFields) {
        updateADHCDetectorTask(detectorId, taskId, updatedFields, ActionListener.wrap(response -> {
            if (response.status() == RestStatus.OK) {
                logger.debug("Updated AD task successfully: {}, taskId: {}", response.status(), taskId);
            } else {
                logger.error("Failed to update AD task {}, status: {}", taskId, response.status());
            }
        }, e -> { logger.error("Failed to update task1: " + taskId, e); }));
    }

    public synchronized void updateADHCDetectorTask(
        String detectorId,
        String taskId,
        Map<String, Object> updatedFields,
        ActionListener<UpdateResponse> listener
    ) {
        Boolean updating = adTaskCacheManager.isDetectorTaskUpdating(detectorId);
        if (updating == null) {
            logger.info("HC detector task updating flag removed", detectorId, taskId);
            return;
        }
        if (!updating) {
            if (updatedFields.containsKey(STATE_FIELD) && updatedFields.get(STATE_FIELD).equals(ADTaskState.FINISHED)) {
                logger.info("Update HC detector task to state to finished. detectorId:{}, taskId:{}", detectorId, taskId);
            }
            adTaskCacheManager.setDetectorTaskUpdating(detectorId, true);
            updateADTask(taskId, updatedFields, ActionListener.wrap(r -> {
                adTaskCacheManager.setDetectorTaskUpdating(detectorId, false);
                listener.onResponse(r);
            }, e -> {
                adTaskCacheManager.setDetectorTaskUpdating(detectorId, false);
                listener.onFailure(e);
            }));
        } else {
            logger.info("HC detector task is updating, detectorId:{}, taskId:{}", detectorId, taskId);
        }
    }

    public void updateADTask(String taskId, Map<String, Object> updatedFields) {
        updateADTask(taskId, updatedFields, ActionListener.wrap(response -> {
            if (response.status() == RestStatus.OK) {
                logger.debug("Updated AD task successfully: {}", response.status());
            } else {
                logger.error("Failed to update AD task {}, status: {}", taskId, response.status());
            }
        }, e -> { logger.error("Failed to update task2: " + taskId, e); }));
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
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, detectorId));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(query).size(MAX_OLD_AD_TASK_DOCS);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(CommonName.DETECTION_STATE_INDEX).source(sourceBuilder);
        deleteTaskDocs(detectorId, searchRequest, function, listener);
    }

    private <T> void deleteTaskDocs(
        String detectorId,
        SearchRequest searchRequest,
        AnomalyDetectorFunction function,
        ActionListener<T> listener
    ) {
        ActionListener<SearchResponse> seachListener = ActionListener.wrap(r -> {
            Iterator<SearchHit> iterator = r.getHits().iterator();
            if (iterator.hasNext()) {
                BulkRequest bulkRequest = new BulkRequest();
                while (iterator.hasNext()) {
                    SearchHit searchHit = iterator.next();
                    try (
                        XContentParser parser = RestHandlerUtils
                            .createXContentParserFromRegistry(xContentRegistry, searchHit.getSourceRef())
                    ) {
                        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                        ADTask adTask = ADTask.parse(parser, searchHit.getId());
                        logger.info("ylwudebug: delete task id: {}", adTask.getTaskId());
                        adTaskCacheManager.addDeletedTask(adTask.getTaskId(), adTask.getTaskType());
                        bulkRequest.add(new DeleteRequest(CommonName.DETECTION_STATE_INDEX).id(adTask.getTaskId()));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }
                client.execute(BulkAction.INSTANCE, bulkRequest, ActionListener.wrap(res -> {
                    logger.info("AD tasks deleted for detector {}", detectorId);
                    deleteChildTasksAndADResults();
                    function.execute();
                }, e -> {
                    logger.warn("Failed to clean AD tasks for detector " + detectorId, e);
                    listener.onFailure(e);
                }));
            } else {
                function.execute();
            }
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                function.execute();
            } else {
                listener.onFailure(e);
            }
        });

        client.search(searchRequest, seachListener);
    }

    public boolean deleteChildTasksAndADResults() {
        if (adTaskCacheManager.hasDeletedTask()) {
            threadPool.executor(AD_BATCH_TASK_THREAD_POOL_NAME).execute(() -> {
                String taskId = adTaskCacheManager.pollDeletedTask();
                DeleteByQueryRequest deleteChildTasksRequest = new DeleteByQueryRequest(CommonName.DETECTION_STATE_INDEX);
                deleteChildTasksRequest.setQuery(new TermsQueryBuilder(PARENT_TASK_ID_FIELD, taskId));

                client.execute(DeleteByQueryAction.INSTANCE, deleteChildTasksRequest, ActionListener.wrap(r -> {
                    logger.info("ylwudebug1: Successfully deleted child tasks of task " + taskId);
                    DeleteByQueryRequest deleteADResultsRequest = new DeleteByQueryRequest(CommonName.ANOMALY_RESULT_INDEX_PATTERN);
                    deleteADResultsRequest.setQuery(new TermsQueryBuilder(TASK_ID_FIELD, taskId));
                    client
                        .execute(
                            DeleteByQueryAction.INSTANCE,
                            deleteADResultsRequest,
                            ActionListener
                                .wrap(
                                    res -> { logger.info("ylwudebug1: Successfully deleted AD results of task " + taskId); },
                                    ex -> { logger.error("ylwudebug1: Failed to delete AD results for task " + taskId, ex); }
                                )
                        );
                }, e -> { logger.error("ylwudebug1: Failed to delete child tasks for task " + taskId, e); }));

            });
            return true;
        } else {
            return false;
        }
    }

    /**
     * Remove detector from cache on coordinating node.
     *
     * @param detectorId detector id
     */
    public void removeDetectorFromCache(String detectorId) {
        adTaskCacheManager.removeDetector(detectorId);
    }

    // TODO: run only on coordinating node
    public boolean hcDetectorDone(String detectorId) {
        return !adTaskCacheManager.hasEntity(detectorId);
    }

    public void updateLatestRealtimeADTask(String detectorId, Map<String, Object> updatedFields) {
        updateLatestADTask(detectorId, ADTaskType.getRealtimeTaskTypes(), updatedFields);
    }

    public void updateLatestADTask(String detectorId, List<ADTaskType> taskTypes, Map<String, Object> updatedFields) {
        updateLatestADTask(
            detectorId,
            taskTypes,
            updatedFields,
            ActionListener.wrap(r -> logger.info("updated"), e -> logger.warn("failed to update latest task for detector {}", detectorId))
        );
    }

    public void updateLatestADTask(
        String detectorId,
        List<ADTaskType> taskTypes,
        Map<String, Object> updatedFields,
        ActionListener listener
    ) {
        getLatestADTask(detectorId, taskTypes, (adTask) -> {
            if (adTask.isPresent()) {
                updateADTask(adTask.get().getTaskId(), updatedFields);
            }
        }, null, listener);
    }

    public void pushBackEntityToCache(String taskId, String detectorId, String entity) {
        adTaskCacheManager.addPendingEntity(detectorId, entity);
        adTaskCacheManager.increaseEntityTaskRetry(detectorId, taskId);
    }

    public void removeRunningEntity(String detectorId, List<Entity> entity) {
        if (entity != null && entity.size() > 0) {
            // TODO: support multiple category fields
            adTaskCacheManager.removeRunningEntity(detectorId, entity.get(0).getValue());
        }
    }

    public float hcDetectorProgress(String detectorId) {
        int entityCount = adTaskCacheManager.getTopEntityCount(detectorId);
        int leftEntities = adTaskCacheManager.getPendingEntityCount(detectorId) + adTaskCacheManager.getRunningEntityCount(detectorId);
        return 1 - (float) leftEntities / entityCount;
    }

    public void clearPendingEntities(String detectorId) {
        adTaskCacheManager.clearPendingEntities(detectorId);
    }

    public boolean taskRetryExceedLimits(String detectorId, String taskId) {
        return adTaskCacheManager.exceedRetryLimit(detectorId, taskId);
    }

    public boolean retryableError(String error) {
        if (error == null) {
            return false;
        }
        return retryableErrors.stream().filter(e -> error.contains(e)).findFirst().isPresent();
    }

    public void countEntityTasks(String detectorTaskId, List<ADTaskState> taskStates, ActionListener<Long> listener) {

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        queryBuilder.filter(new TermQueryBuilder(PARENT_TASK_ID_FIELD, detectorTaskId));
        if (taskStates != null && taskStates.size() > 0) {
            queryBuilder.filter(new TermsQueryBuilder(STATE_FIELD, taskStates.stream().map(s -> s.name()).collect(Collectors.toList())));
        }
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);
        sourceBuilder.size(0);
        sourceBuilder.trackTotalHits(true);
        // logger.info("8888888888 {}", sourceBuilder);
        SearchRequest request = new SearchRequest();
        request.source(sourceBuilder);
        request.indices(CommonName.DETECTION_STATE_INDEX);
        client.search(request, ActionListener.wrap(r -> {
            TotalHits totalHits = r.getHits().getTotalHits();
            listener.onResponse(totalHits.value);
        }, e -> listener.onFailure(e)));
    }

    public synchronized void removeStaleRunningEntity(ADTask adTask, String entity, ActionListener<AnomalyDetectorJobResponse> listener) {
        logger.debug("4444444444 remove stale entity {} for task {}", entity, adTask.getTaskId());
        boolean removed = adTaskCacheManager.removeRunningEntity(adTask.getDetectorId(), entity);
        if (removed && adTaskCacheManager.getPendingEntityCount(adTask.getDetectorId()) > 0) {
            logger.debug("4444444444 run next pending entities");
            this.runBatchResultActionForEntity(adTask, listener);
        } else {
            logger.debug("4444444444 no pending entities");
            setHCDetectorTaskDone(adTask, ADTaskState.FINISHED, null, listener);
        }
    }

    public void setHCDetectorTaskDone(
        ADTask adTask,
        ADTaskState state,
        String errorMsg,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        String detectorId = adTask.getDetectorId();
        String taskId = adTask.isEntityTask() ? adTask.getParentTaskId() : adTask.getTaskId();
        // logger.info("Set HC task as {} : task id {}", state.name(), taskId);

        String detectorTaskId = adTask.isEntityTask() ? adTask.getParentTaskId() : adTask.getTaskId();

        ActionListener<UpdateResponse> wrappedListener = ActionListener.wrap(response -> {
            logger.info("Historical HC detector done with state: {}. Remove from cache, detector id:{}", state.name(), detectorId);
            this.removeDetectorFromCache(detectorId);
            // if (response.status() == RestStatus.OK) {
            // logger.debug("Updated AD task successfully: {}, taskId: {}", response.status(), taskId);
            // } else {
            // logger.error("Failed to update AD task {}, status: {}", taskId, response.status());
            // }
        }, e -> {
            logger.error("Failed to update task: " + taskId, e);
            logger.info("Historical HC detector done with state: {}. Remove from cache, detector id:{}", state.name(), detectorId);
            this.removeDetectorFromCache(detectorId);
        });

        if (state == ADTaskState.FINISHED) {
            this.countEntityTasks(detectorTaskId, ImmutableList.of(ADTaskState.FINISHED), ActionListener.wrap(r -> {
                logger.info("number of finished entity tasks: {}, for detector {}", r, adTask.getDetectorId());
                ADTaskState hcDetectorTaskState = r == 0 ? ADTaskState.FAILED : ADTaskState.FINISHED;
                this
                    .updateADHCDetectorTask(
                        detectorId,
                        taskId,
                        ImmutableMap
                            .of(
                                STATE_FIELD,
                                hcDetectorTaskState.name(),
                                TASK_PROGRESS_FIELD,
                                1.0,
                                EXECUTION_END_TIME_FIELD,
                                Instant.now().toEpochMilli()
                            ),
                        wrappedListener
                    );
            }, e -> {
                logger.error("Failed to get finished entity tasks", e);
                this
                    .updateADHCDetectorTask(
                        detectorId,
                        taskId,
                        ImmutableMap
                            .of(
                                STATE_FIELD,
                                ADTaskState.FAILED.name(),
                                TASK_PROGRESS_FIELD,
                                1.0,
                                ERROR_FIELD,
                                getErrorMessage(e),
                                EXECUTION_END_TIME_FIELD,
                                Instant.now().toEpochMilli()
                            ),
                        wrappedListener
                    );// TODO: check how to handle if no entity case, if there is only 1 entity, false will not work
            }));
        } else {
            this
                .updateADHCDetectorTask(
                    detectorId,
                    taskId,
                    ImmutableMap
                        .of(
                            STATE_FIELD,
                            state.name(),
                            ERROR_FIELD,
                            adTask.getError(),
                            EXECUTION_END_TIME_FIELD,
                            Instant.now().toEpochMilli()
                        ),
                    wrappedListener
                );
        }

        listener.onResponse(new AnomalyDetectorJobResponse(taskId, 0, 0, 0, RestStatus.OK));
    }

    public void debugAddRunningEntity(String detectorId) {
        adTaskCacheManager.moveToRunningEntity(detectorId, "aaabbbccc");
        adTaskCacheManager.addPendingEntity(detectorId, "dddeeefff");
    }
}
