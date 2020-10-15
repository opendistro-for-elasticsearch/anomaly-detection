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

package com.amazon.opendistroforelasticsearch.ad.transport;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.NodeStateManager;
import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.cluster.HashRing;
import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ClientException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.InternalFailure;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ResourceNotFoundException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.feature.FeatureManager;
import com.amazon.opendistroforelasticsearch.ad.feature.SearchFeatureDao;
import com.amazon.opendistroforelasticsearch.ad.feature.SinglePointFeatures;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelPartitioner;
import com.amazon.opendistroforelasticsearch.ad.ml.RcfResult;
import com.amazon.opendistroforelasticsearch.ad.ml.rcf.CombinedRcfResult;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.FeatureData;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.settings.EnabledSetting;
import com.amazon.opendistroforelasticsearch.ad.stats.ADStats;
import com.amazon.opendistroforelasticsearch.ad.stats.StatNames;
import com.amazon.opendistroforelasticsearch.ad.util.ExceptionUtil;
import com.amazon.opendistroforelasticsearch.ad.util.ParseUtils;

public class AnomalyResultTransportAction extends HandledTransportAction<ActionRequest, AnomalyResultResponse> {

    private static final Logger LOG = LogManager.getLogger(AnomalyResultTransportAction.class);
    static final String NO_MODEL_ERR_MSG = "No RCF models are available either because RCF"
        + " models are not ready or all nodes are unresponsive or the system might have bugs.";
    static final String WAIT_FOR_THRESHOLD_ERR_MSG = "Exception in waiting for threshold result";
    static final String NODE_UNRESPONSIVE_ERR_MSG = "Model node is unresponsive.  Mute model";
    static final String READ_WRITE_BLOCKED = "Cannot read/write due to global block.";
    static final String INDEX_READ_BLOCKED = "Cannot read user index due to read block.";
    static final String LIMIT_EXCEEDED_EXCEPTION_NAME_UNDERSCORE = ElasticsearchException
        .getExceptionName(new LimitExceededException("", ""));
    static final String NULL_RESPONSE = "Received null response from";
    static final String BUG_RESPONSE = "We might have bugs.";

    private final TransportService transportService;
    private final NodeStateManager stateManager;
    private final FeatureManager featureManager;
    private final ModelPartitioner modelPartitioner;
    private final ModelManager modelManager;
    private final HashRing hashRing;
    private final TransportRequestOptions option;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ADStats adStats;
    private final ADCircuitBreakerService adCircuitBreakerService;
    private final ThreadPool threadPool;
    private final SearchFeatureDao searchFeatureDao;

    @Inject
    public AnomalyResultTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        Settings settings,
        NodeStateManager manager,
        FeatureManager featureManager,
        ModelManager modelManager,
        ModelPartitioner modelPartitioner,
        HashRing hashRing,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ADCircuitBreakerService adCircuitBreakerService,
        ADStats adStats,
        ThreadPool threadPool,
        SearchFeatureDao searchFeatureDao
    ) {
        super(AnomalyResultAction.NAME, transportService, actionFilters, AnomalyResultRequest::new);
        this.transportService = transportService;
        this.stateManager = manager;
        this.featureManager = featureManager;
        this.modelPartitioner = modelPartitioner;
        this.modelManager = modelManager;
        this.hashRing = hashRing;
        this.option = TransportRequestOptions
            .builder()
            .withType(TransportRequestOptions.Type.REG)
            .withTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings))
            .build();
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.adCircuitBreakerService = adCircuitBreakerService;
        this.adStats = adStats;
        this.threadPool = threadPool;
        this.searchFeatureDao = searchFeatureDao;
    }

    /**
     * All the exceptions thrown by AD is a subclass of AnomalyDetectionException.
     *  ClientException is a subclass of AnomalyDetectionException. All exception visible to
     *   Client is under ClientVisible. Two classes directly extends ClientException:
     *   - InternalFailure for "root cause unknown failure. Maybe transient." We can continue the
     *    detector running.
     *   - EndRunException for "failures that might impact the customer." The method endNow() is
     *    added to indicate whether the client should immediately terminate running a detector.
     *      + endNow() returns true for "unrecoverable issue". We want to terminate the detector run
     *       immediately.
     *      + endNow() returns false for "maybe unrecoverable issue but worth retrying a few more
     *       times." We want to wait for a few more times on different requests before terminating
     *        the detector run.
     *
     *  AD may not be able to get an anomaly grade but can find a feature vector.  Consider the
     *   case when the shingle is not ready.  In that case, AD just put NaN as anomaly grade and
     *    return the feature vector. If AD cannot even find a feature vector, AD throws
     *     EndRunException if there is an issue or returns empty response (all the numeric fields
     *      are Double.NaN and feature array is empty.  Do so so that customer can write painless
     *       script.) otherwise.
     *
     *  Also, AD is responsible for logging the stack trace.  To avoid bloating our logs, alerting
     *   should always just log the message of an AnomalyDetectionException exception by default.
     *
     *  Known causes of EndRunException with endNow returning false:
     *   + training data for cold start not available
     *   + cold start cannot succeed
     *   + unknown prediction error
     *   + memory circuit breaker tripped
     *
     *  Known causes of EndRunException with endNow returning true:
     *   + a model partition's memory size reached limit
     *   + models' total memory size reached limit
     *   + Having trouble querying feature data due to
     *    * index does not exist
     *    * all features have been disabled
     *    * invalid search query
     *   + anomaly detector is not available
     *   + AD plugin is disabled
     *   + training data is invalid due to serious internal bug(s)
     *
     *  Known causes of InternalFailure:
     *   + threshold model node is not available
     *   + cluster read/write is blocked
     *   + cold start hasn't been finished
     *   + fail to get all of rcf model nodes' responses
     *   + fail to get threshold model node's response
     *   + RCF/Threshold model node failing to get checkpoint to restore model before timeout
     *   + Detection is throttle because previous detection query is running
     *
     */
    @Override
    protected void doExecute(Task task, ActionRequest actionRequest, ActionListener<AnomalyResultResponse> listener) {

        AnomalyResultRequest request = AnomalyResultRequest.fromActionRequest(actionRequest);
        ActionListener<AnomalyResultResponse> original = listener;
        listener = ActionListener.wrap(original::onResponse, e -> {
            adStats.getStat(StatNames.AD_EXECUTE_FAIL_COUNT.getName()).increment();
            original.onFailure(e);
        });

        String adID = request.getAdID();

        if (!EnabledSetting.isADPluginEnabled()) {
            throw new EndRunException(adID, CommonErrorMessages.DISABLED_ERR_MSG, true);
        }

        adStats.getStat(StatNames.AD_EXECUTE_REQUEST_COUNT.getName()).increment();

        if (adCircuitBreakerService.isOpen()) {
            listener.onFailure(new LimitExceededException(adID, CommonErrorMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG, false));
            return;
        }

        try {
            stateManager.getAnomalyDetector(adID, onGetDetector(listener, adID, request));
        } catch (Exception ex) {
            handleExecuteException(ex, listener, adID);
        }
    }

    private ActionListener<Optional<AnomalyDetector>> onGetDetector(
        ActionListener<AnomalyResultResponse> listener,
        String adID,
        AnomalyResultRequest request
    ) {
        return ActionListener.wrap(detectorOptional -> {
            if (!detectorOptional.isPresent()) {
                listener.onFailure(new EndRunException(adID, "AnomalyDetector is not available.", true));
                return;
            }

            AnomalyDetector anomalyDetector = detectorOptional.get();

            long delayMillis = Optional
                .ofNullable((IntervalTimeConfiguration) anomalyDetector.getWindowDelay())
                .map(t -> t.toDuration().toMillis())
                .orElse(0L);
            long dataStartTime = request.getStart() - delayMillis;
            long dataEndTime = request.getEnd() - delayMillis;

            List<String> categoryField = anomalyDetector.getCategoryField();
            if (categoryField != null) {
                Optional<AnomalyDetectionException> previousException = stateManager.fetchColdStartException(adID);

                if (previousException.isPresent()) {
                    Exception exception = previousException.get();
                    LOG.error("Previous exception of {}: {}", adID, exception);
                    if (exception instanceof EndRunException) {
                        listener.onFailure(exception);
                        EndRunException endRunException = (EndRunException) exception;
                        if (endRunException.isEndNow()) {
                            return;
                        }
                    }
                }

                ActionListener<Map<String, double[]>> getEntityFeatureslistener = ActionListener.wrap(entityFeatures -> {
                    if (entityFeatures.isEmpty()) {
                        // Feature not available is common when we have data holes. Respond empty response
                        // so that alerting will not print stack trace to avoid bloating our logs.
                        LOG.info("No data in current detection window between {} and {} for {}", dataStartTime, dataEndTime, adID);
                        listener
                            .onResponse(
                                new AnomalyResultResponse(
                                    Double.NaN,
                                    Double.NaN,
                                    Double.NaN,
                                    new ArrayList<FeatureData>(),
                                    "No data in current detection window"
                                )
                            );
                    } else {
                        entityFeatures
                            .entrySet()
                            .stream()
                            .collect(
                                Collectors
                                    .groupingBy(
                                        e -> hashRing.getOwningNode(e.getKey()).get(),
                                        Collectors.toMap(Entry::getKey, Entry::getValue)
                                    )
                            )
                            .entrySet()
                            .stream()
                            .forEach(nodeEntity -> {
                                DiscoveryNode node = nodeEntity.getKey();
                                transportService
                                    .sendRequest(
                                        node,
                                        EntityResultAction.NAME,
                                        new EntityResultRequest(adID, nodeEntity.getValue(), dataStartTime, dataEndTime),
                                        this.option,
                                        new ActionListenerResponseHandler<>(
                                            new EntityResultListener(node.getId(), adID),
                                            AcknowledgedResponse::new,
                                            ThreadPool.Names.SAME
                                        )
                                    );
                            });
                    }

                    listener.onResponse(new AnomalyResultResponse(0, 0, 0, new ArrayList<FeatureData>()));
                }, exception -> handleFailure(exception, listener, adID));

                threadPool
                    .executor(AnomalyDetectorPlugin.AD_THREAD_POOL_NAME)
                    .execute(
                        () -> searchFeatureDao
                            .getFeaturesByEntities(
                                anomalyDetector,
                                dataStartTime,
                                dataEndTime,
                                new ThreadedActionListener<>(
                                    LOG,
                                    threadPool,
                                    AnomalyDetectorPlugin.AD_THREAD_POOL_NAME,
                                    getEntityFeatureslistener,
                                    false
                                )
                            )
                    );
                return;
            }

            String thresholdModelID = modelPartitioner.getThresholdModelId(adID);
            Optional<DiscoveryNode> asThresholdNode = hashRing.getOwningNode(thresholdModelID);
            if (!asThresholdNode.isPresent()) {
                listener.onFailure(new InternalFailure(adID, "Threshold model node is not available."));
                return;
            }

            DiscoveryNode thresholdNode = asThresholdNode.get();

            if (!shouldStart(listener, adID, anomalyDetector, thresholdNode.getId(), thresholdModelID)) {
                return;
            }

            featureManager
                .getCurrentFeatures(
                    anomalyDetector,
                    dataStartTime,
                    dataEndTime,
                    onFeatureResponse(adID, anomalyDetector, listener, thresholdModelID, thresholdNode, dataStartTime, dataEndTime)
                );
        }, exception -> handleExecuteException(exception, listener, adID));

    }

    private ActionListener<SinglePointFeatures> onFeatureResponse(
        String adID,
        AnomalyDetector detector,
        ActionListener<AnomalyResultResponse> listener,
        String thresholdModelID,
        DiscoveryNode thresholdNode,
        long dataStartTime,
        long dataEndTime
    ) {
        return ActionListener.wrap(featureOptional -> {
            List<FeatureData> featureInResponse = null;

            if (featureOptional.getUnprocessedFeatures().isPresent()) {
                featureInResponse = ParseUtils.getFeatureData(featureOptional.getUnprocessedFeatures().get(), detector);
            }

            if (!featureOptional.getProcessedFeatures().isPresent()) {
                Optional<AnomalyDetectionException> exception = coldStartIfNoCheckPoint(detector);
                if (exception.isPresent()) {
                    listener.onFailure(exception.get());
                    return;
                }

                if (!featureOptional.getUnprocessedFeatures().isPresent()) {
                    // Feature not available is common when we have data holes. Respond empty response
                    // so that alerting will not print stack trace to avoid bloating our logs.
                    LOG.info("No data in current detection window between {} and {} for {}", dataStartTime, dataEndTime, adID);
                    listener
                        .onResponse(
                            new AnomalyResultResponse(
                                Double.NaN,
                                Double.NaN,
                                Double.NaN,
                                new ArrayList<FeatureData>(),
                                "No data in current detection window"
                            )
                        );
                } else {
                    LOG.info("Return at least current feature value between {} and {} for {}", dataStartTime, dataEndTime, adID);
                    listener
                        .onResponse(
                            new AnomalyResultResponse(
                                Double.NaN,
                                Double.NaN,
                                Double.NaN,
                                featureInResponse,
                                "No full shingle in current detection window"
                            )
                        );
                }
                return;
            }

            // Can throw LimitExceededException when a single partition is more than X% of heap memory.
            // Compute this number once and the value won't change unless the coordinating AD node for an
            // detector changes or the cluster size changes.
            int rcfPartitionNum = stateManager.getPartitionNumber(adID, detector);

            List<RCFResultResponse> rcfResults = new ArrayList<>();

            final AtomicReference<AnomalyDetectionException> failure = new AtomicReference<AnomalyDetectionException>();

            final AtomicInteger responseCount = new AtomicInteger();

            for (int i = 0; i < rcfPartitionNum; i++) {
                String rcfModelID = modelPartitioner.getRcfModelId(adID, i);

                Optional<DiscoveryNode> rcfNode = hashRing.getOwningNode(rcfModelID.toString());
                if (!rcfNode.isPresent()) {
                    continue;
                }
                String rcfNodeId = rcfNode.get().getId();
                if (stateManager.isMuted(rcfNodeId)) {
                    LOG.info(String.format(Locale.ROOT, NODE_UNRESPONSIVE_ERR_MSG + " %s", rcfNodeId));
                    continue;
                }

                LOG.info("Sending RCF request to {} for model {}", rcfNodeId, rcfModelID);

                RCFActionListener rcfListener = new RCFActionListener(
                    rcfResults,
                    rcfModelID.toString(),
                    failure,
                    rcfNodeId,
                    detector,
                    listener,
                    thresholdModelID,
                    thresholdNode,
                    featureInResponse,
                    rcfPartitionNum,
                    responseCount,
                    adID,
                    detector.getEnabledFeatureIds().size()
                );

                transportService
                    .sendRequest(
                        rcfNode.get(),
                        RCFResultAction.NAME,
                        new RCFResultRequest(adID, rcfModelID, featureOptional.getProcessedFeatures().get()),
                        option,
                        new ActionListenerResponseHandler<>(rcfListener, RCFResultResponse::new)
                    );
            }
        }, exception -> { handleFailure(exception, listener, adID); });
    }

    private void handleFailure(Exception exception, ActionListener<AnomalyResultResponse> listener, String adID) {
        if (exception instanceof IndexNotFoundException) {
            listener.onFailure(new EndRunException(adID, "Having trouble querying data: " + exception.getMessage(), true));
        } else if (exception instanceof EndRunException) {
            // invalid feature query
            listener.onFailure(exception);
        } else {
            handleExecuteException(exception, listener, adID);
        }
    }

    /**
     * Verify failure of rcf or threshold models. If there is no model, trigger cold
     * start. If there is an exception for the previous cold start of this detector,
     * throw exception to the caller.
     *
     * @param failure  object that may contain exceptions thrown
     * @param detector detector object
     * @return exception if AD job execution gets resource not found exception
     * @throws AnomalyDetectionException List of exceptions we can throw
     *     1. Exception from cold start:
     *       1). InternalFailure due to
     *         a. ElasticsearchTimeoutException thrown by putModelCheckpoint during cold start
     *       2). EndRunException with endNow equal to false
     *         a. training data not available
     *         b. cold start cannot succeed
     *         c. invalid training data
     *       3) EndRunException with endNow equal to true
     *         a. invalid search query
     *     2. LimitExceededException from one of RCF model node when the total size of the models
     *      is more than X% of heap memory.
     *     3. InternalFailure wrapping ElasticsearchTimeoutException inside caused by
     *      RCF/Threshold model node failing to get checkpoint to restore model before timeout.
     */
    private AnomalyDetectionException coldStartIfNoModel(AtomicReference<AnomalyDetectionException> failure, AnomalyDetector detector)
        throws AnomalyDetectionException {
        AnomalyDetectionException exp = failure.get();
        if (exp == null) {
            return null;
        }

        // rethrow exceptions like LimitExceededException to caller
        if (!(exp instanceof ResourceNotFoundException)) {
            throw exp;
        }

        // fetch previous cold start exception
        String adID = detector.getDetectorId();
        final Optional<AnomalyDetectionException> previousException = stateManager.fetchColdStartException(adID);
        if (previousException.isPresent()) {
            Exception exception = previousException.get();
            LOG.error("Previous exception of {}: {}", () -> adID, () -> exception);
            if (exception instanceof EndRunException && ((EndRunException) exception).isEndNow()) {
                return (EndRunException) exception;
            }
        }
        LOG.info("Trigger cold start for {}", detector.getDetectorId());
        coldStart(detector);
        return previousException.orElse(new InternalFailure(adID, NO_MODEL_ERR_MSG));
    }

    private void findException(Throwable cause, String adID, AtomicReference<AnomalyDetectionException> failure) {
        if (cause instanceof Error) {
            // we cannot do anything with Error.
            LOG.error(new ParameterizedMessage("Error during prediction for {}: ", adID), cause);
            return;
        }

        Exception causeException = (Exception) cause;
        if (ExceptionUtil
            .isException(causeException, ResourceNotFoundException.class, ExceptionUtil.RESOURCE_NOT_FOUND_EXCEPTION_NAME_UNDERSCORE)
            || (causeException instanceof IndexNotFoundException
                && causeException.getMessage().contains(CommonName.CHECKPOINT_INDEX_NAME))) {
            failure.set(new ResourceNotFoundException(adID, causeException.getMessage()));
        } else if (ExceptionUtil.isException(causeException, LimitExceededException.class, LIMIT_EXCEEDED_EXCEPTION_NAME_UNDERSCORE)) {
            failure.set(new LimitExceededException(adID, causeException.getMessage()));
        } else if (causeException instanceof ElasticsearchTimeoutException) {
            // we can have ElasticsearchTimeoutException when a node tries to load RCF or
            // threshold model
            failure.set(new InternalFailure(adID, causeException));
        } else {
            // some unexpected bugs occur while predicting anomaly
            failure.set(new EndRunException(adID, BUG_RESPONSE, causeException, false));
        }
    }

    private CombinedRcfResult getCombinedResult(List<RCFResultResponse> rcfResults, int numFeatures) {
        List<RcfResult> rcfResultLib = new ArrayList<>();
        for (RCFResultResponse result : rcfResults) {
            rcfResultLib.add(new RcfResult(result.getRCFScore(), result.getConfidence(), result.getForestSize(), result.getAttribution()));
        }
        return modelManager.combineRcfResults(rcfResultLib, numFeatures);
    }

    void handleExecuteException(Exception ex, ActionListener<AnomalyResultResponse> listener, String adID) {
        if (ex instanceof ClientException) {
            listener.onFailure(ex);
        } else if (ex instanceof AnomalyDetectionException) {
            listener.onFailure(new InternalFailure((AnomalyDetectionException) ex));
        } else {
            Throwable cause = ExceptionsHelper.unwrapCause(ex);
            listener.onFailure(new InternalFailure(adID, cause));
        }
    }

    class RCFActionListener implements ActionListener<RCFResultResponse> {
        private List<RCFResultResponse> rcfResults;
        private String modelID;
        private AtomicReference<AnomalyDetectionException> failure;
        private String rcfNodeID;
        private AnomalyDetector detector;
        private ActionListener<AnomalyResultResponse> listener;
        private String thresholdModelID;
        private DiscoveryNode thresholdNode;
        private List<FeatureData> featureInResponse;
        private int nodeCount;
        private final AtomicInteger responseCount;
        private final String adID;
        private int numEnabledFeatures;

        RCFActionListener(
            List<RCFResultResponse> rcfResults,
            String modelID,
            AtomicReference<AnomalyDetectionException> failure,
            String rcfNodeID,
            AnomalyDetector detector,
            ActionListener<AnomalyResultResponse> listener,
            String thresholdModelID,
            DiscoveryNode thresholdNode,
            List<FeatureData> features,
            int nodeCount,
            AtomicInteger responseCount,
            String adID,
            int numEnabledFeatures
        ) {
            this.rcfResults = rcfResults;
            this.modelID = modelID;
            this.rcfNodeID = rcfNodeID;
            this.detector = detector;
            this.listener = listener;
            this.thresholdNode = thresholdNode;
            this.thresholdModelID = thresholdModelID;
            this.featureInResponse = features;
            this.failure = failure;
            this.nodeCount = nodeCount;
            this.responseCount = responseCount;
            this.adID = adID;
            this.numEnabledFeatures = numEnabledFeatures;
        }

        @Override
        public void onResponse(RCFResultResponse response) {
            try {
                stateManager.resetBackpressureCounter(rcfNodeID);
                if (response != null) {
                    rcfResults.add(response);
                } else {
                    LOG.warn(NULL_RESPONSE + " {} for {}", modelID, rcfNodeID);
                }
            } catch (Exception ex) {
                LOG.error("Unexpected exception: {} for {}", ex, adID);
            } finally {
                if (nodeCount == responseCount.incrementAndGet()) {
                    handleRCFResults(numEnabledFeatures);
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            try {
                handlePredictionFailure(e, adID, rcfNodeID, failure);
            } catch (Exception ex) {
                LOG.error("Unexpected exception: {} for {}", ex, adID);
            } finally {
                if (nodeCount == responseCount.incrementAndGet()) {
                    handleRCFResults(numEnabledFeatures);
                }
            }
        }

        private void handleRCFResults(int numFeatures) {
            try {
                AnomalyDetectionException exception = coldStartIfNoModel(failure, detector);
                if (exception != null) {
                    listener.onFailure(exception);
                    return;
                }

                if (rcfResults.isEmpty()) {
                    listener.onFailure(new InternalFailure(adID, NO_MODEL_ERR_MSG));
                    return;
                }

                CombinedRcfResult combinedResult = getCombinedResult(rcfResults, numFeatures);
                double combinedScore = combinedResult.getScore();

                final AtomicReference<AnomalyResultResponse> anomalyResultResponse = new AtomicReference<>();

                String thresholdNodeId = thresholdNode.getId();
                LOG.info("Sending threshold request to {} for model {}", thresholdNodeId, thresholdModelID);
                ThresholdActionListener thresholdListener = new ThresholdActionListener(
                    anomalyResultResponse,
                    featureInResponse,
                    thresholdNodeId,
                    detector,
                    combinedResult,
                    listener,
                    adID
                );
                transportService
                    .sendRequest(
                        thresholdNode,
                        ThresholdResultAction.NAME,
                        new ThresholdResultRequest(adID, thresholdModelID, combinedScore),
                        option,
                        new ActionListenerResponseHandler<>(thresholdListener, ThresholdResultResponse::new)
                    );
            } catch (Exception ex) {
                handleExecuteException(ex, listener, adID);
            }
        }
    }

    class ThresholdActionListener implements ActionListener<ThresholdResultResponse> {
        private AtomicReference<AnomalyResultResponse> anomalyResultResponse;
        private List<FeatureData> features;
        private AtomicReference<AnomalyDetectionException> failure;
        private String thresholdNodeID;
        private ActionListener<AnomalyResultResponse> listener;
        private AnomalyDetector detector;
        private CombinedRcfResult combinedResult;
        private String adID;

        ThresholdActionListener(
            AtomicReference<AnomalyResultResponse> anomalyResultResponse,
            List<FeatureData> features,
            String thresholdNodeID,
            AnomalyDetector detector,
            CombinedRcfResult combinedResult,
            ActionListener<AnomalyResultResponse> listener,
            String adID
        ) {
            this.anomalyResultResponse = anomalyResultResponse;
            this.features = features;
            this.thresholdNodeID = thresholdNodeID;
            this.detector = detector;
            this.combinedResult = combinedResult;
            this.failure = new AtomicReference<AnomalyDetectionException>();
            this.listener = listener;
            this.adID = adID;
        }

        @Override
        public void onResponse(ThresholdResultResponse response) {
            try {
                anomalyResultResponse
                    .set(new AnomalyResultResponse(response.getAnomalyGrade(), response.getConfidence(), Double.NaN, features));
                stateManager.resetBackpressureCounter(thresholdNodeID);
            } catch (Exception ex) {
                LOG.error("Unexpected exception: {} for {}", ex, adID);
            } finally {
                handleThresholdResult();
            }
        }

        @Override
        public void onFailure(Exception e) {
            try {
                handlePredictionFailure(e, adID, thresholdNodeID, failure);
            } catch (Exception ex) {
                LOG.error("Unexpected exception: {} for {}", ex, adID);
            } finally {
                handleThresholdResult();
            }
        }

        private void handleThresholdResult() {
            try {
                AnomalyDetectionException exception = coldStartIfNoModel(failure, detector);
                if (exception != null) {
                    listener.onFailure(exception);
                    return;
                }

                if (anomalyResultResponse.get() != null) {
                    AnomalyResultResponse response = anomalyResultResponse.get();
                    double confidence = response.getConfidence() * combinedResult.getConfidence();
                    response = new AnomalyResultResponse(
                        response.getAnomalyGrade(),
                        confidence,
                        combinedResult.getScore(),
                        response.getFeatures()
                    );
                    listener.onResponse(response);
                } else if (failure.get() != null) {
                    listener.onFailure(failure.get());
                } else {
                    listener.onFailure(new InternalFailure(adID, "Node connection problem or unexpected exception"));
                }
            } catch (Exception ex) {
                handleExecuteException(ex, listener, adID);
            }
        }
    }

    private void handlePredictionFailure(Exception e, String adID, String nodeID, AtomicReference<AnomalyDetectionException> failure) {
        LOG.error(new ParameterizedMessage("Received an error from node {} while fetching anomaly grade for {}", nodeID, adID), e);
        if (e == null) {
            return;
        }
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        if (hasConnectionIssue(cause)) {
            handleConnectionException(nodeID);
        } else {
            findException(cause, adID, failure);
        }
    }

    /**
     * Check if the input exception indicates connection issues.
     *
     * @param e exception
     * @return true if we get disconnected from the node or the node is not in the
     *         right state (being closed) or transport request times out (sent from TimeoutHandler.run)
     */
    private boolean hasConnectionIssue(Throwable e) {
        return e instanceof ConnectTransportException
            || e instanceof NodeClosedException
            || e instanceof ReceiveTimeoutTransportException
            || e instanceof NodeNotConnectedException
            || e instanceof ConnectException;
    }

    private void handleConnectionException(String node) {
        final DiscoveryNodes nodes = clusterService.state().nodes();
        if (!nodes.nodeExists(node) && hashRing.build()) {
            return;
        }
        // rebuilt is not done or node is unresponsive
        stateManager.addPressure(node);
    }

    /**
     * Since we need to read from customer index and write to anomaly result index,
     * we need to make sure we can read and write.
     *
     * @param state Cluster state
     * @return whether we have global block or not
     */
    private boolean checkGlobalBlock(ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ) != null
            || state.blocks().globalBlockedException(ClusterBlockLevel.WRITE) != null;
    }

    /**
     * Similar to checkGlobalBlock, we check block on the indices level.
     *
     * @param state   Cluster state
     * @param level   block level
     * @param indices the indices on which to check block
     * @return whether any of the index has block on the level.
     */
    private boolean checkIndicesBlocked(ClusterState state, ClusterBlockLevel level, String... indices) {
        // the original index might be an index expression with wildcards like "log*",
        // so we need to expand the expression to concrete index name
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, IndicesOptions.lenientExpandOpen(), indices);

        return state.blocks().indicesBlockedException(level, concreteIndices) != null;
    }

    /**
     * Check if we should start anomaly prediction.
     *
     * @param listener listener to respond back to AnomalyResultRequest.
     * @param adID     detector ID
     * @param detector detector instance corresponds to adID
     * @param thresholdNodeId the threshold model hosting node ID for adID
     * @param thresholdModelID the threshold model ID for adID
     * @return if we can start anomaly prediction.
     */
    private boolean shouldStart(
        ActionListener<AnomalyResultResponse> listener,
        String adID,
        AnomalyDetector detector,
        String thresholdNodeId,
        String thresholdModelID
    ) {
        ClusterState state = clusterService.state();
        if (checkGlobalBlock(state)) {
            listener.onFailure(new InternalFailure(adID, READ_WRITE_BLOCKED));
            return false;
        }

        if (stateManager.isMuted(thresholdNodeId)) {
            listener.onFailure(new InternalFailure(adID, String.format(NODE_UNRESPONSIVE_ERR_MSG + " %s", thresholdModelID)));
            return false;
        }

        if (checkIndicesBlocked(state, ClusterBlockLevel.READ, detector.getIndices().toArray(new String[0]))) {
            listener.onFailure(new InternalFailure(adID, INDEX_READ_BLOCKED));
            return false;
        }

        return true;
    }

    private void coldStart(AnomalyDetector detector) {
        String detectorId = detector.getDetectorId();

        // If last cold start is not finished, we don't trigger another one
        if (stateManager.isColdStartRunning(detectorId)) {
            return;
        }

        final Releasable coldStartFinishingCallback = stateManager.markColdStartRunning(detectorId);

        ActionListener<Optional<double[][]>> listener = ActionListener.wrap(trainingData -> {
            if (trainingData.isPresent()) {
                double[][] dataPoints = trainingData.get();

                ActionListener<Void> trainModelListener = ActionListener
                    .wrap(res -> { LOG.info("Succeeded in training {}", detectorId); }, exception -> {
                        if (exception instanceof AnomalyDetectionException) {
                            // e.g., partitioned model exceeds memory limit
                            stateManager.setLastColdStartException(detectorId, (AnomalyDetectionException) exception);
                        } else if (exception instanceof IllegalArgumentException) {
                            // IllegalArgumentException due to invalid training data
                            stateManager
                                .setLastColdStartException(
                                    detectorId,
                                    new EndRunException(detectorId, "Invalid training data", exception, false)
                                );
                        } else if (exception instanceof ElasticsearchTimeoutException) {
                            stateManager
                                .setLastColdStartException(
                                    detectorId,
                                    new InternalFailure(detectorId, "Time out while indexing cold start checkpoint", exception)
                                );
                        } else {
                            stateManager
                                .setLastColdStartException(
                                    detectorId,
                                    new EndRunException(detectorId, "Error while training model", exception, false)
                                );
                        }
                    });

                modelManager
                    .trainModel(
                        detector,
                        dataPoints,
                        new ThreadedActionListener<>(LOG, threadPool, AnomalyDetectorPlugin.AD_THREAD_POOL_NAME, trainModelListener, false)
                    );
            } else {
                stateManager.setLastColdStartException(detectorId, new EndRunException(detectorId, "Cannot get training data", false));
            }
        }, exception -> {
            if (exception instanceof ElasticsearchTimeoutException) {
                stateManager
                    .setLastColdStartException(
                        detectorId,
                        new InternalFailure(detectorId, "Time out while getting training data", exception)
                    );
            } else if (exception instanceof AnomalyDetectionException) {
                // e.g., Invalid search query
                stateManager.setLastColdStartException(detectorId, (AnomalyDetectionException) exception);
            } else {
                stateManager
                    .setLastColdStartException(detectorId, new EndRunException(detectorId, "Error while cold start", exception, false));
            }
        });

        final ActionListener<Optional<double[][]>> listenerWithReleaseCallback = ActionListener
            .runAfter(listener, coldStartFinishingCallback::close);

        threadPool
            .executor(AnomalyDetectorPlugin.AD_THREAD_POOL_NAME)
            .execute(
                () -> featureManager
                    .getColdStartData(
                        detector,
                        new ThreadedActionListener<>(
                            LOG,
                            threadPool,
                            AnomalyDetectorPlugin.AD_THREAD_POOL_NAME,
                            listenerWithReleaseCallback,
                            false
                        )
                    )
            );
    }

    /**
     * Check if checkpoint for an detector exists or not.  If not and previous
     *  run is not EndRunException whose endNow is true, trigger cold start.
     * @param detector detector object
     * @return previous cold start exception
     */
    private Optional<AnomalyDetectionException> coldStartIfNoCheckPoint(AnomalyDetector detector) {
        String detectorId = detector.getDetectorId();

        Optional<AnomalyDetectionException> previousException = stateManager.fetchColdStartException(detectorId);

        if (previousException.isPresent()) {
            Exception exception = previousException.get();
            LOG.error("Previous exception of {}: {}", detectorId, exception);
            if (exception instanceof EndRunException && ((EndRunException) exception).isEndNow()) {
                return previousException;
            }
        }

        stateManager.getDetectorCheckpoint(detectorId, ActionListener.wrap(checkpointExists -> {
            if (!checkpointExists) {
                LOG.info("Trigger cold start for {}", detectorId);
                coldStart(detector);
            }
        }, exception -> {
            Throwable cause = ExceptionsHelper.unwrapCause(exception);
            if (cause instanceof IndexNotFoundException) {
                LOG.info("Trigger cold start for {}", detectorId);
                coldStart(detector);
            } else {
                String errorMsg = String.format("Fail to get checkpoint state for %s", detectorId);
                LOG.error(errorMsg, exception);
                stateManager.setLastColdStartException(detectorId, new AnomalyDetectionException(errorMsg, exception));
            }
        }));

        return previousException;
    }

    class EntityResultListener implements ActionListener<AcknowledgedResponse> {
        private String nodeId;
        private final String adID;

        EntityResultListener(String nodeId, String adID) {
            this.nodeId = nodeId;
            this.adID = adID;
        }

        @Override
        public void onResponse(AcknowledgedResponse response) {
            stateManager.resetBackpressureCounter(nodeId);
            if (response.isAcknowledged() == false) {
                LOG.error("Cannot send entities' features to {} for {}", nodeId, adID);
                stateManager.addPressure(nodeId);
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (e == null) {
                return;
            }
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (hasConnectionIssue(cause)) {
                handleConnectionException(nodeId);
            }
            LOG.error(new ParameterizedMessage("Cannot send entities' features to {} for {}", nodeId, adID), e);
        }
    }
}
