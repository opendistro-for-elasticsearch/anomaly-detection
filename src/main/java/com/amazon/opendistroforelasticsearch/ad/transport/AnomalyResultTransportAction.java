/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.time.Instant;

import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.cluster.HashRing;
import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ClientException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.InternalFailure;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ResourceNotFoundException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.feature.FeatureManager;
import com.amazon.opendistroforelasticsearch.ad.feature.SinglePointFeatures;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.RcfResult;
import com.amazon.opendistroforelasticsearch.ad.ml.rcf.CombinedRcfResult;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.FeatureData;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.stats.ADStats;
import com.amazon.opendistroforelasticsearch.ad.stats.StatNames;
import com.amazon.opendistroforelasticsearch.ad.util.ColdStartRunner;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.common.settings.Settings;

public class AnomalyResultTransportAction extends HandledTransportAction<ActionRequest, AnomalyResultResponse> {

    private static final Logger LOG = LogManager.getLogger(AnomalyResultTransportAction.class);
    static final String NO_MODEL_ERR_MSG = "No RCF models are available either because RCF"
        + " models are not ready or all nodes are unresponsive or the system might have bugs.";
    static final String WAIT_FOR_THRESHOLD_ERR_MSG = "Exception in waiting for threshold result";
    static final String NODE_UNRESPONSIVE_ERR_MSG = "Model node is unresponsive.  Mute model";
    static final String FAIL_TO_SAVE_ERR_MSG = "Fail to save anomaly index: ";
    static final String RETRY_SAVING_ERR_MSG = "Retry in saving anomaly index: ";
    static final String SUCCESS_SAVING_MSG = "Success in saving anomaly index: ";
    static final String READ_WRITE_BLOCKED = "Cannot read/write due to global block.";
    static final String INDEX_READ_BLOCKED = "Cannot read user index due to read block.";
    static final String CANNOT_SAVE_ERR_MSG = "Cannot save anomaly result due to write block.";
    static final String LIMIT_EXCEEDED_EXCEPTION_NAME_UNDERSCORE = ElasticsearchException
        .getExceptionName(new LimitExceededException("", ""));
    static final String RESOURCE_NOT_FOUND_EXCEPTION_NAME_UNDERSCORE = ElasticsearchException
        .getExceptionName(new ResourceNotFoundException("", ""));
    static final String NULL_RESPONSE = "Received null response from";

    private final TransportService transportService;
    private final ADStateManager stateManager;
    private final ColdStartRunner globalRunner;
    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private volatile TimeValue requestTimeout;
    private final Client client;
    private final FeatureManager featureManager;
    private final ModelManager modelManager;
    private final HashRing hashRing;
    private final TransportRequestOptions option;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ThreadPool threadPool;
    private final BackoffPolicy resultSavingBackoffPolicy;
    private final ADStats adStats;
    private final ADCircuitBreakerService adCircuitBreakerService;

    @Inject
    public AnomalyResultTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        Client client,
        Settings settings,
        ADStateManager manager,
        ColdStartRunner eventExecutor,
        AnomalyDetectionIndices anomalyDetectionIndices,
        FeatureManager featureManager,
        ModelManager modelManager,
        HashRing hashRing,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ThreadPool threadPool,
        ADCircuitBreakerService adCircuitBreakerService,
        ADStats adStats
    ) {
        super(AnomalyResultAction.NAME, transportService, actionFilters, AnomalyResultRequest::new);
        this.transportService = transportService;
        this.client = client;
        this.stateManager = manager;
        this.globalRunner = eventExecutor;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.requestTimeout = AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings);
        this.featureManager = featureManager;
        this.modelManager = modelManager;
        this.hashRing = hashRing;
        this.requestTimeout = AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings);
        this.option = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.REG).withTimeout(requestTimeout).build();
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.threadPool = threadPool;
        this.resultSavingBackoffPolicy = BackoffPolicy
            .exponentialBackoff(
                AnomalyDetectorSettings.BACKOFF_INITIAL_DELAY.get(settings),
                AnomalyDetectorSettings.MAX_RETRY_FOR_BACKOFF.get(settings)
            );
        this.adCircuitBreakerService = adCircuitBreakerService;
        this.adStats = adStats;
    }

    private List<FeatureData> getFeatureData(double[] currentFeature, AnomalyDetector detector) {
        List<String> featureIds = detector.getEnabledFeatureIds();
        List<String> featureNames = detector.getEnabledFeatureNames();
        int featureLen = featureIds.size();
        List<FeatureData> featureData = new ArrayList<>();
        for (int i = 0; i < featureLen; i++) {
            featureData.add(new FeatureData(featureIds.get(i), featureNames.get(i), currentFeature[i]));
        }
        return featureData;
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
     *  Known cause of EndRunException with endNow returning false:
     *   + training data for cold start not available
     *   + cold start cannot succeed
     *   + unknown prediction error
     *
     *  Known cause of EndRunException with endNow returning true:
     *   + anomaly detector is not available
     *   + a models' memory size reached limit
     *   + models' total memory size reached limit
     *
     *  Known cause of InternalFailure:
     *   + threshold model node is not available
     *   + cluster read/write is blocked
     *   + interrupted while waiting for rcf/threshold model nodes' responses
     *   + cold start hasn't been finished
     *   + fail to get all of rcf model nodes' responses
     *   + fail to get threshold model node's response
     *   + RCF/Threshold model node failing to get checkpoint to restore model before timeout
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

        adStats.getStat(StatNames.AD_EXECUTE_REQUEST_COUNT.getName()).increment();

        if (adCircuitBreakerService.isOpen()) {
            listener.onFailure(new LimitExceededException(adID, CommonErrorMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG));
            return;
        }

        try {
            Optional<AnomalyDetector> detector = stateManager.getAnomalyDetector(adID);
            if (!detector.isPresent()) {
                listener.onFailure(new EndRunException(adID, "AnomalyDetector is not available.", true));
                return;
            }
            AnomalyDetector anomalyDetector = detector.get();
            if (stateManager.hasRunningQuery(anomalyDetector)) {
                LOG.error("There is one query running for detectorId: {}", anomalyDetector.getDetectorId());
                listener.onFailure(new EndRunException(adID, "There is one query running on AnomalyDetector", true));
                return;
            }

            String thresholdModelID = modelManager.getThresholdModelId(adID);
            Optional<DiscoveryNode> thresholdNode = hashRing.getOwningNode(thresholdModelID);
            if (!thresholdNode.isPresent()) {
                listener.onFailure(new InternalFailure(adID, "Threshold model node is not available."));
                return;
            }

            String thresholdNodeId = thresholdNode.get().getId();

            if (!shouldStart(listener, adID, detector.get(), thresholdNodeId, thresholdModelID)) {
                return;
            }

            long delayMillis = Optional
                .ofNullable((IntervalTimeConfiguration) anomalyDetector.getWindowDelay())
                .map(t -> t.toDuration().toMillis())
                .orElse(0L);
            long startTime = request.getStart() - delayMillis;
            long endTime = request.getEnd() - delayMillis;

            SinglePointFeatures featureOptional = featureManager.getCurrentFeatures(anomalyDetector, startTime, endTime);

            List<FeatureData> featureInResponse = null;

            if (featureOptional.getUnprocessedFeatures().isPresent()) {
                featureInResponse = getFeatureData(featureOptional.getUnprocessedFeatures().get(), detector.get());
            }

            if (!featureOptional.getProcessedFeatures().isPresent()) {
                LOG.info("No full shingle in current detection window for {}", adID);
                if (!featureOptional.getUnprocessedFeatures().isPresent()) {
                    // Feature not available is common when we have data holes. Respond empty response
                    // so that alerting will not print stack trace to avoid bloating our logs.
                    LOG.info("No data in current detection window for {}", adID);
                    listener.onResponse(new AnomalyResultResponse(Double.NaN, Double.NaN, new ArrayList<FeatureData>()));
                } else {
                    LOG.info("Return at least current feature for {}", adID);
                    listener.onResponse(new AnomalyResultResponse(Double.NaN, Double.NaN, featureInResponse));
                }
                return;
            }

            // Can throw LimitExceededException when a single partition is more than X% of heap memory.
            // Compute this number once and the value won't change unless the coordinating AD node for an
            // detector changes or the cluster size changes.
            int rcfPartitionNum = stateManager.getPartitionNumber(adID);

            final CountDownLatch rcfLatch = createCountDownLatch(rcfPartitionNum);
            List<RCFResultResponse> rcfResults = new ArrayList<>();

            final AtomicReference<AnomalyDetectionException> failure = new AtomicReference<AnomalyDetectionException>();

            for (int i = 0; i < rcfPartitionNum; i++) {
                String rcfModelID = modelManager.getRcfModelId(adID, i);

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
                LatchedActionListener<RCFResultResponse> rcfListener = new LatchedActionListener<>(
                    new RCFActionListener(rcfResults, rcfModelID.toString(), failure, rcfNodeId),
                    rcfLatch
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

            // wait a bit longer than transport timeout
            long latchWaitSecs = Math.round(requestTimeout.getSeconds() * 1.25d);

            try {
                LOG.debug("Wait for RCF results...");
                rcfLatch.await(latchWaitSecs, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                listener.onFailure(new InternalFailure(adID, CommonErrorMessages.WAIT_ERR_MSG, e));
                return;
            }

            if (coldStartIfNoModel(failure, detector.get()) || rcfResults.isEmpty()) {
                listener.onFailure(new InternalFailure(adID, NO_MODEL_ERR_MSG));
                return;
            }

            CombinedRcfResult combinedResult = getCombinedResult(rcfResults);
            double combinedScore = combinedResult.getScore();

            final CountDownLatch thresholdLatch = createCountDownLatch(1);

            final AtomicReference<AnomalyResultResponse> anomalyResultResponse = new AtomicReference<>();

            LOG.info("Sending threshold request to {} for model {}", thresholdNodeId, thresholdModelID);
            LatchedActionListener<ThresholdResultResponse> thresholdListener = new LatchedActionListener<>(
                new ThresholdActionListener(anomalyResultResponse, featureInResponse, thresholdModelID, failure, thresholdNodeId),
                thresholdLatch
            );
            transportService
                .sendRequest(
                    thresholdNode.get(),
                    ThresholdResultAction.NAME,
                    new ThresholdResultRequest(adID, thresholdModelID, combinedScore),
                    option,
                    new ActionListenerResponseHandler<>(thresholdListener, ThresholdResultResponse::new)
                );

            try {
                LOG.debug("Wait for threshold results...");
                thresholdLatch.await(latchWaitSecs, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                listener.onFailure(new InternalFailure(adID, WAIT_FOR_THRESHOLD_ERR_MSG, e));
                return;
            }

            if (coldStartIfNoModel(failure, detector.get())) {
                listener.onFailure(new InternalFailure(adID, NO_MODEL_ERR_MSG));
                return;
            }

            if (anomalyResultResponse.get() != null) {
                AnomalyResultResponse response = anomalyResultResponse.get();
                double confidence = response.getConfidence() * combinedResult.getConfidence();
                response = new AnomalyResultResponse(response.getAnomalyGrade(), confidence, response.getFeatures());
                listener.onResponse(response);
                indexAnomalyResult(
                    new AnomalyResult(
                        adID,
                        Double.valueOf(combinedScore),
                        Double.valueOf(response.getAnomalyGrade()),
                        Double.valueOf(confidence),
                        featureInResponse,
                        Instant.ofEpochMilli(startTime),
                        Instant.ofEpochMilli(endTime)
                    )
                );
            } else if (failure.get() != null) {
                listener.onFailure(failure.get());
            } else {
                listener.onFailure(new InternalFailure(adID, "Unexpected exception"));
            }
        } catch (ClientException clientException) {
            listener.onFailure(clientException);
        } catch (AnomalyDetectionException adEx) {
            listener.onFailure(new InternalFailure(adEx));
        } catch (Exception throwable) {
            Throwable cause = ExceptionsHelper.unwrapCause(throwable);
            listener.onFailure(new InternalFailure(adID, cause));
        }

    }

    /**
     * Verify failure of rcf or threshold models. If there is no model, trigger cold
     * start. If there is an exception for the previous cold start of this detector,
     * throw exception to the caller.
     *
     * @param failure  object that may contain exceptions thrown
     * @param detector detector object
     * @return whether cold start runs
     * @throws AnomalyDetectionException List of exceptions we can throw
     *     1. Exception from cold start:
     *       1). InternalFailure due to
     *         a. ElasticsearchTimeoutException thrown by putModelCheckpoint during cold start
     *       2). EndRunException with endNow equal to false
     *         a. training data not available
     *         b. cold start cannot succeed
     *     2. LimitExceededException from one of RCF model node when the total size of the models
     *      is more than X% of heap memory.
     *     3. InternalFailure wrapping ElasticsearchTimeoutException inside caused by
     *      RCF/Threshold model node failing to get checkpoint to restore model before timeout.
     */
    private boolean coldStartIfNoModel(AtomicReference<AnomalyDetectionException> failure, AnomalyDetector detector)
        throws AnomalyDetectionException {
        AnomalyDetectionException exp = failure.get();
        if (exp != null) {
            if (exp instanceof ResourceNotFoundException) {
                LOG.info("Cold start for {}", detector.getDetectorId());
                globalRunner.compute(new ColdStartJob(detector));
                return true;
            } else {
                throw exp;
            }
        }
        return false;
    }

    private void findException(Throwable cause, String adID, AtomicReference<AnomalyDetectionException> failure) {
        if (cause instanceof Error) {
            // we cannot do anything with Error.
            LOG.error(new ParameterizedMessage("Error during prediction for {}: ", adID), cause);
            return;
        }

        Exception causeException = (Exception) cause;
        if (isException(causeException, ResourceNotFoundException.class, RESOURCE_NOT_FOUND_EXCEPTION_NAME_UNDERSCORE)) {
            // fetch previous cold start exception
            Optional<? extends AnomalyDetectionException> previousException = globalRunner.fetchException(adID);

            if (previousException.isPresent()) {
                LOG.error("Previous exception of {}: {}", () -> adID, () -> previousException.get());
                failure.set(previousException.get());
            } else {
                failure.set(new ResourceNotFoundException(adID, causeException.getMessage()));
            }
        } else if (isException(causeException, LimitExceededException.class, LIMIT_EXCEEDED_EXCEPTION_NAME_UNDERSCORE)) {
            failure.set(new LimitExceededException(adID, causeException.getMessage()));
        } else if (causeException instanceof ElasticsearchTimeoutException) {
            // we can have ElasticsearchTimeoutException when a node tries to load RCF or
            // threshold model
            failure.set(new InternalFailure(adID, causeException));
        } else {
            // some unexpected bugs occur while predicting anomaly
            failure.set(new EndRunException(adID, "We might have bugs.", causeException, false));
        }
    }

    /**
     * Elasticsearch restricts the kind of exceptions can be thrown over the wire
     * (See ElasticsearchException.ElasticsearchExceptionHandle). Since we cannot
     * add our own exception like ResourceNotFoundException without modifying
     * Elasticsearch's code, we have to unwrap the remote transport exception and
     * check its root cause message.
     *
     * @param exception exception thrown locally or over the wire
     * @param expected  expected root cause
     * @return whether the exception wraps the expected exception as the cause
     */
    private boolean isException(Throwable exception, Class<? extends Exception> expected, String expectedErrorName) {
        if (exception == null) {
            return false;
        }

        if (expected.isAssignableFrom(exception.getClass())) {
            return true;
        }

        // all exception that has not been registered to sent over wire can be wrapped
        // inside NotSerializableExceptionWrapper.
        // see StreamOutput.writeException
        // ElasticsearchException.getExceptionName(exception) returns exception
        // separated by underscore. For example, ResourceNotFoundException is converted
        // to "resource_not_found_exception".
        if (exception instanceof NotSerializableExceptionWrapper && exception.getMessage().trim().startsWith(expectedErrorName)) {
            return true;
        }
        return false;
    }

    private CombinedRcfResult getCombinedResult(List<RCFResultResponse> rcfResults) {
        List<RcfResult> rcfResultLib = new ArrayList<>();
        for (RCFResultResponse result : rcfResults) {
            rcfResultLib.add(new RcfResult(result.getRCFScore(), result.getConfidence(), result.getForestSize()));
        }
        return modelManager.combineRcfResults(rcfResultLib);
    }

    CountDownLatch createCountDownLatch(int count) {
        return new CountDownLatch(count);
    }

    /**
     * Saves the result unless an exception is thrown. The anomaly result index is
     * implicitly created if it does not exist.
     *
     * @param anomalyResult one anomaly result
     */
    void indexAnomalyResult(AnomalyResult anomalyResult) {
        try {
            if (checkIndicesBlocked(clusterService.state(), ClusterBlockLevel.WRITE, AnomalyResult.ANOMALY_RESULT_INDEX)) {
                LOG.warn(CANNOT_SAVE_ERR_MSG);
                return;
            }
            if (!anomalyDetectionIndices.doesAnomalyResultIndexExist()) {
                anomalyDetectionIndices
                    .initAnomalyResultIndex(
                        ActionListener.wrap(initResponse -> onCreateAnomalyResultIndexResponse(initResponse, anomalyResult), exception -> {
                            if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                                // It is possible the index has been created while we sending the create request
                                saveDetectorResult(anomalyResult);
                            } else {
                                throw new AnomalyDetectionException(
                                    anomalyResult.getDetectorId(),
                                    "Unexpected error creating anomaly result index",
                                    exception
                                );
                            }
                        })
                    );
            } else {
                saveDetectorResult(anomalyResult);
            }
        } catch (Exception e) {
            throw new AnomalyDetectionException(
                anomalyResult.getDetectorId(),
                String
                    .format(
                        Locale.ROOT,
                        "Error in saving anomaly index for ID %s from %s to %s",
                        anomalyResult.getDetectorId(),
                        anomalyResult.getStartTime(),
                        anomalyResult.getEndTime()
                    )
            );
        }
    }

    private void onCreateAnomalyResultIndexResponse(CreateIndexResponse response, AnomalyResult anomalyResult) {
        if (response.isAcknowledged()) {
            saveDetectorResult(anomalyResult);
        } else {
            throw new AnomalyDetectionException(
                anomalyResult.getDetectorId(),
                "Creating anomaly result index with mappings call not acknowledged."
            );
        }
    }

    private void saveDetectorResult(AnomalyResult anomalyResult) {
        try (XContentBuilder builder = jsonBuilder()) {
            IndexRequest indexRequest = new IndexRequest(AnomalyResult.ANOMALY_RESULT_INDEX)
                .source(anomalyResult.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE));
            saveDetectorResult(
                indexRequest,
                String
                    .format(
                        Locale.ROOT,
                        "ID %s from %s to %s",
                        anomalyResult.getDetectorId(),
                        anomalyResult.getStartTime(),
                        anomalyResult.getEndTime()
                    ),
                resultSavingBackoffPolicy.iterator()
            );
        } catch (Exception e) {
            throw new AnomalyDetectionException(anomalyResult.getDetectorId(), "Cannot save result");
        }
    }

    void saveDetectorResult(IndexRequest indexRequest, String context, Iterator<TimeValue> backoff) {
        client
            .index(
                indexRequest,
                ActionListener
                    .<IndexResponse>wrap(
                        response -> LOG.debug(SUCCESS_SAVING_MSG + context),
                        exception -> {
                            // Elasticsearch has a thread pool and a queue for write per node. A thread
                            // pool will have N number of workers ready to handle the requests. When a
                            // request comes and if a worker is free , this is handled by the worker. Now by
                            // default the number of workers is equal to the number of cores on that CPU.
                            // When the workers are full and there are more write requests, the request
                            // will go to queue. The size of queue is also limited. If by default size is,
                            // say, 200 and if there happens more parallel requests than this, then those
                            // requests would be rejected as you can see EsRejectedExecutionException.
                            // So EsRejectedExecutionException is the way that Elasticsearch tells us that
                            // it cannot keep up with the current indexing rate.
                            // When it happens, we should pause indexing a bit before trying again, ideally
                            // with randomized exponential backoff.
                            if (!(exception instanceof EsRejectedExecutionException) || !backoff.hasNext()) {
                                LOG.error(FAIL_TO_SAVE_ERR_MSG + context);
                            } else {
                                TimeValue nextDelay = backoff.next();
                                LOG.info(RETRY_SAVING_ERR_MSG + context);
                                threadPool
                                    .schedule(() -> saveDetectorResult(indexRequest, context, backoff), nextDelay, ThreadPool.Names.SAME);
                            }
                        }
                    )
            );
    }

    class RCFActionListener implements ActionListener<RCFResultResponse> {
        private List<RCFResultResponse> rcfResults;
        private String modelID;
        private AtomicReference<AnomalyDetectionException> failure;
        private String nodeID;

        RCFActionListener(
            List<RCFResultResponse> rcfResults,
            String modelID,
            AtomicReference<AnomalyDetectionException> failure,
            String nodeID
        ) {
            this.rcfResults = rcfResults;
            this.modelID = modelID;
            this.failure = failure;
            this.nodeID = nodeID;
        }

        @Override
        public void onResponse(RCFResultResponse response) {
            stateManager.resetBackpressureCounter(nodeID);
            if (response != null) {
                rcfResults.add(response);
            } else {
                LOG.warn(NULL_RESPONSE + " {} for {}", modelID, nodeID);
            }
        }

        @Override
        public void onFailure(Exception e) {
            handlePredictionFailure(e, modelID, nodeID, failure);
        }
    }

    class ThresholdActionListener implements ActionListener<ThresholdResultResponse> {
        private AtomicReference<AnomalyResultResponse> anomalyResultResponse;
        private List<FeatureData> features;
        private String modelID;
        private AtomicReference<AnomalyDetectionException> failure;
        private String nodeID;

        ThresholdActionListener(
            AtomicReference<AnomalyResultResponse> anomalyResultResponse,
            List<FeatureData> features,
            String modelID,
            AtomicReference<AnomalyDetectionException> failure,
            String nodeID
        ) {
            this.anomalyResultResponse = anomalyResultResponse;
            this.features = features;
            this.modelID = modelID;
            this.failure = failure;
            this.nodeID = nodeID;
        }

        @Override
        public void onResponse(ThresholdResultResponse response) {
            try {
                anomalyResultResponse.set(new AnomalyResultResponse(response.getAnomalyGrade(), response.getConfidence(), features));
                stateManager.resetBackpressureCounter(nodeID);
            } catch (Exception ex) {
                LOG.error("Unexpected exception", ex);
            }
        }

        @Override
        public void onFailure(Exception e) {
            handlePredictionFailure(e, modelID, nodeID, failure);
        }
    }

    private void handlePredictionFailure(Exception e, String adID, String nodeID, AtomicReference<AnomalyDetectionException> failure) {
        LOG.error(new ParameterizedMessage("Received an error from node {} when fetch anomaly grade for {}", nodeID, adID), e);
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
        return e instanceof ConnectTransportException || e instanceof NodeClosedException || e instanceof ReceiveTimeoutTransportException;
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

    class ColdStartJob implements Callable<Boolean> {

        private AnomalyDetector detector;

        ColdStartJob(AnomalyDetector detector) {
            this.detector = detector;
        }

        @Override
        public Boolean call() {
            try {
                Optional<double[][]> traingData = featureManager.getColdStartData(detector);
                if (traingData.isPresent()) {
                    modelManager.trainModel(detector, traingData.get());
                    return true;
                } else {
                    throw new EndRunException(detector.getDetectorId(), "Cannot get training data", false);
                }

            } catch (ElasticsearchTimeoutException timeoutEx) {
                throw new InternalFailure(
                    detector.getDetectorId(),
                    "Time out while indexing cold start checkpoint or get training data",
                    timeoutEx
                );
            } catch (Exception ex) {
                throw new EndRunException(detector.getDetectorId(), "Error while cold start", ex, false);
            }
        }

    }
}
