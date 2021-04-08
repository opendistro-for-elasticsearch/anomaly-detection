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

package com.amazon.opendistroforelasticsearch.ad.ratelimit;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_BATCH_SIZE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.NodeStateManager;
import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.ml.CheckpointDao;
import com.amazon.opendistroforelasticsearch.ad.ml.EntityModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelState;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.ExceptionUtil;

public class CheckpointWriteQueue extends BatchQueue<CheckpointWriteRequest, BulkRequest, BulkResponse> {
    private static final Logger LOG = LogManager.getLogger(CheckpointWriteQueue.class);

    private final AnomalyDetectionIndices indexUtil;
    private final CheckpointDao checkpoint;
    private final String indexName;
    private final Duration checkpointInterval;
    private final NodeStateManager stateManager;

    public CheckpointWriteQueue(
        long heapSizeInBytes,
        int singleRequestSizeInBytes,
        Setting<Float> maxHeapPercentForQueueSetting,
        ClusterService clusterService,
        Random random,
        ADCircuitBreakerService adCircuitBreakerService,
        ThreadPool threadPool,
        Settings settings,
        float maxQueuedTaskRatio,
        Clock clock,
        float mediumSegmentPruneRatio,
        float lowSegmentPruneRatio,
        int maintenanceFreqConstant,
        ClientUtil clientUtil,
        Duration executionTtl,
        AnomalyDetectionIndices indexUtil,
        CheckpointDao checkpoint,
        String indexName,
        Duration checkpointInterval,
        NodeStateManager stateManager,
        Duration stateTtl
    ) {
        super(
            "checkpoint-write",
            heapSizeInBytes,
            singleRequestSizeInBytes,
            maxHeapPercentForQueueSetting,
            clusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            settings,
            maxQueuedTaskRatio,
            clock,
            mediumSegmentPruneRatio,
            lowSegmentPruneRatio,
            maintenanceFreqConstant,
            clientUtil,
            CHECKPOINT_WRITE_QUEUE_CONCURRENCY,
            executionTtl,
            CHECKPOINT_WRITE_QUEUE_BATCH_SIZE,
            stateTtl
        );
        this.indexUtil = indexUtil;
        this.checkpoint = checkpoint;
        this.indexName = indexName;
        this.checkpointInterval = checkpointInterval;
        this.stateManager = stateManager;
    }

    @Override
    protected void executeBatchRequest(BulkRequest request, ActionListener<BulkResponse> listener) {
        if (indexUtil.doesCheckpointIndexExist()) {
            clientUtil.<BulkRequest, BulkResponse>execute(BulkAction.INSTANCE, request, listener);
        } else {
            indexUtil.initCheckpointIndex(ActionListener.wrap(initResponse -> {
                if (initResponse.isAcknowledged()) {
                    clientUtil.<BulkRequest, BulkResponse>execute(BulkAction.INSTANCE, request, listener);
                } else {
                    throw new RuntimeException("Creating checkpoint with mappings call not acknowledged.");
                }
            }, exception -> {
                if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                    // It is possible the index has been created while we sending the create request
                    clientUtil.<BulkRequest, BulkResponse>execute(BulkAction.INSTANCE, request, listener);
                } else {
                    LOG.error(String.format(Locale.ROOT, "Unexpected error creating checkpoint index"), exception);
                }
            }));
        }
    }

    @Override
    protected BulkRequest toBatchRequest(List<CheckpointWriteRequest> toProcess) {
        final BulkRequest bulkRequest = new BulkRequest();
        for (CheckpointWriteRequest request : toProcess) {

            bulkRequest.add(request.getIndexRequest());

            // retrying request
            bulkRequest.add(request.getIndexRequest());
        }
        return bulkRequest;
    }

    @Override
    protected ActionListener<BulkResponse> getResponseListener(List<CheckpointWriteRequest> toProcess, BulkRequest batchRequest) {
        return ActionListener
            .wrap(
                response -> {
                    // don't retry failed requests since checkpoints are too large (250KB+)
                    // Later maintenance window or cold start will retry saving
                    LOG.debug("Finished writing checkpoints");
                },
                exception -> {
                    if (ExceptionUtil.isOverloaded(exception)) {
                        LOG.error("too many get AD model checkpoint requests or shard not avialble");
                        setCoolDownStart();
                    }

                    // don't retry failed requests since checkpoints are too large (250KB+)
                    // Later maintenance window or cold start will retry saving
                    LOG.error("Fail to save models", exception);
                }
            );
    }

    /**
     * Prepare bulking the input model state to the checkpoint index.
     * We don't save checkpoints within checkpointInterval again, except this
     * is from cold start. This method will update the input state's last
     *  checkpoint time if the checkpoint is staged (ready to be written in the
     *  next batch).
     * @param modelState Model state
     * @param forceWrite whether we should write no matter what
     */
    public void write(ModelState<EntityModel> modelState, boolean forceWrite, SegmentPriority priority) {
        Instant instant = modelState.getLastCheckpointTime();
        if ((instant == Instant.MIN || instant.plus(checkpointInterval).isAfter(clock.instant())) && !forceWrite) {
            return;
        }
        // It is possible 2 states of the same model id gets saved: one overwrite another.
        // This can happen if previous checkpoint hasn't been saved to disk, while the
        // 1st one creates a new state without restoring.
        if (modelState.getModel() != null) {
            try {
                String detectorId = modelState.getDetectorId();
                String modelId = modelState.getModelId();
                if (modelId == null || detectorId == null) {
                    return;
                }

                stateManager.getAnomalyDetector(detectorId, onGetDetector(detectorId, modelId, modelState, priority));
            } catch (ConcurrentModificationException e) {
                LOG.info(new ParameterizedMessage("Concurrent modification while serializing models for [{}]", modelState), e);
            }
        }
    }

    private ActionListener<Optional<AnomalyDetector>> onGetDetector(
        String detectorId,
        String modelId,
        ModelState<EntityModel> modelState,
        SegmentPriority priority
    ) {
        return ActionListener.wrap(detectorOptional -> {
            if (false == detectorOptional.isPresent()) {
                LOG.warn(new ParameterizedMessage("AnomalyDetector [{}] is not available.", detectorId));
                return;
            }

            AnomalyDetector detector = detectorOptional.get();
            try {
                Map<String, Object> source = checkpoint.toIndexSource(modelState);

                // the model state is bloated or we have bugs, skip
                if (source == null || source.isEmpty()) {
                    return;
                }

                CheckpointWriteRequest request = new CheckpointWriteRequest(
                    System.currentTimeMillis() + detector.getDetectorIntervalInMilliseconds(),
                    detectorId,
                    priority,
                    new IndexRequest(indexName).id(modelId).source(source)
                );

                put(request);
            } catch (Exception e) {
                // Example exception:
                // ConcurrentModificationException when calling toCheckpoint
                // and updating rcf model at the same time. To prevent this,
                // we need to have a deep copy of models or have a lock. Both
                // options are costly.
                // As we are gonna retry serializing either when the entity is
                // evicted out of cache or during the next maintenance period,
                // don't do anything when the exception happens.
                LOG.info(new ParameterizedMessage("Exception while serializing models for [{}]", modelId), e);
            }

        }, exception -> { LOG.error(new ParameterizedMessage("fail to get detector [{}]", detectorId), exception); });
    }

    public void writeAll(List<ModelState<EntityModel>> modelStates, String detectorId, boolean forceWrite, SegmentPriority priority) {
        ActionListener<Optional<AnomalyDetector>> onGetForAll = ActionListener.wrap(detectorOptional -> {
            if (false == detectorOptional.isPresent()) {
                LOG.warn(new ParameterizedMessage("AnomalyDetector [{}] is not available.", detectorId));
                return;
            }

            AnomalyDetector detector = detectorOptional.get();
            try {
                List<CheckpointWriteRequest> allRequests = new ArrayList<>();
                for (ModelState<EntityModel> state : modelStates) {
                    Instant instant = state.getLastCheckpointTime();
                    if ((instant == Instant.MIN || instant.plus(checkpointInterval).isAfter(clock.instant())) && !forceWrite) {
                        continue;
                    }

                    Map<String, Object> source = checkpoint.toIndexSource(state);
                    String modelId = state.getModelId();

                    // the model state is bloated, skip
                    if (source == null || source.isEmpty() || Strings.isEmpty(modelId)) {
                        continue;
                    }

                    allRequests
                        .add(
                            new CheckpointWriteRequest(
                                System.currentTimeMillis() + detector.getDetectorIntervalInMilliseconds(),
                                detectorId,
                                priority,
                                new IndexRequest(indexName).id(modelId).source(source)
                            )
                        );
                }

                putAll(allRequests);
            } catch (Exception e) {
                // Example exception:
                // ConcurrentModificationException when calling toCheckpoint
                // and updating rcf model at the same time. To prevent this,
                // we need to have a deep copy of models or have a lock. Both
                // options are costly.
                // As we are gonna retry serializing either when the entity is
                // evicted out of cache or during the next maintenance period,
                // don't do anything when the exception happens.
                LOG.info(new ParameterizedMessage("Exception while serializing models for [{}]", detectorId), e);
            }

        }, exception -> { LOG.error(new ParameterizedMessage("fail to get detector [{}]", detectorId), exception); });

        stateManager.getAnomalyDetector(detectorId, onGetForAll);
    }
}
