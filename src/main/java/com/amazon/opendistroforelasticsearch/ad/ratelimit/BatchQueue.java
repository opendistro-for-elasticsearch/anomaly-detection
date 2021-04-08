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

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;

/**
 *
 * @param <RequestType> Individual request type that is a subtype of ADRequest
 * @param <BatchRequestType> Batch request type like BulkRequest
 * @param <BatchResponseType> Response type like BulkResponse
 */
public abstract class BatchQueue<RequestType extends QueuedRequest, BatchRequestType, BatchResponseType> extends
    ConcurrentQueue<RequestType> {
    private static final Logger LOG = LogManager.getLogger(BatchQueue.class);
    protected int batchSize;

    public BatchQueue(
        String queueName,
        long heapSize,
        int singleRequestSize,
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
        Setting<Integer> concurrencySetting,
        Duration executionTtl,
        Setting<Integer> batchSizeSetting,
        Duration stateTtl
    ) {
        super(
            queueName,
            heapSize,
            singleRequestSize,
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
            concurrencySetting,
            executionTtl,
            stateTtl
        );
        this.batchSize = batchSizeSetting.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(batchSizeSetting, it -> batchSize = it);
    }

    /**
     * Used by subclasses to creates customized logic to send batch requests.
     * After everything finishes, the method should call listener.
     * @param request Batch request to execute
     * @param listener customized listener
     */
    protected abstract void executeBatchRequest(BatchRequestType request, ActionListener<BatchResponseType> listener);

    /**
     * We convert from queued requests understood by AD to batchRequest understood by ES.
     * @param toProcess Queued requests
     * @return batch requests
     */
    protected abstract BatchRequestType toBatchRequest(List<RequestType> toProcess);

    @Override
    protected void execute(Runnable afterProcessCallback, Runnable emptyQueueCallback) {

        List<RequestType> toProcess = getRequests(batchSize);

        // it is possible other concurrent threads have drained the queue
        try {
            if (false == toProcess.isEmpty()) {
                BatchRequestType batchRequest = toBatchRequest(toProcess);

                ThreadedActionListener<BatchResponseType> listener = new ThreadedActionListener<>(
                    LOG,
                    threadPool,
                    AnomalyDetectorPlugin.AD_THREAD_POOL_NAME,
                    getResponseListener(toProcess, batchRequest),
                    false
                );

                final ActionListener<BatchResponseType> listenerWithRelease = ActionListener.runAfter(listener, afterProcessCallback);
                executeBatchRequest(batchRequest, listenerWithRelease);
            } else {
                emptyQueueCallback.run();
            }
        } catch (Exception e) {
            LOG.error(new ParameterizedMessage("Fail to execute requests in [{}]", this.queueName), e);
            emptyQueueCallback.run();
        }
    }

    /**
     * Used by subclasses to creates customized logic to handle batch responses
     * or errors.
     * @param toProcess Queued request used to retrieve information of retrying requests
     * @param batchRequest Batch request corresponding to toProcess. We convert
     *  from toProcess understood by AD to batchRequest understood by ES.
     * @return Listener to BatchResponse
     */
    protected abstract ActionListener<BatchResponseType> getResponseListener(List<RequestType> toProcess, BatchRequestType batchRequest);
}
