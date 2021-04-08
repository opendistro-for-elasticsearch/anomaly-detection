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
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;

public abstract class SingleRequestQueue<RequestType extends QueuedRequest> extends ConcurrentQueue<RequestType> {

    public SingleRequestQueue(
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
    }

    private static final Logger LOG = LogManager.getLogger(RateLimitedQueue.class);

    @Override
    protected void execute(Runnable afterProcessCallback, Runnable emptyQueueCallback) {
        RequestType request = null;

        try {
            Optional<BlockingQueue<RequestType>> queueOptional = selectNextQueue();
            if (false == queueOptional.isPresent()) {
                // no queue has requests
                emptyQueueCallback.run();
                return;
            }

            BlockingQueue<RequestType> queue = queueOptional.get();
            if (false == queue.isEmpty()) {
                request = queue.poll();
            }

            if (request == null) {
                emptyQueueCallback.run();
                return;
            }

            final ActionListener<Void> handlerWithRelease = ActionListener.wrap(afterProcessCallback);
            executeRequest(request, handlerWithRelease);
        } catch (Exception e) {
            LOG.error("Fail to execute", e);
            emptyQueueCallback.run();
        }
    }

    /**
     * Used by subclasses to creates customized logic to send batch requests.
     * After everything finishes, the method should call listener.
     * @param request request to execute
     * @param listener customized listener
     */
    protected abstract void executeRequest(RequestType request, ActionListener<Void> listener);
}
