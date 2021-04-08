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
import java.time.Instant;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;

/**
 * HCAD can bombard ES with “thundering herd” traffic, in which many entities
 * make requests that need similar ES reads/writes at approximately the same
 * time. To remedy this issue we queue the requests and ensure that only a
 * limited set of requests are out for ES reads/writes.
 *
 * @param <RequestType> Individual request type that is a subtype of ADRequest
 */
public abstract class ConcurrentQueue<RequestType extends QueuedRequest> extends RateLimitedQueue<RequestType> {
    private static final Logger LOG = LogManager.getLogger(ConcurrentQueue.class);

    private Semaphore permits;

    protected final ClientUtil clientUtil;

    private Instant lastExecuteTime;
    private Duration executionTtl;

    /**
     *
     * Constructor with dependencies and configuration.
     *
     * @param queueName queue's name
     * @param heapSizeInBytes ES heap size
     * @param singleRequestSizeInBytes single request's size in bytes
     * @param maxHeapPercentForQueueSetting max heap size used for the queue. Used for
     *   rate AD's usage on ES threadpools.
     * @param clusterService Cluster service accessor
     * @param random Random number generator
     * @param adCircuitBreakerService AD Circuit breaker service
     * @param threadPool threadpool accessor
     * @param settings Cluster settings getter
     * @param maxQueuedTaskRatio maximum queued tasks ratio in ES threadpools
     * @param clock Clock to get current time
     * @param clientUtil utility with ES client
     * @param concurrencySetting Max concurrent processing of the queued events
     * @param executionTtl Max execution time of a single request
     * @param stateTtl max idle state duration.  Used to clean unused states.
     */
    public ConcurrentQueue(
        String queueName,
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
        Setting<Integer> concurrencySetting,
        Duration executionTtl,
        Duration stateTtl
    ) {
        super(
            queueName,
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
            stateTtl
        );

        this.permits = new Semaphore(concurrencySetting.get(settings));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(concurrencySetting, it -> permits = new Semaphore(it));
        this.clientUtil = clientUtil;

        this.lastExecuteTime = clock.instant();
        this.executionTtl = executionTtl;
    }

    @Override
    public void maintenance() {
        super.maintenance();

        if (lastExecuteTime.plus(executionTtl).isBefore(clock.instant())) {
            LOG.warn("previous execution has been running for too long.  Maybe there are bugs.");

            // release one permit
            permits.release();
        }
    }

    @Override
    protected void triggerProcess() {
        threadPool.executor(AnomalyDetectorPlugin.AD_THREAD_POOL_NAME).execute(() -> {
            if (permits.tryAcquire()) {
                try {
                    lastExecuteTime = clock.instant();
                    execute(() -> {
                        permits.release();
                        process();
                    }, () -> { permits.release(); });
                } catch (Exception e) {
                    LOG.error(String.format(Locale.ROOT, "Failed to process requests from %s", getQueueName()), e);
                    permits.release();
                }
            }
        });
    }

    /**
     * Execute requests in toProcess.  The implementation needs to call cleanUp after done.
     * @param afterProcessCallback callback after processing requests
     * @param emptyQueueCallback callback for empty queues
     */
    protected abstract void execute(Runnable afterProcessCallback, Runnable emptyQueueCallback);
}
