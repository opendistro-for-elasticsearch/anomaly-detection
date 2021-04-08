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

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_SECS;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;

/**
 * A queue slowly releasing low-priority requests to CheckpointReadQueue
 *
 */
public class ColdEntityQueue extends RateLimitedQueue<EntityFeatureRequest> {
    private static final Logger LOG = LogManager.getLogger(ColdEntityQueue.class);

    private int batchSize;
    private final CheckpointReadQueue checkpointReadQueue;
    private boolean scheduled;
    private int expectedExecutionTimeInSecsPerRequest;

    public ColdEntityQueue(
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
        int batchSize,
        CheckpointReadQueue checkpointReadQueue,
        Duration stateTtl,
        int expectedExecutionTimeInSecsPerRequest
    ) {
        super(
            "cold-entity",
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

        this.batchSize = batchSize;
        this.checkpointReadQueue = checkpointReadQueue;
        this.scheduled = false;

        this.expectedExecutionTimeInSecsPerRequest = expectedExecutionTimeInSecsPerRequest;
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_SECS, it -> this.expectedExecutionTimeInSecsPerRequest = it);
    }

    private int pullRequests() {
        try {
            List<EntityFeatureRequest> requests = getRequests(batchSize);
            if (requests == null || requests.isEmpty()) {
                return 0;
            }
            checkpointReadQueue.putAll(requests);
            return requests.size();
        } catch (Exception e) {
            LOG.error("Error enqueuing cold entity requests", e);
        }
        return 0;
    }

    private synchronized void schedulePulling(TimeValue delay) {
        try {
            threadPool.schedule(this::pullRequests, delay, AnomalyDetectorPlugin.AD_THREAD_POOL_NAME);
        } catch (Exception e) {
            LOG.error("Fail to schedule cold entity pulling", e);
        }
    }

    @Override
    protected void triggerProcess() {
        if (false == scheduled) {
            int requestSize = pullRequests();

            if (requestSize < batchSize) {
                scheduled = false;
            } else {
                // there might be more to fetch
                // schedule a pull from queue every few seconds. Add randomness to
                // cope with the case that we want to execute at least 1 request every
                // three seconds, but cannot guarantee that.
                int expectedSingleRequestExecutionMillis = 1000 * expectedExecutionTimeInSecsPerRequest;
                int waitMilliSeconds = requestSize * expectedSingleRequestExecutionMillis;
                schedulePulling(TimeValue.timeValueMillis(waitMilliSeconds + random.nextInt(waitMilliSeconds)));
                scheduled = true;
            }
        }
    }
}
