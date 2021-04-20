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

package com.amazon.opendistroforelasticsearch.ad.cluster;

import java.time.Clock;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.cluster.diskcleanup.IndexCleanup;
import com.amazon.opendistroforelasticsearch.ad.cluster.diskcleanup.ModelCheckpointIndexRetention;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.task.ADTaskManager;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;
import com.google.common.annotations.VisibleForTesting;

public class MasterEventListener implements LocalNodeMasterListener {

    private Cancellable checkpointIndexRetentionCron;
    private Cancellable hourlyCron;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private Client client;
    private Clock clock;
    private ClientUtil clientUtil;
    private DiscoveryNodeFilterer nodeFilter;
    private ADTaskManager adTaskManager;

    public MasterEventListener(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        Clock clock,
        ClientUtil clientUtil,
        DiscoveryNodeFilterer nodeFilter,
        ADTaskManager adTaskManager
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.client = client;
        this.clusterService.addLocalNodeMasterListener(this);
        this.clock = clock;
        this.clientUtil = clientUtil;
        this.nodeFilter = nodeFilter;
        this.adTaskManager = adTaskManager;
    }

    @Override
    public void onMaster() {
        if (hourlyCron == null) {
            hourlyCron = threadPool
                .scheduleWithFixedDelay(new HourlyCron(client, nodeFilter, adTaskManager), TimeValue.timeValueHours(1), executorName());
            clusterService.addLifecycleListener(new LifecycleListener() {
                @Override
                public void beforeStop() {
                    cancel(hourlyCron);
                    hourlyCron = null;
                }
            });
        }

        if (checkpointIndexRetentionCron == null) {
            IndexCleanup indexCleanup = new IndexCleanup(client, clientUtil, clusterService);
            checkpointIndexRetentionCron = threadPool
                .scheduleWithFixedDelay(
                    new ModelCheckpointIndexRetention(AnomalyDetectorSettings.CHECKPOINT_TTL, clock, indexCleanup),
                    TimeValue.timeValueHours(24),
                    executorName()
                );
            clusterService.addLifecycleListener(new LifecycleListener() {
                @Override
                public void beforeStop() {
                    cancel(checkpointIndexRetentionCron);
                    checkpointIndexRetentionCron = null;
                }
            });
        }
    }

    @Override
    public void offMaster() {
        cancel(hourlyCron);
        cancel(checkpointIndexRetentionCron);
        hourlyCron = null;
        checkpointIndexRetentionCron = null;
    }

    private void cancel(Cancellable cron) {
        if (cron != null) {
            cron.cancel();
        }
    }

    @VisibleForTesting
    protected Cancellable getCheckpointIndexRetentionCron() {
        return checkpointIndexRetentionCron;
    }

    protected Cancellable getHourlyCron() {
        return hourlyCron;
    }

    private String executorName() {
        return ThreadPool.Names.GENERIC;
    }
}
