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

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.COLD_ENTITY_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.ml.EntityColdStarter;
import com.amazon.opendistroforelasticsearch.ad.ml.EntityModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager.ModelType;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelState;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.ExceptionUtil;

public class EntityColdStartQueue extends SingleRequestQueue<EntityRequest> {
    private static final Logger LOG = LogManager.getLogger(EntityColdStartQueue.class);

    private final EntityColdStarter entityColdStarter;

    public EntityColdStartQueue(
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
        EntityColdStarter entityColdStarter,
        Duration stateTtl
    ) {
        super(
            "cold-start",
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
            COLD_ENTITY_QUEUE_CONCURRENCY,
            executionTtl,
            stateTtl
        );
        this.entityColdStarter = entityColdStarter;
    }

    @Override
    protected void executeRequest(EntityRequest coldStartRequest, ActionListener<Void> listener) {
        String detectorId = coldStartRequest.getDetectorId();
        String modelId = coldStartRequest.getModelId();

        ModelState<EntityModel> modelState = new ModelState<>(
            new EntityModel(modelId, new ArrayDeque<>(), null, null),
            modelId,
            detectorId,
            ModelType.ENTITY.getName(),
            clock,
            0
        );

        EntityModel model = modelState.getModel();

        if (model == null) {
            model = new EntityModel(coldStartRequest.getModelId(), new ArrayDeque<>(), null, null);
            modelState.setModel(model);
        }

        if (model.getRcf() == null || model.getThreshold() == null) {
            // put models inside modelState if this is an active entity
            ActionListener<Void> failureListener = ActionListener.delegateResponse(listener, (delegateListener, e) -> {
                if (ExceptionUtil.isOverloaded(e)) {
                    LOG.error("ES is overloaded");
                    setCoolDownStart();
                }
                delegateListener.onFailure(e);
            });

            entityColdStarter.trainModel(coldStartRequest.getEntityName(), detectorId, modelState, failureListener);
        }
    }
}
