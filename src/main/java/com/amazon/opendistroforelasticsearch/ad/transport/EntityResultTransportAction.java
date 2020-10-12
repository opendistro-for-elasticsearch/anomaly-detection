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

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.COOLDOWN_MINUTES;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.NodeStateManager;
import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.caching.CacheProvider;
import com.amazon.opendistroforelasticsearch.ad.caching.EntityCache;
import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.ml.CheckpointDao;
import com.amazon.opendistroforelasticsearch.ad.ml.EntityModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelState;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingResult;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.Entity;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.transport.handler.MultitiEntityResultHandler;
import com.amazon.opendistroforelasticsearch.ad.util.ParseUtils;

public class EntityResultTransportAction extends HandledTransportAction<EntityResultRequest, AcknowledgedResponse> {

    private static final Logger LOG = LogManager.getLogger(EntityResultTransportAction.class);
    private ModelManager manager;
    private ADCircuitBreakerService adCircuitBreakerService;
    private MultitiEntityResultHandler anomalyResultHandler;
    private CheckpointDao checkpointDao;
    private EntityCache cache;
    private final NodeStateManager stateManager;
    private final int coolDownMinutes;
    private final Clock clock;

    @Inject
    public EntityResultTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        ModelManager manager,
        ADCircuitBreakerService adCircuitBreakerService,
        MultitiEntityResultHandler anomalyResultHandler,
        CheckpointDao checkpointDao,
        CacheProvider entityCache,
        NodeStateManager stateManager,
        Settings settings,
        Clock clock
    ) {
        super(EntityResultAction.NAME, transportService, actionFilters, EntityResultRequest::new);
        this.manager = manager;
        this.adCircuitBreakerService = adCircuitBreakerService;
        this.anomalyResultHandler = anomalyResultHandler;
        this.checkpointDao = checkpointDao;
        this.cache = entityCache;
        this.stateManager = stateManager;
        this.coolDownMinutes = (int) (COOLDOWN_MINUTES.get(settings).getMinutes());
        this.clock = clock;
    }

    @Override
    protected void doExecute(Task task, EntityResultRequest request, ActionListener<AcknowledgedResponse> listener) {
        if (adCircuitBreakerService.isOpen()) {
            listener.onFailure(new LimitExceededException(request.getDetectorId(), CommonErrorMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG));
            return;
        }

        try {
            String detectorId = request.getDetectorId();
            stateManager.getAnomalyDetector(detectorId, onGetDetector(listener, detectorId, request));
        } catch (Exception exception) {
            LOG.error("fail to get entity's anomaly grade", exception);
            listener.onFailure(exception);
        }

    }

    private ActionListener<Optional<AnomalyDetector>> onGetDetector(
        ActionListener<AcknowledgedResponse> listener,
        String detectorId,
        EntityResultRequest request
    ) {
        return ActionListener.wrap(detectorOptional -> {
            if (!detectorOptional.isPresent()) {
                listener.onFailure(new EndRunException(detectorId, "AnomalyDetector is not available.", true));
                return;
            }

            AnomalyDetector detector = detectorOptional.get();
            // we only support 1 categorical field now
            String categoricalField = detector.getCategoryField().get(0);

            ADResultBulkRequest currentBulkRequest = new ADResultBulkRequest();
            // index pressure is high. Only save anomalies
            boolean onlySaveAnomalies = stateManager
                .getLastIndexThrottledTime()
                .plus(Duration.ofMinutes(coolDownMinutes))
                .isAfter(clock.instant());

            for (Entry<String, double[]> entity : request.getEntities().entrySet()) {
                String entityName = entity.getKey();
                // For ES, the limit of the document ID is 512 bytes.
                // truncate to 256 characters if too long since we are using it as part of document id.
                if (entityName.length() > AnomalyDetectorSettings.MAX_ENTITY_LENGTH) {
                    continue;
                }

                double[] datapoint = entity.getValue();
                String modelId = manager.getEntityModelId(detectorId, entityName);
                ModelState<EntityModel> entityModel = cache.get(modelId, detector, datapoint, entityName);
                if (entityModel == null) {
                    // cache miss
                    continue;
                }
                ThresholdingResult result = manager.getAnomalyResultForEntity(detectorId, datapoint, entityName, entityModel, modelId);
                // result.getRcfScore() = 0 means the model is not initialized
                // result.getGrade() = 0 means it is not an anomaly
                // So many EsRejectedExecutionException if we write no matter what
                if (result.getRcfScore() > 0 && (!onlySaveAnomalies || result.getGrade() > 0)) {
                    this.anomalyResultHandler
                        .write(
                            new AnomalyResult(
                                detectorId,
                                result.getRcfScore(),
                                result.getGrade(),
                                result.getConfidence(),
                                ParseUtils.getFeatureData(datapoint, detector),
                                Instant.ofEpochMilli(request.getStart()),
                                Instant.ofEpochMilli(request.getEnd()),
                                Instant.now(),
                                Instant.now(),
                                null,
                                Arrays.asList(new Entity(categoricalField, entityName))
                            ),
                            currentBulkRequest
                        );
                }
            }
            this.anomalyResultHandler.flush(currentBulkRequest, detectorId);
            // bulk all accumulated checkpoint requests
            this.checkpointDao.flush();

            listener.onResponse(new AcknowledgedResponse(true));
        }, exception -> {
            LOG.error("fail to get entity's anomaly grade", exception);
            listener.onFailure(exception);
        });
    }
}
