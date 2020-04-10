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

import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.RcfResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.time.Instant;
import java.util.Map;
import java.util.Map.Entry;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingResult;
import java.util.stream.Collectors;
import com.amazon.opendistroforelasticsearch.ad.transport.handler.AnomalyResultHandler;
import java.util.stream.IntStream;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.FeatureData;

public class EntityResultTransportAction extends HandledTransportAction<EntityResultRequest, EntityResultResponse> {

    private static final Logger LOG = LogManager.getLogger(EntityResultTransportAction.class);
    private ModelManager manager;
    private ADCircuitBreakerService adCircuitBreakerService;
    private AnomalyResultHandler anomalyResultHandler;

    @Inject
    public EntityResultTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        ModelManager manager,
        ADCircuitBreakerService adCircuitBreakerService,
        AnomalyResultHandler anomalyResultHandler
    ) {
        super(EntityResultAction.NAME, transportService, actionFilters, EntityResultRequest::new);
        this.manager = manager;
        this.adCircuitBreakerService = adCircuitBreakerService;
        this.anomalyResultHandler = anomalyResultHandler;
    }

    @Override
    protected void doExecute(Task task, EntityResultRequest request, ActionListener<EntityResultResponse> listener) {
        if (adCircuitBreakerService.isOpen()) {
            listener.onFailure(new LimitExceededException(request.getDetectorId(), CommonErrorMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG));
            return;
        }

        try {
            String detectorId = request.getDetectorId();
            for (Entry<String, double[]> entity : request.getEntities().entrySet()) {
                String modelId = detectorId + entity.getKey();
                ThresholdingResult result = manager.getAnomalyResultForEntity(detectorId, modelId, entity.getValue());
                this.anomalyResultHandler.indexAnomalyResult(new AnomalyResult(
                    detectorId,
                    0.,
                    result.getGrade(),
                    result.getConfidence(),
                    IntStream.range(0, entity.getValue().length).mapToObj(i -> new FeatureData(String.valueOf(i), String.valueOf(i), entity.getValue()[i])).collect(Collectors.toList()),
                    Instant.ofEpochMilli(request.getStart()),
                    Instant.ofEpochMilli(request.getEnd()),
                    Instant.now(),
                    Instant.now(),
                    null,
                    entity.getKey()
                ));
            }
            listener.onResponse(new EntityResultResponse());
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }

    }

}
