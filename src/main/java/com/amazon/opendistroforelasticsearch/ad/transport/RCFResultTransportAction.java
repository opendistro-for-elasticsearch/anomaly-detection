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

public class RCFResultTransportAction extends HandledTransportAction<RCFResultRequest, RCFResultResponse> {

    private static final Logger LOG = LogManager.getLogger(RCFResultTransportAction.class);
    private ModelManager manager;
    private ADCircuitBreakerService adCircuitBreakerService;

    @Inject
    public RCFResultTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        ModelManager manager,
        ADCircuitBreakerService adCircuitBreakerService
    ) {
        super(RCFResultAction.NAME, transportService, actionFilters, RCFResultRequest::new);
        this.manager = manager;
        this.adCircuitBreakerService = adCircuitBreakerService;
    }

    @Override
    protected void doExecute(Task task, RCFResultRequest request, ActionListener<RCFResultResponse> listener) {

        if (adCircuitBreakerService.isOpen()) {
            listener.onFailure(new LimitExceededException(request.getAdID(), CommonErrorMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG));
            return;
        }

        try {
            LOG.info("Serve rcf request for {}", request.getModelID());
            RcfResult result = manager.getRcfResult(request.getAdID(), request.getModelID(), request.getFeatures());
            listener.onResponse(new RCFResultResponse(result.getScore(), result.getConfidence(), result.getForestSize()));
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }

    }

}
