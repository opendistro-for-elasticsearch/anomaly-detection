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

import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class ThresholdResultTransportAction extends HandledTransportAction<ThresholdResultRequest, ThresholdResultResponse> {

    private static final Logger LOG = LogManager.getLogger(ThresholdResultTransportAction.class);
    private ModelManager manager;

    @Inject
    public ThresholdResultTransportAction(ActionFilters actionFilters, TransportService transportService, ModelManager manager) {
        super(ThresholdResultAction.NAME, transportService, actionFilters, ThresholdResultRequest::new);
        this.manager = manager;
    }

    @Override
    protected void doExecute(Task task, ThresholdResultRequest request, ActionListener<ThresholdResultResponse> listener) {

        try {
            LOG.info("Serve threshold request for {}", request.getModelID());
            ThresholdingResult result = manager.getThresholdingResult(request.getAdID(), request.getModelID(), request.getRCFScore());
            listener.onResponse(new ThresholdResultResponse(result.getGrade(), result.getConfidence()));
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }
    }

}
