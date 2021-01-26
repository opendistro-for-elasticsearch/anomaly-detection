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

package com.amazon.opendistroforelasticsearch.ad.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.task.ADBatchTaskRunner;

public class ADBatchTaskRemoteExecutionTransportAction extends
    HandledTransportAction<ADBatchAnomalyResultRequest, ADBatchAnomalyResultResponse> {

    private final ADBatchTaskRunner adBatchTaskRunner;
    private final TransportService transportService;

    @Inject
    public ADBatchTaskRemoteExecutionTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        ADBatchTaskRunner adBatchTaskRunner
    ) {
        super(ADBatchTaskRemoteExecutionAction.NAME, transportService, actionFilters, ADBatchAnomalyResultRequest::new);
        this.adBatchTaskRunner = adBatchTaskRunner;
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(Task task, ADBatchAnomalyResultRequest request, ActionListener<ADBatchAnomalyResultResponse> listener) {
        adBatchTaskRunner.startADBatchTask(request.getAdTask(), true, transportService, listener);
    }
}
