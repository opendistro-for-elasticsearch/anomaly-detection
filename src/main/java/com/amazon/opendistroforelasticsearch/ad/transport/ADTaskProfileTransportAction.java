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

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.model.ADTaskProfile;
import com.amazon.opendistroforelasticsearch.ad.task.ADTaskManager;

public class ADTaskProfileTransportAction extends
    TransportNodesAction<ADTaskProfileRequest, ADTaskProfileResponse, ADTaskProfileNodeRequest, ADTaskProfileNodeResponse> {

    private ADTaskManager adTaskManager;

    @Inject
    public ADTaskProfileTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ADTaskManager adTaskManager
    ) {
        super(
            ADTaskProfileAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ADTaskProfileRequest::new,
            ADTaskProfileNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            ADTaskProfileNodeResponse.class
        );
        this.adTaskManager = adTaskManager;
    }

    @Override
    protected ADTaskProfileResponse newResponse(
        ADTaskProfileRequest request,
        List<ADTaskProfileNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new ADTaskProfileResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected ADTaskProfileNodeRequest newNodeRequest(ADTaskProfileRequest request) {
        return new ADTaskProfileNodeRequest(request);
    }

    @Override
    protected ADTaskProfileNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new ADTaskProfileNodeResponse(in);
    }

    @Override
    protected ADTaskProfileNodeResponse nodeOperation(ADTaskProfileNodeRequest request) {
        ADTaskProfile adTaskProfile = adTaskManager.getLocalADTaskProfileByDetectorId(request.getDetectorId());

        return new ADTaskProfileNodeResponse(clusterService.localNode(), adTaskProfile);
    }
}
