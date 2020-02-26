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

import java.io.IOException;
import java.util.List;

import com.amazon.opendistroforelasticsearch.ad.feature.FeatureManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class CronTransportAction extends TransportNodesAction<CronRequest, CronResponse, CronNodeRequest, CronNodeResponse> {

    private ADStateManager transportStateManager;
    private ModelManager modelManager;
    private FeatureManager featureManager;

    @Inject
    public CronTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ADStateManager tarnsportStatemanager,
        ModelManager modelManager,
        FeatureManager featureManager
    ) {
        super(
            CronAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            CronRequest::new,
            CronNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            CronNodeResponse.class
        );
        this.transportStateManager = tarnsportStatemanager;
        this.modelManager = modelManager;
        this.featureManager = featureManager;
    }

    @Override
    protected CronResponse newResponse(CronRequest request, List<CronNodeResponse> responses, List<FailedNodeException> failures) {
        return new CronResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected CronNodeRequest newNodeRequest(CronRequest request) {
        return new CronNodeRequest();
    }

    @Override
    protected CronNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new CronNodeResponse(in);
    }

    /**
     * Delete unused models and save checkpoints before deleting (including both RCF
     * and thresholding model), buffered shingle data, and transport state
     *
     * @param request delete request
     * @return delete response including local node Id.
     */
    @Override
    protected CronNodeResponse nodeOperation(CronNodeRequest request) {

        // makes checkpoints for hosted models and stop hosting models not actively
        // used.
        modelManager.maintenance();

        // delete unused buffered shingle data
        featureManager.maintenance();

        // delete unused transport state
        transportStateManager.maintenance();

        return new CronNodeResponse(clusterService.localNode());
    }
}
