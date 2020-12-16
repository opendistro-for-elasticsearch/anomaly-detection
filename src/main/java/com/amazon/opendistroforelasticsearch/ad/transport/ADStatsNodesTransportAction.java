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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.monitor.jvm.JvmService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.stats.ADStats;
import com.amazon.opendistroforelasticsearch.ad.stats.StatNames;

/**
 *  ADStatsNodesTransportAction contains the logic to extract the stats from the nodes
 */
public class ADStatsNodesTransportAction extends
    TransportNodesAction<ADStatsRequest, ADStatsNodesResponse, ADStatsNodeRequest, ADStatsNodeResponse> {

    private ADStats adStats;
    private final JvmService jvmService;

    /**
     * Constructor
     *
     * @param threadPool ThreadPool to use
     * @param clusterService ClusterService
     * @param transportService TransportService
     * @param actionFilters Action Filters
     * @param adStats ADStats object
     */
    @Inject
    public ADStatsNodesTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ADStats adStats,
        JvmService jvmService
    ) {
        super(
            ADStatsNodesAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ADStatsRequest::new,
            ADStatsNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            ADStatsNodeResponse.class
        );
        this.adStats = adStats;
        this.jvmService = jvmService;
    }

    @Override
    protected ADStatsNodesResponse newResponse(
        ADStatsRequest request,
        List<ADStatsNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new ADStatsNodesResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected ADStatsNodeRequest newNodeRequest(ADStatsRequest request) {
        return new ADStatsNodeRequest(request);
    }

    @Override
    protected ADStatsNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new ADStatsNodeResponse(in);
    }

    @Override
    protected ADStatsNodeResponse nodeOperation(ADStatsNodeRequest request) {
        return createADStatsNodeResponse(request.getADStatsRequest());
    }

    private ADStatsNodeResponse createADStatsNodeResponse(ADStatsRequest adStatsRequest) {
        Map<String, Object> statValues = new HashMap<>();
        Set<String> statsToBeRetrieved = adStatsRequest.getStatsToBeRetrieved();

        if (statsToBeRetrieved.contains(StatNames.JVM_HEAP_USAGE.getName())) {
            long heapUsedPercent = jvmService.stats().getMem().getHeapUsedPercent();
            adStats.getStat(StatNames.JVM_HEAP_USAGE.getName()).setValue(heapUsedPercent);
            statValues.put(StatNames.JVM_HEAP_USAGE.getName(), heapUsedPercent);
        }

        for (String statName : adStats.getNodeStats().keySet()) {
            if (statsToBeRetrieved.contains(statName)) {
                statValues.put(statName, adStats.getStats().get(statName).getValue());
            }
        }

        return new ADStatsNodeResponse(clusterService.localNode(), statValues);
    }
}
