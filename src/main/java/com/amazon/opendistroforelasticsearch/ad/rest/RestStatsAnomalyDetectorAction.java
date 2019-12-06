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

package com.amazon.opendistroforelasticsearch.ad.rest;

import com.amazon.opendistroforelasticsearch.ad.transport.ADStatsAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADStatsRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import static com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin.AD_BASE_URI;

/**
 * RestStatsAnomalyDetectorAction consists of the REST handler to get the metrics from the anomaly detector plugin.
 */
public class RestStatsAnomalyDetectorAction extends BaseRestHandler {

    private static final String STATS_ANOMALY_DETECTOR_ACTION = "stats_anomaly_detector";

    /**
     * Constructor
     *
     * @param settings  Settings
     * @param controller Rest Controller
     */
    public RestStatsAnomalyDetectorAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.GET, AD_BASE_URI + "/{nodeId}/stats/",this);
        controller.registerHandler(RestRequest.Method.GET, AD_BASE_URI + "/{nodeId}/stats/{stat}",this);
        controller.registerHandler(RestRequest.Method.GET, AD_BASE_URI + "/stats/",this);
        controller.registerHandler(RestRequest.Method.GET, AD_BASE_URI + "/stats/{stat}",this);
    }

    @Override
    public String getName() {
        return STATS_ANOMALY_DETECTOR_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        ADStatsRequest adStatsRequest = getRequest(request);
        return channel -> client.execute(ADStatsAction.INSTANCE, adStatsRequest,
                new RestActions.NodesResponseRestListener<>(channel));
    }

    /**
     * Creates a ADStatsRequest from a RestRequest
     * @param request RestRequest
     * @return ADStatsRequest
     */
    private ADStatsRequest getRequest(RestRequest request) {
        // parse the nodes the user wants to query the stats for
        String[] nodeIdsArr = null;
        String nodesIdsStr = request.param("nodeId");
        if (!Strings.isEmpty(nodesIdsStr)) {
            nodeIdsArr = nodesIdsStr.split(",");
        }

        ADStatsRequest adStatsRequest = new ADStatsRequest(nodeIdsArr);
        adStatsRequest.timeout(request.param("timeout"));

        // parse the stats the user wants to see
        HashSet<String> statsSet = null;
        String statsStr = request.param("stat");
        if (!Strings.isEmpty(statsStr)) {
            statsSet = new HashSet<>(Arrays.asList(statsStr.split(",")));
        }

        if (statsSet == null) {
            adStatsRequest.all(); // retrieve all stats if none are specified
        } else if (statsSet.size() == 1 && statsSet.contains(ADStatsRequest.ALL_STATS_KEY)) {
            adStatsRequest.all();
        } else if (statsSet.contains(ADStatsRequest.ALL_STATS_KEY)) {
            throw new IllegalArgumentException("Request " + request.path() + " contains " + ADStatsRequest.ALL_STATS_KEY
                    + " and individual stats");
        } else {
            Set<String> invalidStats = new TreeSet<>();

            for (String stat : statsSet) {
                if (!adStatsRequest.addStat(stat)) {
                    invalidStats.add(stat);
                }
            }

            if (!invalidStats.isEmpty()) {
                throw new IllegalArgumentException(unrecognized(request, invalidStats,
                        adStatsRequest.getStatsRetrievalMap().keySet(), "stat"));
            }
        }
        return adStatsRequest;
    }
}
