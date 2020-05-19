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

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.stats.ADStats;
import com.amazon.opendistroforelasticsearch.ad.stats.ADStatsResponse;
import com.amazon.opendistroforelasticsearch.ad.stats.StatNames;
import com.amazon.opendistroforelasticsearch.ad.transport.ADStatsNodesAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADStatsRequest;
import com.amazon.opendistroforelasticsearch.ad.util.MultiResponsesDelegateActionListener;
import com.google.common.collect.ImmutableList;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin.AD_BASE_URI;

/**
 * RestStatsAnomalyDetectorAction consists of the REST handler to get the stats from the anomaly detector plugin.
 */
public class RestStatsAnomalyDetectorAction extends BaseRestHandler {

    private static final String STATS_ANOMALY_DETECTOR_ACTION = "stats_anomaly_detector";
    private ADStats adStats;
    private ClusterService clusterService;

    /**
     * Constructor
     *
     * @param controller Rest Controller
     * @param clusterService ClusterService
     * @param adStats    ADStats object
     */
    public RestStatsAnomalyDetectorAction(RestController controller, ClusterService clusterService, ADStats adStats) {
        this.clusterService = clusterService;
        this.adStats = adStats;
    }

    @Override
    public String getName() {
        return STATS_ANOMALY_DETECTOR_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        ADStatsRequest adStatsRequest = getRequest(request);
        return channel -> getStats(client, channel, adStatsRequest);
    }

    /**
     * Creates a ADStatsRequest from a RestRequest
     *
     * @param request RestRequest
     * @return ADStatsRequest Request containing stats to be retrieved
     */
    private ADStatsRequest getRequest(RestRequest request) {
        // parse the nodes the user wants to query the stats for
        String[] nodeIdsArr = null;
        String nodesIdsStr = request.param("nodeId");
        Set<String> validStats = adStats.getStats().keySet();

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
            adStatsRequest.addAll(validStats); // retrieve all stats if none are specified
        } else if (statsSet.size() == 1 && statsSet.contains(ADStatsRequest.ALL_STATS_KEY)) {
            adStatsRequest.addAll(validStats);
        } else if (statsSet.contains(ADStatsRequest.ALL_STATS_KEY)) {
            throw new IllegalArgumentException(
                "Request " + request.path() + " contains " + ADStatsRequest.ALL_STATS_KEY + " and individual stats"
            );
        } else {
            Set<String> invalidStats = new TreeSet<>();
            for (String stat : statsSet) {
                if (validStats.contains(stat)) {
                    adStatsRequest.addStat(stat);
                } else {
                    invalidStats.add(stat);
                }
            }

            if (!invalidStats.isEmpty()) {
                throw new IllegalArgumentException(unrecognized(request, invalidStats, adStatsRequest.getStatsToBeRetrieved(), "stat"));
            }
        }
        return adStatsRequest;
    }

    /**
     * Make the 2 requests to get the node and cluster statistics
     *
     * @param client Client
     * @param channel Channel to send response
     * @param adStatsRequest Request containing stats to be retrieved
     */
    private void getStats(Client client, RestChannel channel, ADStatsRequest adStatsRequest) {
        // Use MultiResponsesDelegateActionListener to execute 2 async requests and create the response once they finish
        MultiResponsesDelegateActionListener<ADStatsResponse> delegateListener = new MultiResponsesDelegateActionListener<>(
            getRestStatsListener(channel),
            2,
            "Unable to return AD Stats"
        );

        getClusterStats(client, delegateListener, adStatsRequest);
        getNodeStats(client, delegateListener, adStatsRequest);
    }

    /**
     * Make async request to get the number of detectors in AnomalyDetector.ANOMALY_DETECTORS_INDEX if necessary
     * and, onResponse, gather the cluster statistics
     *
     * @param client Client
     * @param listener MultiResponsesDelegateActionListener to be used once both requests complete
     * @param adStatsRequest Request containing stats to be retrieved
     */
    private void getClusterStats(
        Client client,
        MultiResponsesDelegateActionListener<ADStatsResponse> listener,
        ADStatsRequest adStatsRequest
    ) {
        ADStatsResponse adStatsResponse = new ADStatsResponse();
        if (adStatsRequest.getStatsToBeRetrieved().contains(StatNames.DETECTOR_COUNT.getName())) {
            if (clusterService.state().getRoutingTable().hasIndex(AnomalyDetector.ANOMALY_DETECTORS_INDEX)) {
                IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest().docs(true);
                client.execute(IndicesStatsAction.INSTANCE, indicesStatsRequest, ActionListener.wrap(indicesStatsResponse -> {
                    adStats
                        .getStat(StatNames.DETECTOR_COUNT.getName())
                        .setValue(indicesStatsResponse.getIndex(AnomalyDetector.ANOMALY_DETECTORS_INDEX).getPrimaries().docs.getCount());
                    adStatsResponse.setClusterStats(getClusterStatsMap(adStatsRequest));
                    listener.onResponse(adStatsResponse);
                }, e -> listener.onFailure(new RuntimeException("Failed to get AD cluster stats", e))));
            } else {
                adStats.getStat(StatNames.DETECTOR_COUNT.getName()).setValue(0L);
                adStatsResponse.setClusterStats(getClusterStatsMap(adStatsRequest));
                listener.onResponse(adStatsResponse);
            }
        } else {
            adStatsResponse.setClusterStats(getClusterStatsMap(adStatsRequest));
            listener.onResponse(adStatsResponse);
        }
    }

    /**
     * Make async request to get the Anomaly Detection statistics from each node and, onResponse, set the
     * ADStatsNodesResponse field of ADStatsResponse
     *
     * @param client Client
     * @param listener MultiResponsesDelegateActionListener to be used once both requests complete
     * @param adStatsRequest Request containing stats to be retrieved
     */
    private void getNodeStats(
        Client client,
        MultiResponsesDelegateActionListener<ADStatsResponse> listener,
        ADStatsRequest adStatsRequest
    ) {
        client.execute(ADStatsNodesAction.INSTANCE, adStatsRequest, ActionListener.wrap(adStatsResponse -> {
            ADStatsResponse restADStatsResponse = new ADStatsResponse();
            restADStatsResponse.setADStatsNodesResponse(adStatsResponse);
            listener.onResponse(restADStatsResponse);
        }, listener::onFailure));
    }

    /**
     * Collect Cluster Stats into map to be retrieved
     *
     * @param adStatsRequest Request containing stats to be retrieved
     * @return Map containing Cluster Stats
     */
    private Map<String, Object> getClusterStatsMap(ADStatsRequest adStatsRequest) {
        Map<String, Object> clusterStats = new HashMap<>();
        Set<String> statsToBeRetrieved = adStatsRequest.getStatsToBeRetrieved();
        adStats
            .getClusterStats()
            .entrySet()
            .stream()
            .filter(s -> statsToBeRetrieved.contains(s.getKey()))
            .forEach(s -> clusterStats.put(s.getKey(), s.getValue().getValue()));
        return clusterStats;
    }

    /**
     * Listener sends response once Node Stats and Cluster Stats are gathered
     *
     * @param channel Channel
     * @return ActionListener for ADStatsResponse
     */
    private ActionListener<ADStatsResponse> getRestStatsListener(RestChannel channel) {
        return ActionListener
            .wrap(
                adStatsResponse -> {
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, adStatsResponse.toXContent(channel.newBuilder())));
                },
                exception -> channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, exception.getMessage()))
            );
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                new Route(RestRequest.Method.GET, AD_BASE_URI + "/{nodeId}/stats/"),
                new Route(RestRequest.Method.GET, AD_BASE_URI + "/{nodeId}/stats/{stat}"),
                new Route(RestRequest.Method.GET, AD_BASE_URI + "/stats/"),
                new Route(RestRequest.Method.GET, AD_BASE_URI + "/stats/{stat}")
            );
    }
}
