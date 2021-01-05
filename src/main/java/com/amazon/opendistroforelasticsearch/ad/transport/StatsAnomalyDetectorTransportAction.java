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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorType;
import com.amazon.opendistroforelasticsearch.ad.stats.ADStats;
import com.amazon.opendistroforelasticsearch.ad.stats.ADStatsResponse;
import com.amazon.opendistroforelasticsearch.ad.stats.StatNames;
import com.amazon.opendistroforelasticsearch.ad.util.MultiResponsesDelegateActionListener;

public class StatsAnomalyDetectorTransportAction extends HandledTransportAction<ADStatsRequest, StatsAnomalyDetectorResponse> {
    public static final String DETECTOR_TYPE_AGG = "detector_type_agg";
    private final Logger logger = LogManager.getLogger(StatsAnomalyDetectorTransportAction.class);

    private final Client client;
    private final ADStats adStats;
    private final ClusterService clusterService;

    @Inject
    public StatsAnomalyDetectorTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ADStats adStats,
        ClusterService clusterService

    ) {
        super(StatsAnomalyDetectorAction.NAME, transportService, actionFilters, ADStatsRequest::new);
        this.client = client;
        this.adStats = adStats;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, ADStatsRequest request, ActionListener<StatsAnomalyDetectorResponse> listener) {
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            getStats(client, listener, request);
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    /**
     * Make the 2 requests to get the node and cluster statistics
     *
     * @param client Client
     * @param listener Listener to send response
     * @param adStatsRequest Request containing stats to be retrieved
     */
    private void getStats(Client client, ActionListener<StatsAnomalyDetectorResponse> listener, ADStatsRequest adStatsRequest) {
        // Use MultiResponsesDelegateActionListener to execute 2 async requests and create the response once they finish
        MultiResponsesDelegateActionListener<ADStatsResponse> delegateListener = new MultiResponsesDelegateActionListener<>(
            getRestStatsListener(listener),
            2,
            "Unable to return AD Stats",
            false
        );

        getClusterStats(client, delegateListener, adStatsRequest);
        getNodeStats(client, delegateListener, adStatsRequest);
    }

    /**
     * Listener sends response once Node Stats and Cluster Stats are gathered
     *
     * @param listener Listener to send response
     * @return ActionListener for ADStatsResponse
     */
    private ActionListener<ADStatsResponse> getRestStatsListener(ActionListener<StatsAnomalyDetectorResponse> listener) {
        return ActionListener
            .wrap(
                adStatsResponse -> { listener.onResponse(new StatsAnomalyDetectorResponse(adStatsResponse)); },
                exception -> listener.onFailure(new ElasticsearchStatusException(exception.getMessage(), RestStatus.INTERNAL_SERVER_ERROR))
            );
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
        if ((adStatsRequest.getStatsToBeRetrieved().contains(StatNames.DETECTOR_COUNT.getName())
            || adStatsRequest.getStatsToBeRetrieved().contains(StatNames.HISTORICAL_SINGLE_ENTITY_DETECTOR_COUNT.getName()))
            && clusterService.state().getRoutingTable().hasIndex(AnomalyDetector.ANOMALY_DETECTORS_INDEX)) {

            TermsAggregationBuilder termsAgg = AggregationBuilders.terms(DETECTOR_TYPE_AGG).field(AnomalyDetector.DETECTOR_TYPE_FIELD);
            SearchRequest request = new SearchRequest()
                .indices(AnomalyDetector.ANOMALY_DETECTORS_INDEX)
                .source(new SearchSourceBuilder().aggregation(termsAgg).size(0).trackTotalHits(true));

            client.search(request, ActionListener.wrap(r -> {
                StringTerms aggregation = r.getAggregations().get(DETECTOR_TYPE_AGG);
                List<StringTerms.Bucket> buckets = aggregation.getBuckets();
                long totalDetectors = r.getHits().getTotalHits().value;
                long totalHistoricalSingleEntityDetectors = 0;
                for (StringTerms.Bucket b : buckets) {
                    if (AnomalyDetectorType.HISTORICAL_SINGLE_ENTITY.name().equals(b.getKeyAsString())) {
                        totalHistoricalSingleEntityDetectors += b.getDocCount();
                    }
                }
                if (adStatsRequest.getStatsToBeRetrieved().contains(StatNames.DETECTOR_COUNT.getName())) {
                    adStats.getStat(StatNames.DETECTOR_COUNT.getName()).setValue(totalDetectors);
                }
                if (adStatsRequest.getStatsToBeRetrieved().contains(StatNames.HISTORICAL_SINGLE_ENTITY_DETECTOR_COUNT.getName())) {
                    adStats
                        .getStat(StatNames.HISTORICAL_SINGLE_ENTITY_DETECTOR_COUNT.getName())
                        .setValue(totalHistoricalSingleEntityDetectors);
                }
                adStatsResponse.setClusterStats(getClusterStatsMap(adStatsRequest));
                listener.onResponse(adStatsResponse);
            }, e -> listener.onFailure(e)));
        } else {
            adStatsResponse.setClusterStats(getClusterStatsMap(adStatsRequest));
            listener.onResponse(adStatsResponse);
        }
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
}
