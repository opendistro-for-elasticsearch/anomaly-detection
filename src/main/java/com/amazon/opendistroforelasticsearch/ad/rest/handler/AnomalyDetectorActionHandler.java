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

package com.amazon.opendistroforelasticsearch.ad.rest.handler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * Common handler to process AD request.
 */
public class AnomalyDetectorActionHandler {

    private final Logger logger = LogManager.getLogger(AnomalyDetectorActionHandler.class);
    private static final String MONITOR_INPUTS_ANOMALY_DETECTOR_ID = "monitor.inputs.anomaly_detector.detector_id";
    private static final String MONITOR_INPUTS = "monitor.inputs";
    private static final String OPENDISTRO_ALERTING_CONFIG_INDEX = ".opendistro-alerting-config";

    /**
     * Get monitor which is running on specified detector.
     * If monitor exists, return error message; otherwise, execute {@link AnomalyDetectorFunction}
     *
     * @param clusterService ES cluster service
     * @param client         ES node client
     * @param detectorId     Anomaly detector id
     * @param channel        ES rest channel
     * @param function       Anomaly detector function
     */
    public void getMonitorUsingDetector(
        ClusterService clusterService,
        NodeClient client,
        String detectorId,
        RestChannel channel,
        AnomalyDetectorFunction function
    ) {
        final BoolQueryBuilder booleanQuery = QueryBuilders.boolQuery();
        booleanQuery.must(QueryBuilders.termQuery(MONITOR_INPUTS_ANOMALY_DETECTOR_ID, detectorId));
        NestedQueryBuilder queryBuilder = QueryBuilders.nestedQuery(MONITOR_INPUTS, booleanQuery, ScoreMode.None);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder).size(1);

        if (clusterService.state().getMetaData().indices().containsKey(OPENDISTRO_ALERTING_CONFIG_INDEX)) {
            SearchRequest request = new SearchRequest();
            request.indices(OPENDISTRO_ALERTING_CONFIG_INDEX).source(searchSourceBuilder);
            client.search(request, ActionListener.wrap(response -> onSearchResponse(response, channel, function), exception -> {
                logger.error("Fail to search monitor using detector id" + detectorId, exception);
                try {
                    channel.sendResponse(new BytesRestResponse(channel, exception));
                } catch (IOException e) {
                    logger.error("Fail to send exception" + detectorId, e);
                }
            }));
        } else {
            function.execute();
        }
    }

    /**
     * Callback method for {@link AnomalyDetectorActionHandler#getMonitorUsingDetector}.
     * If search result contains at least one monitor, return error message;
     * otherwise, execute {@link AnomalyDetectorFunction}
     *
     * @param response Response of searching monitor
     * @param channel  ES rest channel
     * @param function Anomaly detector function
     */
    private void onSearchResponse(SearchResponse response, RestChannel channel, AnomalyDetectorFunction function) {
        if (response.getHits().getTotalHits().value > 0) {
            String monitorId = response.getHits().getAt(0).getId();
            if (monitorId != null) {
                // check if any monitor running on the detector, if yes, we can't delete the detector
                channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, "Detector is used by monitor: " + monitorId));
                return;
            }
        }
        function.execute();
    }
}
