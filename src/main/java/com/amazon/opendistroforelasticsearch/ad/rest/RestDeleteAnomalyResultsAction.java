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

package com.amazon.opendistroforelasticsearch.ad.rest;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.settings.EnabledSetting;
import com.amazon.opendistroforelasticsearch.ad.transport.DeleteAnomalyResultsAction;
import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to retrieve an anomaly detector.
 */
public class RestDeleteAnomalyResultsAction extends BaseRestHandler {

    private static final String DELETE_AD_RESULTS_ACTION = "delete_anomaly_results";
    private static final Logger logger = LogManager.getLogger(RestDeleteAnomalyResultsAction.class);

    public RestDeleteAnomalyResultsAction() {}

    @Override
    public String getName() {
        return DELETE_AD_RESULTS_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.parseXContent(request.contentOrSourceParamParser());
        // searchSourceBuilder.fetchSource(getSourceContext(request));
        // searchSourceBuilder.seqNoAndPrimaryTerm(true).version(true);
        // logger.info("----------------------------------------");
        // logger.info(searchSourceBuilder);
        // logger.info("----------------------------------------");
        DeleteByQueryRequest deleteRequest = new DeleteByQueryRequest(CommonName.ANOMALY_RESULT_INDEX_PATTERN)
            .setQuery(searchSourceBuilder.query())
            // .setBatchSize(1000)
            // .setRequestsPerSecond(1)
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        // return channel -> client.execute(DeleteByQueryAction.INSTANCE, deleteRequest, ActionListener.wrap(r -> {
        //// XContentBuilder xContentBuilder = channel.newBuilder().startObject();
        // XContentBuilder contentBuilder = r.toXContent(channel.newBuilder().startObject(), ToXContent.EMPTY_PARAMS);
        // contentBuilder.endObject();
        // channel.sendResponse(new BytesRestResponse(RestStatus.OK, contentBuilder));
        // }, e-> {
        // try {
        // channel.sendResponse(new BytesRestResponse(channel, e));
        // } catch (IOException exception) {
        // logger.error("Failed to send back delete anomaly result exception result", exception);
        // }
        // }));
        return channel -> client.execute(DeleteAnomalyResultsAction.INSTANCE, deleteRequest, ActionListener.wrap(r -> {
            XContentBuilder contentBuilder = r.toXContent(channel.newBuilder().startObject(), ToXContent.EMPTY_PARAMS);
            contentBuilder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, contentBuilder));
        }, e -> {
            try {
                channel.sendResponse(new BytesRestResponse(channel, e));
            } catch (IOException exception) {
                logger.error("Failed to send back delete anomaly result exception result", exception);
            }
        }));
        // return channel -> client
        // .execute(GetAnomalyDetectorAction.INSTANCE, getAnomalyDetectorRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(RestRequest.Method.DELETE, AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI + "/results"));
    }
}
