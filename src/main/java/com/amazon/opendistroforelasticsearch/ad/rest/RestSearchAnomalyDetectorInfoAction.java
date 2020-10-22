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

import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.COUNT;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.MATCH;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.settings.EnabledSetting;
import com.amazon.opendistroforelasticsearch.ad.transport.SearchAnomalyDetectorInfoAction;
import com.amazon.opendistroforelasticsearch.ad.transport.SearchAnomalyDetectorInfoRequest;
import com.google.common.collect.ImmutableList;

public class RestSearchAnomalyDetectorInfoAction extends BaseRestHandler {

    public static final String SEARCH_ANOMALY_DETECTOR_INFO_ACTION = "search_anomaly_detector_info";

    private static final Logger logger = LogManager.getLogger(RestSearchAnomalyDetectorInfoAction.class);

    public RestSearchAnomalyDetectorInfoAction() {}

    @Override
    public String getName() {
        return SEARCH_ANOMALY_DETECTOR_INFO_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, org.elasticsearch.client.node.NodeClient client) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }

        String detectorName = request.param("name", null);
        String rawPath = request.rawPath();

        SearchAnomalyDetectorInfoRequest searchAnomalyDetectorInfoRequest = new SearchAnomalyDetectorInfoRequest(detectorName, rawPath);
        return channel -> client
            .execute(SearchAnomalyDetectorInfoAction.INSTANCE, searchAnomalyDetectorInfoRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public List<RestHandler.Route> routes() {
        return ImmutableList
            .of(
                // get the count of number of detectors
                new RestHandler.Route(
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/%s", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, COUNT)
                ),
                // get if a detector name exists with name
                new RestHandler.Route(RestRequest.Method.GET, String.format("%s/%s", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, MATCH))
            );
    }
}
