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
import com.amazon.opendistroforelasticsearch.ad.rest.handler.AnomalyDetectorActionHandler;
import com.amazon.opendistroforelasticsearch.ad.transport.StopDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.transport.StopDetectorRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.StopDetectorResponse;
import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.Locale;

import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.DETECTOR_ID;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.REFRESH;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.STOP;

/**
 * This class consists of the REST handler to delete anomaly detector.
 */
public class RestDeleteAnomalyDetectorAction extends BaseRestHandler {

    public static final String DELETE_ANOMALY_DETECTOR_ACTION = "delete_anomaly_detector";

    private static final Logger logger = LogManager.getLogger(RestDeleteAnomalyDetectorAction.class);
    private final ClusterService clusterService;
    private final AnomalyDetectorActionHandler handler = new AnomalyDetectorActionHandler();

    public RestDeleteAnomalyDetectorAction(RestController controller, ClusterService clusterService) {
        this.clusterService = clusterService;
        // delete anomaly detector document
        controller
            .registerHandler(
                RestRequest.Method.DELETE,
                String.format(Locale.ROOT, "%s/{%s}", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID),
                this
            );
        // stop running anomaly detector: clear model, cache, checkpoint, ad result
        controller
            .registerHandler(
                RestRequest.Method.POST,
                String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID, STOP),
                this
            );
    }

    @Override
    public String getName() {
        return DELETE_ANOMALY_DETECTOR_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String detectorId = request.param(DETECTOR_ID);

        WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy
            .parse(request.param(REFRESH, WriteRequest.RefreshPolicy.IMMEDIATE.getValue()));

        return channel -> {
            if (channel.request().method() == RestRequest.Method.POST) {
                logger.info("Stop anomaly detector {}", detectorId);
                StopDetectorRequest stopDetectorRequest = new StopDetectorRequest(detectorId);
                client.execute(StopDetectorAction.INSTANCE, stopDetectorRequest, stopAdDetectorListener(channel, detectorId));
            } else if (channel.request().method() == RestRequest.Method.DELETE) {
                logger.info("Delete anomaly detector {}", detectorId);
                handler
                    .getMonitorUsingDetector(
                        clusterService,
                        client,
                        detectorId,
                        channel,
                        () -> deleteAnomalyDetectorDoc(client, detectorId, channel, refreshPolicy)
                    );
            }
        };
    }

    private void deleteAnomalyDetectorDoc(
        NodeClient client,
        String detectorId,
        RestChannel channel,
        WriteRequest.RefreshPolicy refreshPolicy
    ) {
        logger.info("Delete anomaly detector {}", detectorId);
        DeleteRequest deleteRequest = new DeleteRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX, detectorId)
            .setRefreshPolicy(refreshPolicy);
        client.delete(deleteRequest, new RestStatusToXContentListener<>(channel));
    }

    private ActionListener<StopDetectorResponse> stopAdDetectorListener(RestChannel channel, String detectorId) {
        return new ActionListener<StopDetectorResponse>() {
            @Override
            public void onResponse(StopDetectorResponse stopDetectorResponse) {
                if (stopDetectorResponse.success()) {
                    logger.info("AD model deleted successfully for detector {}", detectorId);
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, "AD model deleted successfully"));
                } else {
                    logger.error("Failed to delete AD model for detector {}", detectorId);
                    channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, "Failed to delete AD model"));
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to delete AD model for detector " + detectorId, e);
            }
        };
    }
}
