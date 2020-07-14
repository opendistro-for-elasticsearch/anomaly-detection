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

import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.DETECTOR_ID;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorInternalState;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.AnomalyDetectorActionHandler;
import com.amazon.opendistroforelasticsearch.ad.settings.EnabledSetting;
import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to delete anomaly detector.
 */
public class RestDeleteAnomalyDetectorAction extends BaseRestHandler {

    public static final String DELETE_ANOMALY_DETECTOR_ACTION = "delete_anomaly_detector";

    private static final Logger logger = LogManager.getLogger(RestDeleteAnomalyDetectorAction.class);
    private final ClusterService clusterService;
    private final AnomalyDetectorActionHandler handler = new AnomalyDetectorActionHandler();

    public RestDeleteAnomalyDetectorAction(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String getName() {
        return DELETE_ANOMALY_DETECTOR_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }

        String detectorId = request.param(DETECTOR_ID);

        return channel -> {
            logger.info("Delete anomaly detector {}", detectorId);
            handler
                .getDetectorJob(
                    clusterService,
                    client,
                    detectorId,
                    channel,
                    () -> deleteAnomalyDetectorJobDoc(client, detectorId, channel)
                );
        };
    }

    private void deleteAnomalyDetectorJobDoc(NodeClient client, String detectorId, RestChannel channel) {
        logger.info("Delete anomaly detector job {}", detectorId);
        DeleteRequest deleteRequest = new DeleteRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX, detectorId)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client.delete(deleteRequest, ActionListener.wrap(response -> {
            if (response.getResult() == DocWriteResponse.Result.DELETED || response.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                deleteDetectorStateDoc(client, detectorId, channel);
            } else {
                logger.error("Fail to delete anomaly detector job {}", detectorId);
            }
        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                deleteDetectorStateDoc(client, detectorId, channel);
            } else {
                logger.error("Failed to delete anomaly detector job", exception);
                try {
                    channel.sendResponse(new BytesRestResponse(channel, exception));
                } catch (IOException e) {
                    logger.error("Failed to send response of delete anomaly detector job exception", e);
                }
            }
        }));
    }

    private void deleteDetectorStateDoc(NodeClient client, String detectorId, RestChannel channel) {
        logger.info("Delete detector info {}", detectorId);
        DeleteRequest deleteRequest = new DeleteRequest(DetectorInternalState.DETECTOR_STATE_INDEX, detectorId);
        client
            .delete(
                deleteRequest,
                ActionListener
                    .wrap(
                        response -> {
                            // whether deleted state doc or not, continue as state doc may not exist
                            deleteAnomalyDetectorDoc(client, detectorId, channel);
                        },
                        exception -> {
                            if (exception instanceof IndexNotFoundException) {
                                deleteAnomalyDetectorDoc(client, detectorId, channel);
                            } else {
                                logger.error("Failed to delete detector state", exception);
                                try {
                                    channel.sendResponse(new BytesRestResponse(channel, exception));
                                } catch (IOException e) {
                                    logger.error("Failed to send response of deletedetector state", e);
                                }
                            }
                        }
                    )
            );
    }

    private void deleteAnomalyDetectorDoc(NodeClient client, String detectorId, RestChannel channel) {
        logger.info("Delete anomaly detector {}", detectorId);
        DeleteRequest deleteRequest = new DeleteRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX, detectorId)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client.delete(deleteRequest, new RestStatusToXContentListener<>(channel));
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                // delete anomaly detector document
                new Route(
                    RestRequest.Method.DELETE,
                    String.format(Locale.ROOT, "%s/{%s}", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID)
                )
            );
    }
}
