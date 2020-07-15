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

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.REQUEST_TIMEOUT;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.DETECTOR_ID;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.IF_PRIMARY_TERM;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.IF_SEQ_NO;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.START_JOB;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.STOP_JOB;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.IndexAnomalyDetectorJobActionHandler;
import com.amazon.opendistroforelasticsearch.ad.settings.EnabledSetting;
import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to handle request to start/stop AD job.
 */
public class RestAnomalyDetectorJobAction extends BaseRestHandler {

    public static final String AD_JOB_ACTION = "anomaly_detector_job_action";
    private volatile TimeValue requestTimeout;
    private final AnomalyDetectionIndices anomalyDetectionIndices;

    public RestAnomalyDetectorJobAction(Settings settings, ClusterService clusterService, AnomalyDetectionIndices anomalyDetectionIndices) {
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.requestTimeout = REQUEST_TIMEOUT.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(REQUEST_TIMEOUT, it -> requestTimeout = it);
    }

    @Override
    public String getName() {
        return AD_JOB_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }

        String detectorId = request.param(DETECTOR_ID);

        return channel -> {
            long seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO);
            long primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM);

            IndexAnomalyDetectorJobActionHandler handler = new IndexAnomalyDetectorJobActionHandler(
                client,
                channel,
                anomalyDetectionIndices,
                detectorId,
                seqNo,
                primaryTerm,
                requestTimeout
            );

            String rawPath = request.rawPath();

            if (rawPath.endsWith(START_JOB)) {
                handler.startAnomalyDetectorJob();
            } else if (rawPath.endsWith(STOP_JOB)) {
                handler.stopAnomalyDetectorJob(detectorId);
            }
        };
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                // start AD job
                new Route(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID, START_JOB)
                ),
                // stop AD job
                new Route(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID, STOP_JOB)
                )
            );
    }
}
