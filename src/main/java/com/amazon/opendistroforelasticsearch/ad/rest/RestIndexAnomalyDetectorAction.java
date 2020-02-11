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

import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.IndexAnomalyDetectorActionHandler;
import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.util.Locale;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.DETECTION_INTERVAL;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.DETECTION_WINDOW_DELAY;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.REQUEST_TIMEOUT;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.DETECTOR_ID;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.IF_PRIMARY_TERM;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.IF_SEQ_NO;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.REFRESH;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Rest handlers to create and update anomaly detector.
 */
public class RestIndexAnomalyDetectorAction extends BaseRestHandler {

    private static final String INDEX_ANOMALY_DETECTOR_ACTION = "index_anomaly_detector_action";
    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final Logger logger = LogManager.getLogger(RestIndexAnomalyDetectorAction.class);
    private final ClusterService clusterService;
    private final Settings settings;

    private volatile TimeValue requestTimeout;
    private volatile TimeValue detectionInterval;
    private volatile TimeValue detectionWindowDelay;

    public RestIndexAnomalyDetectorAction(
        Settings settings,
        RestController controller,
        ClusterService clusterService,
        AnomalyDetectionIndices anomalyDetectionIndices
    ) {
        controller.registerHandler(RestRequest.Method.POST, AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, this); // Create
        controller
            .registerHandler(
                RestRequest.Method.PUT,
                String.format(Locale.ROOT, "%s/{%s}", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID),
                this
            ); // update
        this.settings = settings;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.requestTimeout = REQUEST_TIMEOUT.get(settings);
        this.detectionInterval = DETECTION_INTERVAL.get(settings);
        this.detectionWindowDelay = DETECTION_WINDOW_DELAY.get(settings);
        this.clusterService = clusterService;
        // TODO: will add more cluster setting consumer later
        // TODO: inject ClusterSettings only if clusterService is only used to get ClusterSettings
        clusterService.getClusterSettings().addSettingsUpdateConsumer(REQUEST_TIMEOUT, it -> requestTimeout = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DETECTION_INTERVAL, it -> detectionInterval = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DETECTION_WINDOW_DELAY, it -> detectionWindowDelay = it);
    }

    @Override
    public String getName() {
        return INDEX_ANOMALY_DETECTOR_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String detectorId = request.param(DETECTOR_ID, AnomalyDetector.NO_ID);
        logger.info("AnomalyDetector {} action for detectorId {}", request.method(), detectorId);

        XContentParser parser = request.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        // TODO: check detection interval < modelTTL
        AnomalyDetector detector = AnomalyDetector.parse(parser, detectorId, null, detectionInterval, detectionWindowDelay);

        long seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO);
        long primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
        WriteRequest.RefreshPolicy refreshPolicy = request.hasParam(REFRESH)
            ? WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
            : WriteRequest.RefreshPolicy.IMMEDIATE;

        return channel -> new IndexAnomalyDetectorActionHandler(
            settings,
            clusterService,
            client,
            channel,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            detector,
            requestTimeout
        ).start();
    }

}
