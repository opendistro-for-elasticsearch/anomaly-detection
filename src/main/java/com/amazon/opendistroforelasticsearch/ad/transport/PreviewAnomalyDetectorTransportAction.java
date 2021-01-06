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

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_ANOMALY_FEATURES;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorRunner;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;

public class PreviewAnomalyDetectorTransportAction extends
    HandledTransportAction<PreviewAnomalyDetectorRequest, PreviewAnomalyDetectorResponse> {
    private final Logger logger = LogManager.getLogger(PreviewAnomalyDetectorTransportAction.class);
    private final AnomalyDetectorRunner anomalyDetectorRunner;
    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    private volatile Integer maxAnomalyFeatures;

    @Inject
    public PreviewAnomalyDetectorTransportAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        Client client,
        AnomalyDetectorRunner anomalyDetectorRunner,
        NamedXContentRegistry xContentRegistry
    ) {
        super(PreviewAnomalyDetectorAction.NAME, transportService, actionFilters, PreviewAnomalyDetectorRequest::new);
        this.client = client;
        this.anomalyDetectorRunner = anomalyDetectorRunner;
        this.xContentRegistry = xContentRegistry;
        maxAnomalyFeatures = MAX_ANOMALY_FEATURES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_ANOMALY_FEATURES, it -> maxAnomalyFeatures = it);
    }

    @Override
    protected void doExecute(Task task, PreviewAnomalyDetectorRequest request, ActionListener<PreviewAnomalyDetectorResponse> listener) {
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            AnomalyDetector detector = request.getDetector();
            String detectorId = request.getDetectorId();
            Instant startTime = request.getStartTime();
            Instant endTime = request.getEndTime();
            if (detector != null) {
                String error = validateDetector(detector);
                if (StringUtils.isNotBlank(error)) {
                    listener.onFailure(new ElasticsearchException(error, RestStatus.BAD_REQUEST));
                    return;
                }
                anomalyDetectorRunner.executeDetector(detector, startTime, endTime, getPreviewDetectorActionListener(listener, detector));
            } else {
                previewAnomalyDetector(listener, detectorId, startTime, endTime);
            }
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    private String validateDetector(AnomalyDetector detector) {
        if (detector.getFeatureAttributes().isEmpty()) {
            return "Can't preview detector without feature";
        } else {
            return RestHandlerUtils.validateAnomalyDetector(detector, maxAnomalyFeatures);
        }
    }

    private ActionListener<List<AnomalyResult>> getPreviewDetectorActionListener(
        ActionListener<PreviewAnomalyDetectorResponse> listener,
        AnomalyDetector detector
    ) {
        return ActionListener.wrap(new CheckedConsumer<List<AnomalyResult>, Exception>() {
            @Override
            public void accept(List<AnomalyResult> anomalyResult) throws Exception {
                PreviewAnomalyDetectorResponse response = new PreviewAnomalyDetectorResponse(anomalyResult, detector);
                listener.onResponse(response);
            }
        }, exception -> {
            logger.error("Unexpected error running anomaly detector " + detector.getDetectorId(), exception);
            listener
                .onFailure(
                    new ElasticsearchException(
                        "Unexpected error running anomaly detector " + detector.getDetectorId(),
                        RestStatus.INTERNAL_SERVER_ERROR
                    )
                );
        });
    }

    private void previewAnomalyDetector(
        ActionListener<PreviewAnomalyDetectorResponse> listener,
        String detectorId,
        Instant startTime,
        Instant endTime
    ) {
        if (!StringUtils.isBlank(detectorId)) {
            GetRequest getRequest = new GetRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX).id(detectorId);
            client.get(getRequest, onGetAnomalyDetectorResponse(listener, startTime, endTime));
        } else {
            listener.onFailure(new ElasticsearchException("Wrong input, no detector id", RestStatus.BAD_REQUEST));
        }
    }

    private ActionListener<GetResponse> onGetAnomalyDetectorResponse(
        ActionListener<PreviewAnomalyDetectorResponse> listener,
        Instant startTime,
        Instant endTime
    ) {
        return ActionListener.wrap(new CheckedConsumer<GetResponse, Exception>() {
            @Override
            public void accept(GetResponse response) throws Exception {
                if (!response.isExists()) {
                    listener
                        .onFailure(
                            new ElasticsearchException("Can't find anomaly detector with id:" + response.getId(), RestStatus.NOT_FOUND)
                        );
                    return;
                }

                try {
                    XContentParser parser = RestHandlerUtils
                        .createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef());
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    AnomalyDetector detector = AnomalyDetector.parse(parser, response.getId(), response.getVersion());

                    anomalyDetectorRunner
                        .executeDetector(detector, startTime, endTime, getPreviewDetectorActionListener(listener, detector));
                } catch (IOException e) {
                    listener.onFailure(e);
                }
            }
        }, exception -> { listener.onFailure(new ElasticsearchException("Could not execute get query to find detector")); });
    }
}
