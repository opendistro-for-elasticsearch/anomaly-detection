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

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.REQUEST_TIMEOUT;

import java.io.IOException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.IndexAnomalyDetectorJobActionHandler;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;

public class AnomalyDetectorJobTransportAction extends HandledTransportAction<AnomalyDetectorJobRequest, AnomalyDetectorJobResponse> {

    private final Client client;
    private final Settings settings;
    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final NamedXContentRegistry xContentRegistry;

    @Inject
    public AnomalyDetectorJobTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        Settings settings,
        AnomalyDetectionIndices anomalyDetectionIndices,
        NamedXContentRegistry xContentRegistry
    ) {
        super(AnomalyDetectorJobAction.NAME, transportService, actionFilters, AnomalyDetectorJobRequest::new);
        this.client = client;
        this.settings = settings;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    protected void doExecute(Task task, AnomalyDetectorJobRequest request, ActionListener<AnomalyDetectorJobResponse> listener) {
        String detectorId = request.getDetectorID();
        long seqNo = request.getSeqNo();
        long primaryTerm = request.getPrimaryTerm();
        String rawPath = request.getRawPath();
        TimeValue requestTimeout = REQUEST_TIMEOUT.get(settings);

        IndexAnomalyDetectorJobActionHandler handler = new IndexAnomalyDetectorJobActionHandler(
            client,
            listener,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            requestTimeout,
            xContentRegistry
        );
        try {
            if (rawPath.endsWith(RestHandlerUtils.START_JOB)) {
                handler.startAnomalyDetectorJob();
            } else if (rawPath.endsWith(RestHandlerUtils.STOP_JOB)) {
                handler.stopAnomalyDetectorJob(detectorId);
            }
        } catch (IOException e) {
            logger.error(e);
        }
    }
}
