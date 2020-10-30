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

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorInternalState;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.AnomalyDetectorFunction;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;

public class DeleteAnomalyDetectorTransportAction extends HandledTransportAction<DeleteAnomalyDetectorRequest, DeleteResponse> {

    private static final Logger LOG = LogManager.getLogger(DeleteAnomalyDetectorTransportAction.class);
    private final Client client;
    private final ClusterService clusterService;
    private NamedXContentRegistry xContentRegistry;

    @Inject
    public DeleteAnomalyDetectorTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry
    ) {
        super(DeleteAnomalyDetectorAction.NAME, transportService, actionFilters, DeleteAnomalyDetectorRequest::new);
        this.client = client;
        this.clusterService = clusterService;
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    protected void doExecute(Task task, DeleteAnomalyDetectorRequest request, ActionListener<DeleteResponse> listener) {
        String detectorId = request.getDetectorID();
        LOG.info("Delete anomaly detector job {}", detectorId);

        // By the time request reaches here, the user permissions are validated by Security plugin.
        // Since the detectorID is provided, this can only happen if User is part of a role which has access
        // to the detector. This is filtered by our Search Detector API.
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            getDetectorJob(detectorId, listener, () -> deleteAnomalyDetectorJobDoc(detectorId, listener));

            DeleteRequest deleteRequest = new DeleteRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX, detectorId)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            client.delete(deleteRequest, ActionListener.wrap(response -> {
                if (response.getResult() == DocWriteResponse.Result.DELETED || response.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                    deleteDetectorStateDoc(detectorId, listener);
                } else {
                    LOG.error("Fail to delete anomaly detector job {}", detectorId);
                }
            }, exception -> {
                if (exception instanceof IndexNotFoundException) {
                    deleteDetectorStateDoc(detectorId, listener);
                } else {
                    LOG.error("Failed to delete anomaly detector job", exception);
                    listener.onFailure(exception);
                }
            }));
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }
    }

    private void deleteAnomalyDetectorJobDoc(String detectorId, ActionListener<DeleteResponse> listener) {
        LOG.info("Delete anomaly detector job {}", detectorId);
        DeleteRequest deleteRequest = new DeleteRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX, detectorId)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client.delete(deleteRequest, ActionListener.wrap(response -> {
            if (response.getResult() == DocWriteResponse.Result.DELETED || response.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                deleteDetectorStateDoc(detectorId, listener);
            } else {
                String message = "Fail to delete anomaly detector job " + detectorId;
                LOG.error(message);
                listener.onFailure(new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                deleteDetectorStateDoc(detectorId, listener);
            } else {
                LOG.error("Failed to delete anomaly detector job", exception);
                listener.onFailure(exception);
            }
        }));
    }

    private void deleteDetectorStateDoc(String detectorId, ActionListener<DeleteResponse> listener) {
        LOG.info("Delete detector info {}", detectorId);
        DeleteRequest deleteRequest = new DeleteRequest(DetectorInternalState.DETECTOR_STATE_INDEX, detectorId);
        client
            .delete(
                deleteRequest,
                ActionListener
                    .wrap(
                        response -> {
                            // whether deleted state doc or not, continue as state doc may not exist
                            deleteAnomalyDetectorDoc(detectorId, listener);
                        },
                        exception -> {
                            if (exception instanceof IndexNotFoundException) {
                                deleteAnomalyDetectorDoc(detectorId, listener);
                            } else {
                                LOG.error("Failed to delete detector state", exception);
                                listener.onFailure(exception);
                            }
                        }
                    )
            );
    }

    private void deleteAnomalyDetectorDoc(String detectorId, ActionListener<DeleteResponse> listener) {
        LOG.info("Delete anomaly detector {}", detectorId);
        DeleteRequest deleteRequest = new DeleteRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX, detectorId)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client.delete(deleteRequest, new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                listener.onResponse(deleteResponse);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void getDetectorJob(String detectorId, ActionListener<DeleteResponse> listener, AnomalyDetectorFunction function) {
        if (clusterService.state().metadata().indices().containsKey(ANOMALY_DETECTOR_JOB_INDEX)) {
            GetRequest request = new GetRequest(ANOMALY_DETECTOR_JOB_INDEX).id(detectorId);
            client.get(request, ActionListener.wrap(response -> onGetAdJobResponseForWrite(response, listener, function), exception -> {
                LOG.error("Fail to get anomaly detector job: " + detectorId, exception);
                listener.onFailure(exception);
            }));
        } else {
            function.execute();
        }
    }

    private void onGetAdJobResponseForWrite(GetResponse response, ActionListener<DeleteResponse> listener, AnomalyDetectorFunction function)
        throws IOException {
        if (response.isExists()) {
            String adJobId = response.getId();
            if (adJobId != null) {
                // check if AD job is running on the detector, if yes, we can't delete the detector
                try (
                    XContentParser parser = RestHandlerUtils
                        .createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                    AnomalyDetectorJob adJob = AnomalyDetectorJob.parse(parser);
                    if (adJob.isEnabled()) {
                        listener.onFailure(new ElasticsearchStatusException("Detector job is running: " + adJobId, RestStatus.BAD_REQUEST));
                        return;
                    }
                } catch (IOException e) {
                    String message = "Failed to parse anomaly detector job " + adJobId;
                    LOG.error(message, e);
                }
            }
        }
        function.execute();
    }
}
