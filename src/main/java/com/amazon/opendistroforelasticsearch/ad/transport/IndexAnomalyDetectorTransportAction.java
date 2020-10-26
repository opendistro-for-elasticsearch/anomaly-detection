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

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.AnomalyDetectorFunction;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.IndexAnomalyDetectorActionHandler;

public class IndexAnomalyDetectorTransportAction extends HandledTransportAction<IndexAnomalyDetectorRequest, IndexAnomalyDetectorResponse> {
    private static final Logger LOG = LogManager.getLogger(IndexAnomalyDetectorTransportAction.class);
    private final Client client;
    private final RestClient restClient;
    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;

    @Inject
    public IndexAnomalyDetectorTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        RestClient restClient,
        ClusterService clusterService,
        Settings settings,
        AnomalyDetectionIndices anomalyDetectionIndices,
        NamedXContentRegistry xContentRegistry
    ) {
        super(IndexAnomalyDetectorAction.NAME, transportService, actionFilters, IndexAnomalyDetectorRequest::new);
        this.client = client;
        this.clusterService = clusterService;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.xContentRegistry = xContentRegistry;
        this.restClient = restClient;
    }

    @Override
    protected void doExecute(Task task, IndexAnomalyDetectorRequest request, ActionListener<IndexAnomalyDetectorResponse> listener) {
        anomalyDetectionIndices.updateMappingIfNecessary();
        String detectorId = request.getDetectorID();
        long seqNo = request.getSeqNo();
        long primaryTerm = request.getPrimaryTerm();
        WriteRequest.RefreshPolicy refreshPolicy = request.getRefreshPolicy();
        AnomalyDetector detector = request.getDetector();
        RestRequest.Method method = request.getMethod();
        TimeValue requestTimeout = request.getRequestTimeout();
        Integer maxSingleEntityAnomalyDetectors = request.getMaxSingleEntityAnomalyDetectors();
        Integer maxMultiEntityAnomalyDetectors = request.getMaxMultiEntityAnomalyDetectors();
        Integer maxAnomalyFeatures = request.getMaxAnomalyFeatures();

        checkIndicesAndExecute(detector.getIndices(), () -> {
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                IndexAnomalyDetectorActionHandler indexAnomalyDetectorActionHandler = new IndexAnomalyDetectorActionHandler(
                    clusterService,
                    client,
                    listener,
                    anomalyDetectionIndices,
                    detectorId,
                    seqNo,
                    primaryTerm,
                    refreshPolicy,
                    detector,
                    requestTimeout,
                    maxSingleEntityAnomalyDetectors,
                    maxMultiEntityAnomalyDetectors,
                    maxAnomalyFeatures,
                    method,
                    xContentRegistry,
                    restClient,
                    request.getAuthHeader()
                );
                try {
                    indexAnomalyDetectorActionHandler.resolveUserAndStart();
                } catch (IOException exception) {
                    LOG.error("Fail to index detector", exception);
                    listener.onFailure(exception);
                }
            } catch (Exception e) {
                LOG.error(e);
                listener.onFailure(e);
            }

        }, listener);
    }

    private void checkIndicesAndExecute(
        List<String> indices,
        AnomalyDetectorFunction function,
        ActionListener<IndexAnomalyDetectorResponse> listener
    ) {
        SearchRequest searchRequest = new SearchRequest()
            .indices(indices.toArray(new String[0]))
            .source(new SearchSourceBuilder().size(1).query(QueryBuilders.matchAllQuery()));
        client.search(searchRequest, ActionListener.wrap(r -> {
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                function.execute();
            } catch (Exception e) {
                LOG.error(e);
                listener.onFailure(e);
            }
        }, e -> {
            // Due to below issue with security plugin, we get security_exception when invalid index name is mentioned.
            // https://github.com/opendistro-for-elasticsearch/security/issues/718
            LOG.error(e);
            listener.onFailure(e);
        }));
    }
}
