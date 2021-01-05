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

package com.amazon.opendistroforelasticsearch.ad.transport.handler;

import static com.amazon.opendistroforelasticsearch.ad.constant.CommonName.ANOMALY_RESULT_INDEX_ALIAS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.IndexUtils;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;

public class AnomalyResultBulkIndexHandler extends AnomalyIndexHandler<AnomalyResult> {
    private static final Logger LOG = LogManager.getLogger(AnomalyResultBulkIndexHandler.class);

    private AnomalyDetectionIndices anomalyDetectionIndices;

    public AnomalyResultBulkIndexHandler(
        Client client,
        Settings settings,
        ThreadPool threadPool,
        Consumer<ActionListener<CreateIndexResponse>> createIndex,
        BooleanSupplier indexExists,
        ClientUtil clientUtil,
        IndexUtils indexUtils,
        ClusterService clusterService,
        AnomalyDetectionIndices anomalyDetectionIndices
    ) {
        super(client, settings, threadPool, ANOMALY_RESULT_INDEX_ALIAS, createIndex, indexExists, clientUtil, indexUtils, clusterService);
        this.anomalyDetectionIndices = anomalyDetectionIndices;
    }

    /**
     * Bulk index anomaly results. Create anomaly result index first if it doesn't exist.
     *
     * @param anomalyResults anomaly results
     * @param listener action listener
     */
    public void bulkIndexAnomalyResult(List<AnomalyResult> anomalyResults, ActionListener<BulkResponse> listener) {
        if (anomalyResults == null || anomalyResults.size() == 0) {
            listener.onResponse(null);
            return;
        }
        try {
            if (!anomalyDetectionIndices.doesAnomalyResultIndexExist()) {
                anomalyDetectionIndices.initAnomalyResultIndexDirectly(ActionListener.wrap(response -> {
                    if (response.isAcknowledged()) {
                        bulkSaveDetectorResult(anomalyResults, listener);
                    } else {
                        String error = "Creating anomaly result index with mappings call not acknowledged";
                        LOG.error(error);
                        listener.onFailure(new AnomalyDetectionException(error));
                    }
                }, exception -> {
                    if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                        // It is possible the index has been created while we sending the create request
                        bulkSaveDetectorResult(anomalyResults, listener);
                    } else {
                        listener.onFailure(exception);
                    }
                }));
            } else {
                bulkSaveDetectorResult(anomalyResults, listener);
            }
        } catch (AnomalyDetectionException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            String error = "Failed to bulk index anomaly result";
            LOG.error(error, e);
            listener.onFailure(new AnomalyDetectionException(error, e));
        }
    }

    private void bulkSaveDetectorResult(List<AnomalyResult> anomalyResults, ActionListener<BulkResponse> listener) {
        LOG.debug("Start to bulk save {} anomaly results", anomalyResults.size());
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        anomalyResults.forEach(anomalyResult -> {
            try (XContentBuilder builder = jsonBuilder()) {
                IndexRequest indexRequest = new IndexRequest(ANOMALY_RESULT_INDEX_ALIAS)
                    .source(anomalyResult.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE));
                bulkRequestBuilder.add(indexRequest);
            } catch (Exception e) {
                String error = "Failed to prepare request to bulk index anomaly results";
                LOG.error(error, e);
                throw new AnomalyDetectionException(error);
            }
        });
        client.bulk(bulkRequestBuilder.request(), ActionListener.wrap(r -> {
            LOG.debug("bulk index AD result successfully, took: {}", r.getTook().duration());
            listener.onResponse(r);
        }, e -> {
            LOG.error("bulk index ad result failed", e);
            listener.onFailure(e);
        }));
    }

}
