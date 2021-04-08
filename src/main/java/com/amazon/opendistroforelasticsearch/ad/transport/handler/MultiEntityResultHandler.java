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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.transport.ADResultBulkAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADResultBulkRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.ADResultBulkResponse;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.IndexUtils;
import com.amazon.opendistroforelasticsearch.ad.util.ThrowingConsumerWrapper;

/**
 * EntityResultTransportAction depends on this class.  I cannot use
 * AnomalyIndexHandler &lt; AnomalyResult &gt; . All transport actions
 * needs dependency injection.  Guice has a hard time initializing generics class
 * AnomalyIndexHandler &lt; AnomalyResult &gt; due to type erasure.
 * To avoid that, I create a class with a built-in details so
 * that Guice would be able to work out the details.
 *
 */
public class MultiEntityResultHandler extends AnomalyIndexHandler<AnomalyResult> {
    private static final Logger LOG = LogManager.getLogger(MultiEntityResultHandler.class);
    private static final String SUCCESS_SAVING_RESULT_MSG = "Succeed in saving ";
    private static final String CANNOT_SAVE_RESULT_ERR_MSG = "Cannot save results due to write block.";

    @Inject
    public MultiEntityResultHandler(
        Client client,
        Settings settings,
        ThreadPool threadPool,
        AnomalyDetectionIndices anomalyDetectionIndices,
        ClientUtil clientUtil,
        IndexUtils indexUtils,
        ClusterService clusterService
    ) {
        super(
            client,
            settings,
            threadPool,
            CommonName.ANOMALY_RESULT_INDEX_ALIAS,
            ThrowingConsumerWrapper.throwingConsumerWrapper(anomalyDetectionIndices::initAnomalyResultIndexDirectly),
            anomalyDetectionIndices::doesAnomalyResultIndexExist,
            clientUtil,
            indexUtils,
            clusterService
        );
    }

    /**
     * Execute the bulk request
     * @param currentBulkRequest The bulk request
     * @param listener callback after flushing
     */
    public void flush(ADResultBulkRequest currentBulkRequest, ActionListener<ADResultBulkResponse> listener) {
        if (indexUtils.checkIndicesBlocked(clusterService.state(), ClusterBlockLevel.WRITE, this.indexName)) {
            listener.onFailure(new AnomalyDetectionException(CANNOT_SAVE_RESULT_ERR_MSG));
            return;
        }

        try {
            if (!indexExists.getAsBoolean()) {
                createIndex.accept(ActionListener.wrap(initResponse -> {
                    if (initResponse.isAcknowledged()) {
                        bulk(currentBulkRequest, listener);
                    } else {
                        LOG.warn("Creating result index with mappings call not acknowledged.");
                        listener.onFailure(new AnomalyDetectionException("", "Creating result index with mappings call not acknowledged."));
                    }
                }, exception -> {
                    if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                        // It is possible the index has been created while we sending the create request
                        bulk(currentBulkRequest, listener);
                    } else {
                        LOG.warn("Unexpected error creating result index", exception);
                        listener.onFailure(exception);
                    }
                }));
            } else {
                bulk(currentBulkRequest, listener);
            }
        } catch (Exception e) {
            LOG.warn("Error in bulking results", e);
            listener.onFailure(e);
        }
    }

    private void bulk(ADResultBulkRequest currentBulkRequest, ActionListener<ADResultBulkResponse> listener) {
        if (currentBulkRequest.numberOfActions() <= 0) {
            listener.onFailure(new AnomalyDetectionException("no result to save"));
            return;
        }
        client.execute(ADResultBulkAction.INSTANCE, currentBulkRequest, ActionListener.<ADResultBulkResponse>wrap(response -> {
            LOG.debug(SUCCESS_SAVING_RESULT_MSG);
            listener.onResponse(response);
        }, exception -> {
            LOG.error("Error in bulking results", exception);
            listener.onFailure(exception);
        }));
    }
}
