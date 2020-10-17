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

import java.time.Clock;
import java.util.Locale;
import java.util.concurrent.RejectedExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.Throwables;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.NodeStateManager;
import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.transport.ADResultBulkAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADResultBulkRequest;
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
    private final NodeStateManager nodeStateManager;
    private final Clock clock;

    @Inject
    public MultiEntityResultHandler(
        Client client,
        Settings settings,
        ThreadPool threadPool,
        AnomalyDetectionIndices anomalyDetectionIndices,
        ClientUtil clientUtil,
        IndexUtils indexUtils,
        ClusterService clusterService,
        NodeStateManager nodeStateManager,
        Clock clock
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
        this.nodeStateManager = nodeStateManager;
        this.clock = clock;
    }

    /**
     * Execute the bulk request
     * @param currentBulkRequest The bulk request
     * @param detectorId Detector Id
     */
    public void flush(ADResultBulkRequest currentBulkRequest, String detectorId) {
        if (indexUtils.checkIndicesBlocked(clusterService.state(), ClusterBlockLevel.WRITE, this.indexName)) {
            LOG.warn(String.format(Locale.ROOT, CANNOT_SAVE_ERR_MSG, detectorId));
            return;
        }

        try {
            if (!indexExists.getAsBoolean()) {
                createIndex
                    .accept(
                        ActionListener
                            .wrap(initResponse -> onCreateIndexResponse(initResponse, currentBulkRequest, detectorId), exception -> {
                                if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                                    // It is possible the index has been created while we sending the create request
                                    bulk(currentBulkRequest, detectorId);
                                } else {
                                    throw new AnomalyDetectionException(
                                        detectorId,
                                        String.format("Unexpected error creating index %s", indexName),
                                        exception
                                    );
                                }
                            })
                    );
            } else {
                bulk(currentBulkRequest, detectorId);
            }
        } catch (Exception e) {
            throw new AnomalyDetectionException(
                detectorId,
                String.format(Locale.ROOT, "Error in bulking %s for detector %s", indexName, detectorId),
                e
            );
        }
    }

    private void onCreateIndexResponse(CreateIndexResponse response, ADResultBulkRequest bulkRequest, String detectorId) {
        if (response.isAcknowledged()) {
            bulk(bulkRequest, detectorId);
        } else {
            throw new AnomalyDetectionException(detectorId, "Creating %s with mappings call not acknowledged.");
        }
    }

    private void bulk(ADResultBulkRequest currentBulkRequest, String detectorId) {
        if (currentBulkRequest.numberOfActions() <= 0) {
            return;
        }
        client
            .execute(
                ADResultBulkAction.INSTANCE,
                currentBulkRequest,
                ActionListener.<BulkResponse>wrap(response -> LOG.debug(String.format(SUCCESS_SAVING_MSG, detectorId)), exception -> {
                    LOG.error(String.format(FAIL_TO_SAVE_ERR_MSG, detectorId), exception);
                    Throwable cause = Throwables.getRootCause(exception);
                    // too much indexing pressure
                    // TODO: pause indexing a bit before trying again, ideally with randomized exponential backoff.
                    if (cause instanceof RejectedExecutionException) {
                        nodeStateManager.setLastIndexThrottledTime(clock.instant());
                    }
                })
            );
    }
}
