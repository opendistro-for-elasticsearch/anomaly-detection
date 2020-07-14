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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.Iterator;
import java.util.Locale;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.IndexUtils;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;

public class AnomalyIndexHandler<T extends ToXContentObject> {
    private static final Logger LOG = LogManager.getLogger(AnomalyIndexHandler.class);

    static final String CANNOT_SAVE_ERR_MSG = "Cannot save %s due to write block.";
    static final String FAIL_TO_SAVE_ERR_MSG = "Fail to save %s: ";
    static final String RETRY_SAVING_ERR_MSG = "Retry in saving %s: ";
    static final String SUCCESS_SAVING_MSG = "Succeed in saving %s";

    protected final Client client;

    private final ThreadPool threadPool;
    private final BackoffPolicy savingBackoffPolicy;
    protected final String indexName;
    private final Consumer<ActionListener<CreateIndexResponse>> createIndex;
    private final BooleanSupplier indexExists;
    // whether save to a specific doc id or not
    private final boolean fixedDoc;
    protected final ClientUtil clientUtil;
    private final IndexUtils indexUtils;
    private final ClusterService clusterService;

    public AnomalyIndexHandler(
        Client client,
        Settings settings,
        ThreadPool threadPool,
        String indexName,
        Consumer<ActionListener<CreateIndexResponse>> createIndex,
        BooleanSupplier indexExists,
        boolean fixedDoc,
        ClientUtil clientUtil,
        IndexUtils indexUtils,
        ClusterService clusterService
    ) {
        this.client = client;
        this.threadPool = threadPool;
        this.savingBackoffPolicy = BackoffPolicy
            .exponentialBackoff(
                AnomalyDetectorSettings.BACKOFF_INITIAL_DELAY.get(settings),
                AnomalyDetectorSettings.MAX_RETRY_FOR_BACKOFF.get(settings)
            );
        this.indexName = indexName;
        this.createIndex = createIndex;
        this.indexExists = indexExists;
        this.fixedDoc = fixedDoc;
        this.clientUtil = clientUtil;
        this.indexUtils = indexUtils;
        this.clusterService = clusterService;
    }

    public void index(T toSave, String detectorId) {
        if (indexUtils.checkIndicesBlocked(clusterService.state(), ClusterBlockLevel.WRITE, this.indexName)) {
            LOG.warn(String.format(Locale.ROOT, CANNOT_SAVE_ERR_MSG, detectorId));
            return;
        }

        try {
            if (!indexExists.getAsBoolean()) {
                createIndex
                    .accept(ActionListener.wrap(initResponse -> onCreateIndexResponse(initResponse, toSave, detectorId), exception -> {
                        if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                            // It is possible the index has been created while we sending the create request
                            save(toSave, detectorId);
                        } else {
                            throw new AnomalyDetectionException(
                                detectorId,
                                String.format("Unexpected error creating index %s", indexName),
                                exception
                            );
                        }
                    }));
            } else {
                save(toSave, detectorId);
            }
        } catch (Exception e) {
            throw new AnomalyDetectionException(
                detectorId,
                String.format(Locale.ROOT, "Error in saving %s for detector %s", indexName, detectorId),
                e
            );
        }
    }

    private void onCreateIndexResponse(CreateIndexResponse response, T toSave, String detectorId) {
        if (response.isAcknowledged()) {
            save(toSave, detectorId);
        } else {
            throw new AnomalyDetectionException(detectorId, "Creating %s with mappings call not acknowledged.");
        }
    }

    protected void save(T toSave, String detectorId) {
        try (XContentBuilder builder = jsonBuilder()) {
            IndexRequest indexRequest = new IndexRequest(indexName).source(toSave.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE));
            if (fixedDoc) {
                indexRequest.id(detectorId);
            }

            saveIteration(indexRequest, detectorId, savingBackoffPolicy.iterator());
        } catch (Exception e) {
            LOG.error(String.format("Failed to save %s", indexName), e);
            throw new AnomalyDetectionException(detectorId, String.format("Cannot save %s", indexName));
        }
    }

    void saveIteration(IndexRequest indexRequest, String detectorId, Iterator<TimeValue> backoff) {
        clientUtil
            .<IndexRequest, IndexResponse>asyncRequest(
                indexRequest,
                client::index,
                ActionListener.<IndexResponse>wrap(response -> { LOG.debug(String.format(SUCCESS_SAVING_MSG, detectorId)); }, exception -> {
                    // Elasticsearch has a thread pool and a queue for write per node. A thread
                    // pool will have N number of workers ready to handle the requests. When a
                    // request comes and if a worker is free , this is handled by the worker. Now by
                    // default the number of workers is equal to the number of cores on that CPU.
                    // When the workers are full and there are more write requests, the request
                    // will go to queue. The size of queue is also limited. If by default size is,
                    // say, 200 and if there happens more parallel requests than this, then those
                    // requests would be rejected as you can see EsRejectedExecutionException.
                    // So EsRejectedExecutionException is the way that Elasticsearch tells us that
                    // it cannot keep up with the current indexing rate.
                    // When it happens, we should pause indexing a bit before trying again, ideally
                    // with randomized exponential backoff.
                    Throwable cause = ExceptionsHelper.unwrapCause(exception);
                    if (!(cause instanceof EsRejectedExecutionException) || !backoff.hasNext()) {
                        LOG.error(String.format(FAIL_TO_SAVE_ERR_MSG, detectorId), cause);
                    } else {
                        TimeValue nextDelay = backoff.next();
                        LOG.warn(String.format(RETRY_SAVING_ERR_MSG, detectorId), cause);
                        // copy original request's source without other information like autoGeneratedTimestamp
                        // otherwise, an exception will be thrown indicating autoGeneratedTimestamp should not be set
                        // while request id is already set (id is set because we have already sent the request before).
                        IndexRequest newReuqest = new IndexRequest(indexRequest.index());
                        newReuqest.source(indexRequest.source(), indexRequest.getContentType());
                        threadPool.schedule(() -> saveIteration(newReuqest, detectorId, backoff), nextDelay, ThreadPool.Names.SAME);
                    }
                })
            );
    }
}
