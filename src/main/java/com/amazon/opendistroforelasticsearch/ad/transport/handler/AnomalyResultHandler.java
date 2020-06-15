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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;

public class AnomalyResultHandler {
    private static final Logger LOG = LogManager.getLogger(AnomalyResultHandler.class);

    static final String CANNOT_SAVE_ERR_MSG = "Cannot save anomaly result due to write block.";
    static final String FAIL_TO_SAVE_ERR_MSG = "Fail to save anomaly index: ";
    static final String RETRY_SAVING_ERR_MSG = "Retry in saving anomaly index: ";
    static final String SUCCESS_SAVING_MSG = "SSUCCESS_SAVING_MSGuccess in saving anomaly index: ";

    private final Client client;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final ThreadPool threadPool;
    private final BackoffPolicy resultSavingBackoffPolicy;

    public AnomalyResultHandler(
        Client client,
        Settings settings,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        AnomalyDetectionIndices anomalyDetectionIndices,
        ThreadPool threadPool
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.threadPool = threadPool;
        this.resultSavingBackoffPolicy = BackoffPolicy
            .exponentialBackoff(
                AnomalyDetectorSettings.BACKOFF_INITIAL_DELAY.get(settings),
                AnomalyDetectorSettings.MAX_RETRY_FOR_BACKOFF.get(settings)
            );
    }

    public void indexAnomalyResult(AnomalyResult anomalyResult) {
        try {
            if (checkIndicesBlocked(clusterService.state(), ClusterBlockLevel.WRITE, AnomalyResult.ANOMALY_RESULT_INDEX)) {
                LOG.warn(CANNOT_SAVE_ERR_MSG);
                return;
            }
            if (!anomalyDetectionIndices.doesAnomalyResultIndexExist()) {
                anomalyDetectionIndices
                    .initAnomalyResultIndexDirectly(
                        ActionListener.wrap(initResponse -> onCreateAnomalyResultIndexResponse(initResponse, anomalyResult), exception -> {
                            if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                                // It is possible the index has been created while we sending the create request
                                saveDetectorResult(anomalyResult);
                            } else {
                                throw new AnomalyDetectionException(
                                    anomalyResult.getDetectorId(),
                                    "Unexpected error creating anomaly result index",
                                    exception
                                );
                            }
                        })
                    );
            } else {
                saveDetectorResult(anomalyResult);
            }
        } catch (Exception e) {
            throw new AnomalyDetectionException(
                anomalyResult.getDetectorId(),
                String
                    .format(
                        Locale.ROOT,
                        "Error in saving anomaly index for ID %s from %s to %s",
                        anomalyResult.getDetectorId(),
                        anomalyResult.getDataStartTime(),
                        anomalyResult.getDataEndTime()
                    ),
                e
            );
        }
    }

    /**
     * Similar to checkGlobalBlock, we check block on the indices level.
     *
     * @param state   Cluster state
     * @param level   block level
     * @param indices the indices on which to check block
     * @return whether any of the index has block on the level.
     */
    private boolean checkIndicesBlocked(ClusterState state, ClusterBlockLevel level, String... indices) {
        // the original index might be an index expression with wildcards like "log*",
        // so we need to expand the expression to concrete index name
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, IndicesOptions.lenientExpandOpen(), indices);

        return state.blocks().indicesBlockedException(level, concreteIndices) != null;
    }

    private void onCreateAnomalyResultIndexResponse(CreateIndexResponse response, AnomalyResult anomalyResult) {
        if (response.isAcknowledged()) {
            saveDetectorResult(anomalyResult);
        } else {
            throw new AnomalyDetectionException(
                anomalyResult.getDetectorId(),
                "Creating anomaly result index with mappings call not acknowledged."
            );
        }
    }

    private void saveDetectorResult(AnomalyResult anomalyResult) {
        try (XContentBuilder builder = jsonBuilder()) {
            IndexRequest indexRequest = new IndexRequest(AnomalyResult.ANOMALY_RESULT_INDEX)
                .source(anomalyResult.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE));
            saveDetectorResult(
                indexRequest,
                String
                    .format(
                        Locale.ROOT,
                        "ID %s from %s to %s",
                        anomalyResult.getDetectorId(),
                        anomalyResult.getDataStartTime(),
                        anomalyResult.getDataEndTime()
                    ),
                resultSavingBackoffPolicy.iterator()
            );
        } catch (Exception e) {
            LOG.error("Failed to save anomaly result", e);
            throw new AnomalyDetectionException(anomalyResult.getDetectorId(), "Cannot save result");
        }
    }

    void saveDetectorResult(IndexRequest indexRequest, String context, Iterator<TimeValue> backoff) {
        client.index(indexRequest, ActionListener.<IndexResponse>wrap(response -> LOG.debug(SUCCESS_SAVING_MSG + context), exception -> {
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
                LOG.error(FAIL_TO_SAVE_ERR_MSG + context, cause);
            } else {
                TimeValue nextDelay = backoff.next();
                LOG.warn(RETRY_SAVING_ERR_MSG + context, cause);
                // copy original request's source without other information like autoGeneratedTimestamp
                // otherwise, an exception will be thrown indicating autoGeneratedTimestamp should not be set
                // while request id is already set (id is set because we have already sent the request before).
                IndexRequest newReuqest = new IndexRequest(AnomalyResult.ANOMALY_RESULT_INDEX);
                newReuqest.source(indexRequest.source(), indexRequest.getContentType());
                threadPool.schedule(() -> saveDetectorResult(newReuqest, context, backoff), nextDelay, ThreadPool.Names.SAME);
            }
        }));
    }
}
