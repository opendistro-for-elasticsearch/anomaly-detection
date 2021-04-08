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

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.INDEX_PRESSURE_HARD_LIMIT;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.INDEX_PRESSURE_SOFT_LIMIT;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.IndexingPressure.MAX_INDEXING_BYTES;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.util.BulkUtil;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;

public class ADResultBulkTransportAction extends HandledTransportAction<ADResultBulkRequest, ADResultBulkResponse> {

    private static final Logger LOG = LogManager.getLogger(ADResultBulkTransportAction.class);
    private IndexingPressure indexingPressure;
    private final long primaryAndCoordinatingLimits;
    private float softLimit;
    private float hardLimit;
    private String indexName;
    private Client client;

    @Inject
    public ADResultBulkTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        IndexingPressure indexingPressure,
        Settings settings,
        ClusterService clusterService,
        Client client
    ) {
        super(ADResultBulkAction.NAME, transportService, actionFilters, ADResultBulkRequest::new, ThreadPool.Names.SAME);
        this.indexingPressure = indexingPressure;
        this.primaryAndCoordinatingLimits = MAX_INDEXING_BYTES.get(settings).getBytes();
        this.softLimit = INDEX_PRESSURE_SOFT_LIMIT.get(settings);
        this.hardLimit = INDEX_PRESSURE_HARD_LIMIT.get(settings);
        this.indexName = CommonName.ANOMALY_RESULT_INDEX_ALIAS;
        this.client = client;
        clusterService.getClusterSettings().addSettingsUpdateConsumer(INDEX_PRESSURE_SOFT_LIMIT, it -> softLimit = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(INDEX_PRESSURE_HARD_LIMIT, it -> hardLimit = it);
    }

    @Override
    protected void doExecute(Task task, ADResultBulkRequest request, ActionListener<ADResultBulkResponse> listener) {
        // Concurrent indexing memory limit = 10% of heap
        // indexing pressure = indexing bytes / indexing limit
        // Write all until index pressure (global indexing memory pressure) is less than 80% of 10% of heap. Otherwise, index
        // all non-zero anomaly grade index requests and index zero anomaly grade index requests with probability (1 - index pressure).
        long totalBytes = indexingPressure.getCurrentCombinedCoordinatingAndPrimaryBytes() + indexingPressure.getCurrentReplicaBytes();
        float indexingPressurePercent = (float) totalBytes / primaryAndCoordinatingLimits;

        List<AnomalyResult> results = request.getAnomalyResults();

        if (results == null || results.size() < 1) {
            listener.onResponse(new ADResultBulkResponse());
        }

        BulkRequest bulkRequest = new BulkRequest();

        if (indexingPressurePercent <= softLimit) {
            for (AnomalyResult result : results) {
                addResult(bulkRequest, result);
            }
        } else if (indexingPressurePercent <= hardLimit) {
            // exceed soft limit (60%) but smaller than hard limit (100%)
            // random seed is 42. Can be any number
            Random random = new Random(42);
            float acceptProbability = 1 - indexingPressurePercent;
            for (AnomalyResult result : results) {
                if (result.getAnomalyGrade() > 0 || random.nextFloat() < acceptProbability) {
                    addResult(bulkRequest, result);
                }
            }
        } else {
            // if exceeding hard limit, only index non-zero grade result
            for (AnomalyResult result : results) {
                if (result.getAnomalyGrade() > 0) {
                    addResult(bulkRequest, result);
                }
            }
        }

        if (bulkRequest.numberOfActions() > 0) {
            client.execute(BulkAction.INSTANCE, bulkRequest, ActionListener.<BulkResponse>wrap(bulkResponse -> {
                List<IndexRequest> failedRequests = BulkUtil.getFailedIndexRequest(bulkRequest, bulkResponse);
                listener.onResponse(new ADResultBulkResponse(failedRequests));
            }, listener::onFailure));
        } else {
            listener.onResponse(new ADResultBulkResponse());
        }
    }

    private void addResult(BulkRequest bulkRequest, AnomalyResult result) {
        try (XContentBuilder builder = jsonBuilder()) {
            IndexRequest indexRequest = new IndexRequest(indexName).source(result.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE));
            bulkRequest.add(indexRequest);
        } catch (IOException e) {
            LOG.error(String.format(Locale.ROOT, "Failed to prepare bulk %s", indexName), e);
        }
    }
}
