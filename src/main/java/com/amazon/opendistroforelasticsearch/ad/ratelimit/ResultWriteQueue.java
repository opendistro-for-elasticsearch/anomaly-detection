/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.ad.ratelimit;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.RESULT_WRITE_QUEUE_BATCH_SIZE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.RESULT_WRITE_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.NodeStateManager;
import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.transport.ADResultBulkRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.ADResultBulkResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.handler.MultiEntityResultHandler;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.ExceptionUtil;

public class ResultWriteQueue extends BatchQueue<ResultWriteRequest, ADResultBulkRequest, ADResultBulkResponse> {
    private static final Logger LOG = LogManager.getLogger(ResultWriteQueue.class);

    private final MultiEntityResultHandler resultHandler;
    private NamedXContentRegistry xContentRegistry;
    private final NodeStateManager stateManager;

    public ResultWriteQueue(
        long heapSizeInBytes,
        int singleRequestSizeInBytes,
        Setting<Float> maxHeapPercentForQueueSetting,
        ClusterService clusterService,
        Random random,
        ADCircuitBreakerService adCircuitBreakerService,
        ThreadPool threadPool,
        Settings settings,
        float maxQueuedTaskRatio,
        Clock clock,
        float mediumSegmentPruneRatio,
        float lowSegmentPruneRatio,
        int maintenanceFreqConstant,
        ClientUtil clientUtil,
        Duration executionTtl,
        MultiEntityResultHandler resultHandler,
        NamedXContentRegistry xContentRegistry,
        NodeStateManager stateManager,
        Duration stateTtl
    ) {
        super(
            "result-write",
            heapSizeInBytes,
            singleRequestSizeInBytes,
            maxHeapPercentForQueueSetting,
            clusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            settings,
            maxQueuedTaskRatio,
            clock,
            mediumSegmentPruneRatio,
            lowSegmentPruneRatio,
            maintenanceFreqConstant,
            clientUtil,
            RESULT_WRITE_QUEUE_CONCURRENCY,
            executionTtl,
            RESULT_WRITE_QUEUE_BATCH_SIZE,
            stateTtl
        );
        this.resultHandler = resultHandler;
        this.xContentRegistry = xContentRegistry;
        this.stateManager = stateManager;
    }

    @Override
    protected void executeBatchRequest(ADResultBulkRequest request, ActionListener<ADResultBulkResponse> listener) {
        if (request.numberOfActions() < 1) {
            return;
        }
        resultHandler.flush(request, listener);
    }

    @Override
    protected ADResultBulkRequest toBatchRequest(List<ResultWriteRequest> toProcess) {
        final ADResultBulkRequest bulkRequest = new ADResultBulkRequest();
        for (ResultWriteRequest request : toProcess) {
            bulkRequest.add(request.getResult());
        }
        return bulkRequest;
    }

    @Override
    protected ActionListener<ADResultBulkResponse> getResponseListener(
        List<ResultWriteRequest> toProcess,
        ADResultBulkRequest bulkRequest
    ) {
        return ActionListener.wrap(adResultBulkResponse -> {
            if (false == adResultBulkResponse.getRetryRequests().isPresent()) {
                // all successful
                return;
            }

            enqueueRetryRequestIteration(adResultBulkResponse.getRetryRequests().get(), 0);
        }, exception -> {
            if (ExceptionUtil.isOverloaded(exception)) {
                LOG.error("too many get AD model checkpoint requests or shard not avialble");
                setCoolDownStart();
            }

            if (ExceptionUtil.isRetryAble(exception)) {
                // retry all of them
                super.putAll(toProcess);
            } else {
                LOG.error("Fail to save results", exception);
            }
        });
    }

    private void enqueueRetryRequestIteration(List<IndexRequest> requestToRetry, int index) {
        if (index >= requestToRetry.size()) {
            return;
        }
        DocWriteRequest<?> currentRequest = requestToRetry.get(index);
        Optional<AnomalyResult> resultToRetry = getAnomalyResult(currentRequest);
        if (false == resultToRetry.isPresent()) {
            enqueueRetryRequestIteration(requestToRetry, index + 1);
            return;
        }
        AnomalyResult result = resultToRetry.get();
        String detectorId = result.getDetectorId();
        stateManager.getAnomalyDetector(detectorId, onGetDetector(requestToRetry, index, detectorId, result));
    }

    private ActionListener<Optional<AnomalyDetector>> onGetDetector(
        List<IndexRequest> requestToRetry,
        int index,
        String detectorId,
        AnomalyResult resultToRetry
    ) {
        return ActionListener.wrap(detectorOptional -> {
            if (false == detectorOptional.isPresent()) {
                LOG.warn(new ParameterizedMessage("AnomalyDetector [{}] is not available.", detectorId));
                enqueueRetryRequestIteration(requestToRetry, index + 1);
                return;
            }

            AnomalyDetector detector = detectorOptional.get();

            super.put(
                new ResultWriteRequest(
                    // expire based on execute start time
                    resultToRetry.getExecutionStartTime().toEpochMilli() + detector.getDetectorIntervalInMilliseconds(),
                    detectorId,
                    resultToRetry.getAnomalyGrade() > 0 ? SegmentPriority.HIGH : SegmentPriority.MEDIUM,
                    resultToRetry
                )
            );

            enqueueRetryRequestIteration(requestToRetry, index + 1);

        }, exception -> {
            LOG.error(new ParameterizedMessage("fail to get detector [{}]", detectorId), exception);
            enqueueRetryRequestIteration(requestToRetry, index + 1);
        });
    }

    private Optional<AnomalyResult> getAnomalyResult(DocWriteRequest<?> request) {
        try {
            if (false == (request instanceof IndexRequest)) {
                LOG.error(new ParameterizedMessage("We should only send IndexRquest, but get [{}].", request));
                return Optional.empty();
            }
            // we send IndexRequest previously
            IndexRequest indexRequest = (IndexRequest) request;
            BytesReference indexSource = indexRequest.source();
            XContentType indexContentType = indexRequest.getContentType();
            try (
                XContentParser xContentParser = XContentHelper
                    .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, indexSource, indexContentType)
            ) {
                // the first character is null.  Without skipping it, we get
                // org.elasticsearch.common.ParsingException: Failed to parse object: expecting token of type [START_OBJECT] but found [null]
                xContentParser.nextToken();
                return Optional.of(AnomalyResult.parse(xContentParser));
            }
        } catch (Exception e) {
            LOG.error(new ParameterizedMessage("Fail to parse index request [{}]", request), e);
        }
        return Optional.empty();
    }
}
