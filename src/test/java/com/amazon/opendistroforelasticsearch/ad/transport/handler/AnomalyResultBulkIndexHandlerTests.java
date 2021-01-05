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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.Clock;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.ADUnitTestCase;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.IndexUtils;
import com.amazon.opendistroforelasticsearch.ad.util.Throttler;
import com.amazon.opendistroforelasticsearch.ad.util.ThrowingConsumerWrapper;
import com.google.common.collect.ImmutableList;

public class AnomalyResultBulkIndexHandlerTests extends ADUnitTestCase {

    private AnomalyResultBulkIndexHandler bulkIndexHandler;
    private Client client;
    private IndexUtils indexUtils;
    private ActionListener<BulkResponse> listener;
    private AnomalyDetectionIndices anomalyDetectionIndices;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        anomalyDetectionIndices = mock(AnomalyDetectionIndices.class);
        client = mock(Client.class);
        Settings settings = Settings.EMPTY;
        Clock clock = mock(Clock.class);
        Throttler throttler = new Throttler(clock);
        ThreadPool threadpool = mock(ThreadPool.class);
        ClientUtil clientUtil = new ClientUtil(Settings.EMPTY, client, throttler, threadpool);
        indexUtils = mock(IndexUtils.class);
        ClusterService clusterService = mock(ClusterService.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        bulkIndexHandler = new AnomalyResultBulkIndexHandler(
            client,
            settings,
            threadPool,
            ThrowingConsumerWrapper.throwingConsumerWrapper(anomalyDetectionIndices::initDetectionStateIndex),
            anomalyDetectionIndices::doesDetectorStateIndexExist,
            clientUtil,
            indexUtils,
            clusterService,
            anomalyDetectionIndices
        );
        listener = spy(new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {}

            @Override
            public void onFailure(Exception e) {}
        });
    }

    public void testNullAnomalyResults() {
        bulkIndexHandler.bulkIndexAnomalyResult(null, listener);
        verify(listener, times(1)).onResponse(null);
        verify(anomalyDetectionIndices, never()).doesAnomalyDetectorIndexExist();
    }

    public void testCreateADResultIndexNotAcknowledged() throws IOException {
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onResponse(new CreateIndexResponse(false, false, ANOMALY_RESULT_INDEX_ALIAS));
            return null;
        }).when(anomalyDetectionIndices).initAnomalyResultIndexDirectly(any());
        bulkIndexHandler.bulkIndexAnomalyResult(ImmutableList.of(mock(AnomalyResult.class)), listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Creating anomaly result index with mappings call not acknowledged", exceptionCaptor.getValue().getMessage());
    }

    public void testWrongAnomalyResult() {
        BulkRequestBuilder bulkRequestBuilder = mock(BulkRequestBuilder.class);
        doReturn(bulkRequestBuilder).when(client).prepareBulk();
        doReturn(true).when(anomalyDetectionIndices).doesAnomalyResultIndexExist();
        bulkIndexHandler.bulkIndexAnomalyResult(ImmutableList.of(wrongAnomalyResult(), TestHelpers.randomAnomalyDetectResult()), listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Failed to prepare request to bulk index anomaly results", exceptionCaptor.getValue().getMessage());
    }

    public void testBulkSaveException() {
        BulkRequestBuilder bulkRequestBuilder = mock(BulkRequestBuilder.class);
        doReturn(bulkRequestBuilder).when(client).prepareBulk();
        doReturn(true).when(anomalyDetectionIndices).doesAnomalyResultIndexExist();

        String testError = randomAlphaOfLength(5);
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException(testError));
            return null;
        }).when(client).bulk(any(), any());

        bulkIndexHandler.bulkIndexAnomalyResult(ImmutableList.of(TestHelpers.randomAnomalyDetectResult()), listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals(testError, exceptionCaptor.getValue().getMessage());
    }

    private AnomalyResult wrongAnomalyResult() {
        return new AnomalyResult(
            randomAlphaOfLength(5),
            randomDouble(),
            randomDouble(),
            randomDouble(),
            null,
            null,
            null,
            null,
            null,
            randomAlphaOfLength(5),
            null,
            null,
            null
        );
    }
}
