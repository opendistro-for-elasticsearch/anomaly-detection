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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;

public class ADResultBulkTransportActionTests extends AbstractADTest {
    private ADResultBulkTransportAction resultBulk;
    private TransportService transportService;
    private ClusterService clusterService;
    private IndexingPressure indexingPressure;
    private Client client;

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(AnomalyResultTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings
            .builder()
            .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "1KB")
            .put(AnomalyDetectorSettings.INDEX_PRESSURE_SOFT_LIMIT.getKey(), 0.8)
            .build();
        setupTestNodes(settings);
        transportService = testNodes[0].transportService;
        clusterService = spy(testNodes[0].clusterService);
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.INDEX_PRESSURE_SOFT_LIMIT)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        ActionFilters actionFilters = mock(ActionFilters.class);
        indexingPressure = mock(IndexingPressure.class);

        client = mock(Client.class);

        resultBulk = new ADResultBulkTransportAction(transportService, actionFilters, indexingPressure, settings, clusterService, client);
    }

    @Override
    @After
    public final void tearDown() throws Exception {
        tearDownTestNodes();
        super.tearDown();
    }

    @SuppressWarnings("unchecked")
    public void testSendAll() {
        when(indexingPressure.getCurrentCombinedCoordinatingAndPrimaryBytes()).thenReturn(0L);
        when(indexingPressure.getCurrentReplicaBytes()).thenReturn(0L);

        ADResultBulkRequest originalRequest = new ADResultBulkRequest();
        originalRequest.add(TestHelpers.randomMultiEntityAnomalyDetectResult(0.8d, 0d));
        originalRequest.add(TestHelpers.randomMultiEntityAnomalyDetectResult(8d, 0.2d));

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length == 3);

            assertTrue(args[1] instanceof BulkRequest);
            assertTrue(args[2] instanceof ActionListener);
            BulkRequest request = (BulkRequest) args[1];
            ActionListener<BulkResponse> listener = (ActionListener<BulkResponse>) args[2];

            assertEquals(2, request.requests().size());
            listener.onResponse(null);
            return null;
        }).when(client).execute(any(), any(), any());

        PlainActionFuture<BulkResponse> future = PlainActionFuture.newFuture();
        resultBulk.doExecute(null, originalRequest, future);

        future.actionGet();
    }

    @SuppressWarnings("unchecked")
    public void testSendPartial() {
        // the limit is 1024 Bytes
        when(indexingPressure.getCurrentCombinedCoordinatingAndPrimaryBytes()).thenReturn(1000L);
        when(indexingPressure.getCurrentReplicaBytes()).thenReturn(24L);

        ADResultBulkRequest originalRequest = new ADResultBulkRequest();
        originalRequest.add(TestHelpers.randomMultiEntityAnomalyDetectResult(0.8d, 0d));
        originalRequest.add(TestHelpers.randomMultiEntityAnomalyDetectResult(8d, 0.2d));

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length == 3);

            assertTrue(args[1] instanceof BulkRequest);
            assertTrue(args[2] instanceof ActionListener);
            BulkRequest request = (BulkRequest) args[1];
            ActionListener<BulkResponse> listener = (ActionListener<BulkResponse>) args[2];

            assertEquals(1, request.requests().size());
            listener.onResponse(null);
            return null;
        }).when(client).execute(any(), any(), any());

        PlainActionFuture<BulkResponse> future = PlainActionFuture.newFuture();
        resultBulk.doExecute(null, originalRequest, future);

        future.actionGet();
    }

    @SuppressWarnings("unchecked")
    public void testSendRandomPartial() {
        // 400 + 421 > 1024 * 0.8. 1024 is 1KB, our INDEX_PRESSURE_SOFT_LIMIT
        when(indexingPressure.getCurrentCombinedCoordinatingAndPrimaryBytes()).thenReturn(400L);
        when(indexingPressure.getCurrentReplicaBytes()).thenReturn(421L);

        ADResultBulkRequest originalRequest = new ADResultBulkRequest();
        for (int i = 0; i < 1000; i++) {
            originalRequest.add(TestHelpers.randomMultiEntityAnomalyDetectResult(0.8d, 0d));
        }

        originalRequest.add(TestHelpers.randomMultiEntityAnomalyDetectResult(8d, 0.2d));

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length == 3);

            assertTrue(args[1] instanceof BulkRequest);
            assertTrue(args[2] instanceof ActionListener);
            BulkRequest request = (BulkRequest) args[1];
            ActionListener<BulkResponse> listener = (ActionListener<BulkResponse>) args[2];

            int size = request.requests().size();
            assertTrue(1 < size);
            // at least 1 half should be removed
            assertTrue(String.format("size is actually %d", size), size < 500);
            listener.onResponse(null);
            return null;
        }).when(client).execute(any(), any(), any());

        PlainActionFuture<BulkResponse> future = PlainActionFuture.newFuture();
        resultBulk.doExecute(null, originalRequest, future);

        future.actionGet();
    }

    public void testSerialzationRequest() throws IOException {
        ADResultBulkRequest request = new ADResultBulkRequest();
        request.add(TestHelpers.randomMultiEntityAnomalyDetectResult(0.8d, 0d));
        request.add(TestHelpers.randomMultiEntityAnomalyDetectResult(8d, 0.2d));
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        ADResultBulkRequest readRequest = new ADResultBulkRequest(streamInput);
        assertThat(2, equalTo(readRequest.numberOfActions()));
    }

    public void testValidateRequest() {
        ActionRequestValidationException e = new ADResultBulkRequest().validate();
        assertThat(e.validationErrors(), hasItem(ADResultBulkRequest.NO_REQUESTS_ADDED_ERR));
    }
}
