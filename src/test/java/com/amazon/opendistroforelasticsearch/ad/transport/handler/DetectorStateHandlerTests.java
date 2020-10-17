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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.NodeStateManager;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorInternalState;
import com.amazon.opendistroforelasticsearch.ad.transport.handler.DetectionStateHandler.ErrorStrategy;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.IndexUtils;
import com.amazon.opendistroforelasticsearch.ad.util.Throttler;
import com.amazon.opendistroforelasticsearch.ad.util.ThrowingConsumerWrapper;

public class DetectorStateHandlerTests extends ESTestCase {
    private DetectionStateHandler detectorStateHandler;
    private String detectorId = "123";
    private Client client;
    private String error = "Stopped due to blah";
    private IndexUtils indexUtils;
    private NodeStateManager stateManager;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        AnomalyDetectionIndices anomalyDetectionIndices = mock(AnomalyDetectionIndices.class);
        client = mock(Client.class);
        Settings settings = Settings.EMPTY;
        Clock clock = mock(Clock.class);
        Throttler throttler = new Throttler(clock);
        ThreadPool threadpool = mock(ThreadPool.class);
        ClientUtil clientUtil = new ClientUtil(Settings.EMPTY, client, throttler, threadpool);
        indexUtils = mock(IndexUtils.class);
        ClusterService clusterService = mock(ClusterService.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        stateManager = mock(NodeStateManager.class);
        detectorStateHandler = new DetectionStateHandler(
            client,
            settings,
            threadPool,
            ThrowingConsumerWrapper.throwingConsumerWrapper(anomalyDetectionIndices::initDetectorStateIndex),
            anomalyDetectionIndices::doesDetectorStateIndexExist,
            clientUtil,
            indexUtils,
            clusterService,
            NamedXContentRegistry.EMPTY,
            stateManager
        );
    }

    public void testNullState() {
        ErrorStrategy errorStrategy = detectorStateHandler.new ErrorStrategy(error);
        DetectorInternalState state = errorStrategy.createNewState(null);
        assertEquals(error, state.getError());
        assertTrue(state.getLastUpdateTime() != null);
    }

    public void testNonNullState() {
        String error = "blah";
        DetectorInternalState oldState = new DetectorInternalState.Builder().error(error).lastUpdateTime(Instant.ofEpochSecond(1L)).build();
        ErrorStrategy errorStrategy = detectorStateHandler.new ErrorStrategy(error);
        DetectorInternalState state = errorStrategy.createNewState(oldState);
        assertEquals(null, state);
    }

    public void testOldErrorNull() {
        ErrorStrategy errorStrategy = detectorStateHandler.new ErrorStrategy(error);
        // old state's error is null
        DetectorInternalState state = errorStrategy.createNewState(new DetectorInternalState.Builder().build());
        assertEquals(error, state.getError());
        assertTrue(state.getLastUpdateTime() != null);
    }

    public void testBothErrorNull() {
        ErrorStrategy errorStrategy = detectorStateHandler.new ErrorStrategy(null);
        // old state's error is null
        DetectorInternalState state = errorStrategy.createNewState(new DetectorInternalState.Builder().build());
        assertEquals(null, state);
    }

    public void testNoUpdateWitoutErrorChange() {
        when(stateManager.getLastDetectionError(anyString())).thenReturn(error);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            @SuppressWarnings("unchecked")
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];
            DetectorInternalState.Builder result = new DetectorInternalState.Builder().lastUpdateTime(Instant.now()).error(error);
            listener.onResponse(TestHelpers.createGetResponse(result.build(), detectorId, DetectorInternalState.DETECTOR_STATE_INDEX));
            return null;
        }).when(client).get(any(), any());

        detectorStateHandler.saveError(error, detectorId);

        verify(indexUtils, never()).checkIndicesBlocked(any(), any(), any());
    }

    public void testUpdateWithErrorChange() {
        when(stateManager.getLastDetectionError(anyString())).thenReturn("blah");
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            @SuppressWarnings("unchecked")
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];
            DetectorInternalState.Builder result = new DetectorInternalState.Builder().lastUpdateTime(Instant.now()).error("blah");
            listener.onResponse(TestHelpers.createGetResponse(result.build(), detectorId, DetectorInternalState.DETECTOR_STATE_INDEX));
            return null;
        }).when(client).get(any(), any());

        detectorStateHandler.saveError(error, detectorId);

        verify(indexUtils, times(1)).checkIndicesBlocked(any(), any(), any());
    }

    public void testUpdateWithFirstChange() {
        when(stateManager.getLastDetectionError(anyString())).thenReturn(NodeStateManager.NO_ERROR);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            @SuppressWarnings("unchecked")
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];
            DetectorInternalState.Builder result = new DetectorInternalState.Builder()
                .lastUpdateTime(Instant.ofEpochMilli(1))
                .error("blah");
            listener.onResponse(TestHelpers.createGetResponse(result.build(), detectorId, DetectorInternalState.DETECTOR_STATE_INDEX));
            return null;
        }).when(client).get(any(), any());

        detectorStateHandler.saveError(error, detectorId);

        verify(indexUtils, times(1)).checkIndicesBlocked(any(), any(), any());
    }
}
