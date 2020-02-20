/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.IntStream;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.Throttler;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.After;
import org.junit.Before;

import com.amazon.randomcutforest.RandomCutForest;

public class ADStateManagerTests extends ESTestCase {
    private ADStateManager stateManager;
    private ModelManager modelManager;
    private Client client;
    private Clock clock;
    private Duration duration;
    private Throttler throttler;

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        modelManager = mock(ModelManager.class);
        when(modelManager.getPartitionedForestSizes(any(RandomCutForest.class), any(String.class)))
            .thenReturn(new SimpleImmutableEntry<>(2, 20));
        client = mock(Client.class);
        Settings settings = Settings
            .builder()
            .put("ml.anomaly_detectors.max_retry_for_unresponsive_node", 3)
            .put("ml.anomaly_detectors.ad_mute_minutes", TimeValue.timeValueMinutes(10))
            .build();
        clock = mock(Clock.class);
        duration = Duration.ofHours(1);
        throttler = new Throttler(clock);
        stateManager = new ADStateManager(
            client,
            xContentRegistry(),
            modelManager,
            settings,
            new ClientUtil(settings, client, throttler),
            clock,
            duration
        );

    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        stateManager = null;
        modelManager = null;
        client = null;
    }

    @SuppressWarnings("unchecked")
    private String setupDetector(boolean responseExists) throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null);
        XContentBuilder content = detector.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length >= 2);

            GetRequest request = null;
            ActionListener<GetResponse> listener = null;
            if (args[0] instanceof GetRequest) {
                request = (GetRequest) args[0];
            }
            if (args[1] instanceof ActionListener) {
                listener = (ActionListener<GetResponse>) args[1];
            }

            assertTrue(request != null && listener != null);
            listener
                .onResponse(
                    new GetResponse(
                        new GetResult(
                            AnomalyDetector.ANOMALY_DETECTORS_INDEX,
                            MapperService.SINGLE_MAPPING_NAME,
                            detector.getDetectorId(),
                            UNASSIGNED_SEQ_NO,
                            0,
                            -1,
                            responseExists,
                            BytesReference.bytes(content),
                            Collections.emptyMap()
                        )
                    )
                );

            return null;
        }).when(client).get(any(), any());
        return detector.getDetectorId();
    }

    public void testGetPartitionNumber() throws IOException, InterruptedException {
        String detectorId = setupDetector(true);
        int partitionNumber = stateManager.getPartitionNumber(detectorId);
        assertEquals(2, partitionNumber);
    }

    public void testGetResponseNotFound() throws IOException, InterruptedException {
        String detectorId = setupDetector(false);
        expectThrows(AnomalyDetectionException.class, () -> stateManager.getPartitionNumber(detectorId));
    }

    public void testShouldMute() {
        String nodeId = "123";
        assertTrue(!stateManager.isMuted(nodeId));

        when(clock.millis()).thenReturn(10000L);
        IntStream.range(0, 4).forEach(j -> stateManager.addPressure(nodeId));

        when(clock.millis()).thenReturn(20000L);
        assertTrue(stateManager.isMuted(nodeId));

        // > 15 minutes have passed, we should not mute anymore
        when(clock.millis()).thenReturn(1000001L);
        assertTrue(!stateManager.isMuted(nodeId));

        // the backpressure counter should be reset
        when(clock.millis()).thenReturn(100001L);
        stateManager.resetBackpressureCounter(nodeId);
        assertTrue(!stateManager.isMuted(nodeId));
    }

    public void testMaintenanceDoNothing() {
        stateManager.maintenance();

        verifyZeroInteractions(clock);
    }

    public void testMaintenanceNotRemove() throws IOException {
        ConcurrentHashMap<String, Entry<AnomalyDetector, Instant>> states = new ConcurrentHashMap<>();
        when(clock.instant()).thenReturn(Instant.MIN);
        states.put("123", new SimpleImmutableEntry<>(TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null), Instant.MAX));
        stateManager.maintenance(states);
        assertEquals(1, states.size());

    }

    public void testMaintenancRemove() throws IOException {
        ConcurrentHashMap<String, Entry<AnomalyDetector, Instant>> states = new ConcurrentHashMap<>();
        when(clock.instant()).thenReturn(Instant.MAX);
        states.put("123", new SimpleImmutableEntry<>(TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null), Instant.MIN));
        stateManager.maintenance(states);
        assertEquals(0, states.size());

    }

    public void testHasRunningQuery() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of(), null);
        SearchRequest dummySearchRequest = new SearchRequest();
        assertFalse(stateManager.hasRunningQuery(detector));
        throttler.insertFilteredQuery(detector.getDetectorId(), dummySearchRequest);
        assertTrue(stateManager.hasRunningQuery(detector));
    }
}
