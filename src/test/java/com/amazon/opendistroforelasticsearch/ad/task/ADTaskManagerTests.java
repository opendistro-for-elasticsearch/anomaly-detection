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

package com.amazon.opendistroforelasticsearch.ad.task;

import static com.amazon.opendistroforelasticsearch.ad.TestHelpers.randomDetector;
import static com.amazon.opendistroforelasticsearch.ad.TestHelpers.randomFeature;
import static com.amazon.opendistroforelasticsearch.ad.TestHelpers.randomUser;
import static com.amazon.opendistroforelasticsearch.ad.constant.CommonName.ANOMALY_RESULT_INDEX_ALIAS;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;

import com.amazon.opendistroforelasticsearch.ad.ADUnitTestCase;
import com.amazon.opendistroforelasticsearch.ad.cluster.HashRing;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.DetectionDateRange;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyDetectorJobResponse;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;
import com.google.common.collect.ImmutableList;

public class ADTaskManagerTests extends ADUnitTestCase {

    private Settings settings;
    private Client client;
    private ClusterService clusterService;
    private ClusterSettings clusterSettings;
    private DiscoveryNodeFilterer nodeFilter;
    private AnomalyDetectionIndices anomalyDetectionIndices;
    private ADTaskCacheManager adTaskCacheManager;
    private HashRing hashRing;
    private ADTaskManager adTaskManager;

    private Instant startTime;
    private Instant endTime;
    private ActionListener<AnomalyDetectorJobResponse> listener;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Instant now = Instant.now();
        startTime = now.minus(10, ChronoUnit.DAYS);
        endTime = now.minus(1, ChronoUnit.DAYS);

        settings = Settings
            .builder()
            .put(MAX_OLD_AD_TASK_DOCS_PER_DETECTOR.getKey(), 2)
            .put(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1)
            .build();

        clusterSettings = clusterSetting(settings, MAX_OLD_AD_TASK_DOCS_PER_DETECTOR, BATCH_TASK_PIECE_INTERVAL_SECONDS);

        clusterService = new ClusterService(settings, clusterSettings, null);

        client = mock(Client.class);
        nodeFilter = mock(DiscoveryNodeFilterer.class);
        anomalyDetectionIndices = mock(AnomalyDetectionIndices.class);
        adTaskCacheManager = mock(ADTaskCacheManager.class);
        hashRing = mock(HashRing.class);

        adTaskManager = new ADTaskManager(
            settings,
            clusterService,
            client,
            NamedXContentRegistry.EMPTY,
            anomalyDetectionIndices,
            nodeFilter,
            hashRing,
            adTaskCacheManager
        );

        listener = spy(new ActionListener<AnomalyDetectorJobResponse>() {
            @Override
            public void onResponse(AnomalyDetectorJobResponse bulkItemResponses) {}

            @Override
            public void onFailure(Exception e) {}
        });
    }

    public void testCreateTaskIndexNotAcknowledged() throws IOException {
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onResponse(new CreateIndexResponse(false, false, ANOMALY_RESULT_INDEX_ALIAS));
            return null;
        }).when(anomalyDetectionIndices).initDetectionStateIndex(any());
        AnomalyDetector detector = randomDetector(
            new DetectionDateRange(startTime, endTime),
            ImmutableList.of(randomFeature(true)),
            randomAlphaOfLength(5),
            1,
            randomAlphaOfLength(5)
        );

        adTaskManager.startHistoricalDetector(detector, randomUser(), listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals(
            "Create index .opendistro-anomaly-detection-state with mappings not acknowledged",
            exceptionCaptor.getValue().getMessage()
        );
    }

    public void testCreateTaskIndexWithResourceAlreadyExistsException() throws IOException {
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new ResourceAlreadyExistsException("index created"));
            return null;
        }).when(anomalyDetectionIndices).initDetectionStateIndex(any());
        AnomalyDetector detector = randomDetector(
            new DetectionDateRange(startTime, endTime),
            ImmutableList.of(randomFeature(true)),
            randomAlphaOfLength(5),
            1,
            randomAlphaOfLength(5)
        );

        adTaskManager.startHistoricalDetector(detector, randomUser(), listener);
        verify(listener, never()).onFailure(any());
    }

    public void testCreateTaskIndexWithException() throws IOException {
        String error = randomAlphaOfLength(5);
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new RuntimeException(error));
            return null;
        }).when(anomalyDetectionIndices).initDetectionStateIndex(any());
        AnomalyDetector detector = randomDetector(
            new DetectionDateRange(startTime, endTime),
            ImmutableList.of(randomFeature(true)),
            randomAlphaOfLength(5),
            1,
            randomAlphaOfLength(5)
        );

        adTaskManager.startHistoricalDetector(detector, randomUser(), listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals(error, exceptionCaptor.getValue().getMessage());
    }
}
