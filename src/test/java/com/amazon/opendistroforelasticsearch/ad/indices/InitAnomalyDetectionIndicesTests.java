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

package com.amazon.opendistroforelasticsearch.ad.indices;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.mockito.ArgumentCaptor;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorInternalState;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;

public class InitAnomalyDetectionIndicesTests extends AbstractADTest {
    Client client;
    ClusterService clusterService;
    ThreadPool threadPool;
    Settings settings;
    DiscoveryNodeFilterer nodeFilter;
    AnomalyDetectionIndices adIndices;
    ClusterName clusterName;
    ClusterState clusterState;
    IndicesAdminClient indicesClient;
    int numberOfHotNodes;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        client = mock(Client.class);
        indicesClient = mock(IndicesAdminClient.class);
        AdminClient adminClient = mock(AdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesClient);

        clusterService = mock(ClusterService.class);
        threadPool = mock(ThreadPool.class);

        numberOfHotNodes = 4;
        nodeFilter = mock(DiscoveryNodeFilterer.class);
        when(nodeFilter.getNumberOfEligibleDataNodes()).thenReturn(numberOfHotNodes);

        Settings settings = Settings.EMPTY;
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Collections
                .unmodifiableSet(
                    new HashSet<>(
                        Arrays
                            .asList(
                                AnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS,
                                AnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD,
                                AnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD,
                                AnomalyDetectorSettings.MAX_PRIMARY_SHARDS
                            )
                    )
                )
        );

        clusterName = new ClusterName("test");
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        clusterState = ClusterState.builder(clusterName).metadata(Metadata.builder().build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        adIndices = new AnomalyDetectionIndices(client, clusterService, threadPool, settings, nodeFilter);
    }

    @SuppressWarnings("unchecked")
    private void fixedPrimaryShardsIndexCreationTemplate(String index) throws IOException {
        doAnswer(invocation -> {
            CreateIndexRequest request = invocation.getArgument(0);
            assertEquals(index, request.index());

            ActionListener<CreateIndexResponse> listener = (ActionListener<CreateIndexResponse>) invocation.getArgument(1);

            listener.onResponse(new CreateIndexResponse(true, true, index));
            return null;
        }).when(indicesClient).create(any(), any());

        ActionListener<CreateIndexResponse> listener = mock(ActionListener.class);
        if (index.equals(AnomalyDetector.ANOMALY_DETECTORS_INDEX)) {
            adIndices.initAnomalyDetectorIndexIfAbsent(listener);
        } else {
            adIndices.initDetectorStateIndex(listener);
        }

        ArgumentCaptor<CreateIndexResponse> captor = ArgumentCaptor.forClass(CreateIndexResponse.class);
        verify(listener).onResponse(captor.capture());
        CreateIndexResponse result = captor.getValue();
        assertEquals(index, result.index());
    }

    @SuppressWarnings("unchecked")
    private void fixedPrimaryShardsIndexNoCreationTemplate(String index, String alias) throws IOException {
        clusterState = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(clusterState);

        RoutingTable.Builder rb = RoutingTable.builder();
        rb.addAsNew(indexMeta(index, 1L));
        when(clusterState.getRoutingTable()).thenReturn(rb.build());

        Metadata.Builder mb = Metadata.builder();
        mb.put(indexMeta(".opendistro-anomaly-results-history-2020.06.24-000003", 1L, CommonName.ANOMALY_RESULT_INDEX_ALIAS), true);

        ActionListener<CreateIndexResponse> listener = mock(ActionListener.class);
        if (index.equals(AnomalyDetector.ANOMALY_DETECTORS_INDEX)) {
            adIndices.initAnomalyDetectorIndexIfAbsent(listener);
        } else {
            adIndices.initAnomalyResultIndexIfAbsent(listener);
        }

        verify(indicesClient, never()).create(any(), any());
    }

    @SuppressWarnings("unchecked")
    private void adaptivePrimaryShardsIndexCreationTemplate(String index) throws IOException {

        doAnswer(invocation -> {
            CreateIndexRequest request = invocation.getArgument(0);
            if (index.equals(CommonName.ANOMALY_RESULT_INDEX_ALIAS)) {
                assertTrue(request.aliases().contains(new Alias(CommonName.ANOMALY_RESULT_INDEX_ALIAS)));
            } else {
                assertEquals(index, request.index());
            }

            Settings settings = request.settings();
            assertThat(settings.get("index.number_of_shards"), equalTo(Integer.toString(numberOfHotNodes)));

            ActionListener<CreateIndexResponse> listener = (ActionListener<CreateIndexResponse>) invocation.getArgument(1);

            listener.onResponse(new CreateIndexResponse(true, true, index));
            return null;
        }).when(indicesClient).create(any(), any());

        ActionListener<CreateIndexResponse> listener = mock(ActionListener.class);
        if (index.equals(AnomalyDetector.ANOMALY_DETECTORS_INDEX)) {
            adIndices.initAnomalyDetectorIndexIfAbsent(listener);
        } else if (index.equals(DetectorInternalState.DETECTOR_STATE_INDEX)) {
            adIndices.initDetectorStateIndex(listener);
        } else if (index.equals(CommonName.CHECKPOINT_INDEX_NAME)) {
            adIndices.initCheckpointIndex(listener);
        } else if (index.equals(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX)) {
            adIndices.initAnomalyDetectorJobIndex(listener);
        } else {
            adIndices.initAnomalyResultIndexIfAbsent(listener);
        }

        ArgumentCaptor<CreateIndexResponse> captor = ArgumentCaptor.forClass(CreateIndexResponse.class);
        verify(listener).onResponse(captor.capture());
        CreateIndexResponse result = captor.getValue();
        assertEquals(index, result.index());
    }

    public void testNotCreateDetector() throws IOException {
        fixedPrimaryShardsIndexNoCreationTemplate(AnomalyDetector.ANOMALY_DETECTORS_INDEX, null);
    }

    public void testNotCreateResult() throws IOException {
        fixedPrimaryShardsIndexNoCreationTemplate(AnomalyDetector.ANOMALY_DETECTORS_INDEX, null);
    }

    public void testCreateDetector() throws IOException {
        fixedPrimaryShardsIndexCreationTemplate(AnomalyDetector.ANOMALY_DETECTORS_INDEX);
    }

    public void testCreateState() throws IOException {
        fixedPrimaryShardsIndexCreationTemplate(DetectorInternalState.DETECTOR_STATE_INDEX);
    }

    public void testCreateJob() throws IOException {
        adaptivePrimaryShardsIndexCreationTemplate(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX);
    }

    public void testCreateResult() throws IOException {
        adaptivePrimaryShardsIndexCreationTemplate(CommonName.ANOMALY_RESULT_INDEX_ALIAS);
    }

    public void testCreateCheckpoint() throws IOException {
        adaptivePrimaryShardsIndexCreationTemplate(CommonName.CHECKPOINT_INDEX_NAME);
    }
}
