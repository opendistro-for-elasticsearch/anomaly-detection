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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.rollover.Condition;
import org.elasticsearch.action.admin.indices.rollover.MaxDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;

public class RolloverTests extends AbstractADTest {
    private AnomalyDetectionIndices adIndices;
    private IndicesAdminClient indicesClient;
    private ClusterAdminClient clusterAdminClient;
    private ClusterName clusterName;
    private ClusterState clusterState;
    private ClusterService clusterService;
    private long defaultMaxDocs;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Client client = mock(Client.class);
        indicesClient = mock(IndicesAdminClient.class);
        AdminClient adminClient = mock(AdminClient.class);
        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
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

        ThreadPool threadPool = mock(ThreadPool.class);
        Settings settings = Settings.EMPTY;
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesClient);

        DiscoveryNodeFilterer nodeFilter = mock(DiscoveryNodeFilterer.class);

        adIndices = new AnomalyDetectionIndices(client, clusterService, threadPool, settings, nodeFilter);

        clusterAdminClient = mock(ClusterAdminClient.class);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);

        doAnswer(invocation -> {
            ClusterStateRequest clusterStateRequest = invocation.getArgument(0);
            assertEquals(AnomalyDetectionIndices.ALL_AD_RESULTS_INDEX_PATTERN, clusterStateRequest.indices()[0]);
            @SuppressWarnings("unchecked")
            ActionListener<ClusterStateResponse> listener = (ActionListener<ClusterStateResponse>) invocation.getArgument(1);
            listener.onResponse(new ClusterStateResponse(clusterName, clusterState, true));
            return null;
        }).when(clusterAdminClient).state(any(), any());

        defaultMaxDocs = AnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS.getDefault(Settings.EMPTY);
    }

    private void assertRolloverRequest(RolloverRequest request) {
        assertEquals(CommonName.ANOMALY_RESULT_INDEX_ALIAS, request.indices()[0]);

        Map<String, Condition<?>> conditions = request.getConditions();
        assertEquals(1, conditions.size());
        assertEquals(new MaxDocsCondition(defaultMaxDocs), conditions.get(MaxDocsCondition.NAME));

        CreateIndexRequest createIndexRequest = request.getCreateIndexRequest();
        assertEquals(AnomalyDetectionIndices.AD_RESULT_HISTORY_INDEX_PATTERN, createIndexRequest.index());
        assertTrue(createIndexRequest.mappings().get(CommonName.MAPPING_TYPE).contains("data_start_time"));
    }

    public void testNotRolledOver() {
        doAnswer(invocation -> {
            RolloverRequest request = invocation.getArgument(0);
            assertRolloverRequest(request);

            @SuppressWarnings("unchecked")
            ActionListener<RolloverResponse> listener = (ActionListener<RolloverResponse>) invocation.getArgument(1);

            listener.onResponse(new RolloverResponse(null, null, Collections.emptyMap(), request.isDryRun(), false, true, true));
            return null;
        }).when(indicesClient).rolloverIndex(any(), any());

        Metadata.Builder metaBuilder = Metadata
            .builder()
            .put(indexMeta(".opendistro-anomaly-results-history-2020.06.24-000003", 1L, CommonName.ANOMALY_RESULT_INDEX_ALIAS), true);
        clusterState = ClusterState.builder(clusterName).metadata(metaBuilder.build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        adIndices.rolloverAndDeleteHistoryIndex();
        verify(clusterAdminClient, never()).state(any(), any());
        verify(indicesClient, times(1)).rolloverIndex(any(), any());
    }

    public void testRolledOverButNotDeleted() {
        doAnswer(invocation -> {
            RolloverRequest request = invocation.getArgument(0);
            @SuppressWarnings("unchecked")
            ActionListener<RolloverResponse> listener = (ActionListener<RolloverResponse>) invocation.getArgument(1);

            assertEquals(CommonName.ANOMALY_RESULT_INDEX_ALIAS, request.indices()[0]);

            Map<String, Condition<?>> conditions = request.getConditions();
            assertEquals(1, conditions.size());
            assertEquals(new MaxDocsCondition(defaultMaxDocs), conditions.get(MaxDocsCondition.NAME));

            CreateIndexRequest createIndexRequest = request.getCreateIndexRequest();
            assertEquals(AnomalyDetectionIndices.AD_RESULT_HISTORY_INDEX_PATTERN, createIndexRequest.index());
            assertTrue(createIndexRequest.mappings().get(CommonName.MAPPING_TYPE).contains("data_start_time"));
            listener.onResponse(new RolloverResponse(null, null, Collections.emptyMap(), request.isDryRun(), true, true, true));
            return null;
        }).when(indicesClient).rolloverIndex(any(), any());

        Metadata.Builder metaBuilder = Metadata
            .builder()
            .put(indexMeta(".opendistro-anomaly-results-history-2020.06.24-000003", 1L, CommonName.ANOMALY_RESULT_INDEX_ALIAS), true)
            .put(
                indexMeta(
                    ".opendistro-anomaly-results-history-2020.06.24-000004",
                    Instant.now().toEpochMilli(),
                    CommonName.ANOMALY_RESULT_INDEX_ALIAS
                ),
                true
            );
        clusterState = ClusterState.builder(clusterName).metadata(metaBuilder.build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        adIndices.rolloverAndDeleteHistoryIndex();
        verify(clusterAdminClient, times(1)).state(any(), any());
        verify(indicesClient, times(1)).rolloverIndex(any(), any());
        verify(indicesClient, never()).delete(any(), any());
    }

    public void testRolledOverDeleted() {
        doAnswer(invocation -> {
            RolloverRequest request = invocation.getArgument(0);
            @SuppressWarnings("unchecked")
            ActionListener<RolloverResponse> listener = (ActionListener<RolloverResponse>) invocation.getArgument(1);

            assertEquals(CommonName.ANOMALY_RESULT_INDEX_ALIAS, request.indices()[0]);

            Map<String, Condition<?>> conditions = request.getConditions();
            assertEquals(1, conditions.size());
            assertEquals(new MaxDocsCondition(defaultMaxDocs), conditions.get(MaxDocsCondition.NAME));

            CreateIndexRequest createIndexRequest = request.getCreateIndexRequest();
            assertEquals(AnomalyDetectionIndices.AD_RESULT_HISTORY_INDEX_PATTERN, createIndexRequest.index());
            assertTrue(createIndexRequest.mappings().get(CommonName.MAPPING_TYPE).contains("data_start_time"));
            listener.onResponse(new RolloverResponse(null, null, Collections.emptyMap(), request.isDryRun(), true, true, true));
            return null;
        }).when(indicesClient).rolloverIndex(any(), any());

        Metadata.Builder metaBuilder = Metadata
            .builder()
            .put(indexMeta(".opendistro-anomaly-results-history-2020.06.24-000002", 1L, CommonName.ANOMALY_RESULT_INDEX_ALIAS), true)
            .put(indexMeta(".opendistro-anomaly-results-history-2020.06.24-000003", 2L, CommonName.ANOMALY_RESULT_INDEX_ALIAS), true)
            .put(
                indexMeta(
                    ".opendistro-anomaly-results-history-2020.06.24-000004",
                    Instant.now().toEpochMilli(),
                    CommonName.ANOMALY_RESULT_INDEX_ALIAS
                ),
                true
            );
        clusterState = ClusterState.builder(clusterName).metadata(metaBuilder.build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        adIndices.rolloverAndDeleteHistoryIndex();
        verify(clusterAdminClient, times(1)).state(any(), any());
        verify(indicesClient, times(1)).rolloverIndex(any(), any());
        verify(indicesClient, times(1)).delete(any(), any());
    }
}
