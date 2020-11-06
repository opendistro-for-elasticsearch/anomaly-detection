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

package com.amazon.opendistroforelasticsearch.ad.cluster.diskcleanup;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.store.StoreStats;
import org.junit.Assert;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;

public class IndexCleanupTests extends AbstractADTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    Client client;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    ClusterService clusterService;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    ClientUtil clientUtil;

    @InjectMocks
    IndexCleanup indexCleanup;

    @Mock
    IndicesStatsResponse indicesStatsResponse;

    @Mock
    ShardStats shardStats;

    @Mock
    CommonStats commonStats;

    @Mock
    StoreStats storeStats;

    @Mock
    IndicesAdminClient indicesAdminClient;

    @SuppressWarnings("unchecked")
    @Override
    public void setUp() throws Exception {
        super.setUp();
        super.setUpLog4jForJUnit(IndexCleanup.class);
        MockitoAnnotations.initMocks(this);
        when(clusterService.state().getRoutingTable().hasIndex(anyString())).thenReturn(true);
        indexCleanup = new IndexCleanup(client, clientUtil, clusterService);
        when(indicesStatsResponse.getShards()).thenReturn(new ShardStats[] { shardStats });
        when(shardStats.getStats()).thenReturn(commonStats);
        when(commonStats.getStore()).thenReturn(storeStats);
        when(client.admin().indices()).thenReturn(indicesAdminClient);
        when(client.threadPool().getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<IndicesStatsResponse> listener = (ActionListener<IndicesStatsResponse>) args[1];
            listener.onResponse(indicesStatsResponse);
            return null;
        }).when(indicesAdminClient).stats(any(), any());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        super.tearDownLog4jForJUnit();
    }

    public void testDeleteDocsBasedOnShardSizeWithCleanupNeededAsTrue() throws Exception {
        long maxShardSize = 1000;
        when(storeStats.getSizeInBytes()).thenReturn(maxShardSize + 1);
        indexCleanup.deleteDocsBasedOnShardSize("indexname", maxShardSize, null, ActionListener.wrap(result -> {
            assertTrue(result);
            verify(clientUtil).execute(eq(DeleteByQueryAction.INSTANCE), any(), any());
        }, exception -> { throw new RuntimeException(exception); }));
    }

    public void testDeleteDocsBasedOnShardSizeWithCleanupNeededAsFalse() throws Exception {
        long maxShardSize = 1000;
        when(storeStats.getSizeInBytes()).thenReturn(maxShardSize - 1);
        indexCleanup
            .deleteDocsBasedOnShardSize(
                "indexname",
                maxShardSize,
                null,
                ActionListener.wrap(Assert::assertFalse, exception -> { throw new RuntimeException(exception); })
            );
    }

    public void testDeleteDocsBasedOnShardSizeIndexNotExisted() throws Exception {
        when(clusterService.state().getRoutingTable().hasIndex(anyString())).thenReturn(false);
        indexCleanup.deleteDocsBasedOnShardSize("indexname", 1000, null, null);
        assertTrue(testAppender.containsMessage("skip as the index:indexname doesn't exist"));
    }
}
