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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.apache.lucene.index.IndexNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;

public class SearchAnomalyDetectorActionTests extends ESIntegTestCase {
    private SearchAnomalyDetectorTransportAction action;
    private Task task;
    private ActionListener<SearchResponse> response;
    private ClusterService clusterService;
    private Client client;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        ThreadPool threadPool = mock(ThreadPool.class);
        client = mock(Client.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(client.threadPool()).thenReturn(threadPool);
        when(client.threadPool().getThreadContext()).thenReturn(threadContext);

        action = new SearchAnomalyDetectorTransportAction(
            Settings.EMPTY,
            mock(TransportService.class),
            clusterService,
            mock(ActionFilters.class),
            client,
            mock(RestClient.class)
        );
        task = mock(Task.class);
        response = new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                Assert.assertEquals(searchResponse.getSuccessfulShards(), 5);
            }

            @Override
            public void onFailure(Exception e) {
                Assert.assertFalse(IndexNotFoundException.class == e.getClass());
            }
        };
    }

    // Ignoring this test as this is flaky.
    @Ignore
    @Test
    public void testSearchResponse() throws IOException {
        // Will call response.onResponse as Index exists
        Settings indexSettings = Settings.builder().put("index.number_of_shards", 5).put("index.number_of_replicas", 1).build();
        CreateIndexRequest indexRequest = new CreateIndexRequest("my-test-index", indexSettings);
        client().admin().indices().create(indexRequest).actionGet();
        SearchRequest searchRequest = new SearchRequest("my-test-index");
        SearchAnomalyRequest searchAnomalyRequest = new SearchAnomalyRequest(searchRequest, "authHeader");
        action.doExecute(task, searchAnomalyRequest, response);
    }

    @Test
    public void testSearchDetectorAction() {
        Assert.assertNotNull(SearchAnomalyDetectorAction.INSTANCE.name());
        Assert.assertEquals(SearchAnomalyDetectorAction.INSTANCE.name(), SearchAnomalyDetectorAction.NAME);
    }

    @Test
    public void testNoIndex() throws IOException {
        // No Index, will call response.onFailure
        SearchRequest searchRequest = new SearchRequest("my-test-index");
        SearchAnomalyRequest searchAnomalyRequest = new SearchAnomalyRequest(searchRequest, "authHeader");
        action.doExecute(task, searchAnomalyRequest, response);
    }

    @Test
    public void testSearchAnomalyRequest() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        SearchRequest searchRequest = new SearchRequest("my-test-index");
        SearchAnomalyRequest request = new SearchAnomalyRequest(searchRequest, "authHeader");
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        SearchAnomalyRequest newRequest = new SearchAnomalyRequest(input);
        Assert.assertEquals(request.getAuthHeader(), newRequest.getAuthHeader());
    }
}
