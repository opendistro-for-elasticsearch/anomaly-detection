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

import org.apache.lucene.index.IndexNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SearchAnomalyDetectorActionTests extends ESIntegTestCase {
    private SearchAnomalyDetectorTransportAction action;
    private Task task;
    private ActionListener<SearchResponse> response;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        action = new SearchAnomalyDetectorTransportAction(mock(TransportService.class), mock(ActionFilters.class), client());
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

    @Test
    public void testSearchResponse() {
        // Will call response.onResponse as Index exists
        Settings indexSettings = Settings.builder().put("index.number_of_shards", 5).put("index.number_of_replicas", 1).build();
        CreateIndexRequest indexRequest = new CreateIndexRequest("my-test-index", indexSettings);
        client().admin().indices().create(indexRequest).actionGet();
        SearchRequest searchRequest = new SearchRequest("my-test-index");
        action.doExecute(task, searchRequest, response);
    }

    @Test
    public void testSearchDetectorAction() {
        Assert.assertNotNull(SearchAnomalyDetectorAction.INSTANCE.name());
        Assert.assertEquals(SearchAnomalyDetectorAction.INSTANCE.name(), SearchAnomalyDetectorAction.NAME);
    }

    @Test
    public void testNoIndex() {
        // No Index, will call response.onFailure
        SearchRequest searchRequest = new SearchRequest("my-test-index");
        action.doExecute(task, searchRequest, response);
    }
}
