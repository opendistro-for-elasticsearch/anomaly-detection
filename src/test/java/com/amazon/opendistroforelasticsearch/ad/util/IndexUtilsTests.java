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

package com.amazon.opendistroforelasticsearch.ad.util;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;
import org.junit.Test;

public class IndexUtilsTests extends ESIntegTestCase {

    private ClientUtil clientUtil;

    @Before
    public void setup() {
        clientUtil = new ClientUtil(Settings.EMPTY);
    }

    @Test
    public void testGetIndexHealth_NoIndex() {
        IndexUtils indexUtils = new IndexUtils(client(), clientUtil, clusterService());
        String output = indexUtils.getIndexHealthStatus("test");
        assertEquals(IndexUtils.NONEXISTENT_INDEX_STATUS, output);
    }

    @Test
    public void testGetIndexHealth_Index() {
        String indexName = "test-2";
        createIndex(indexName);
        flush();
        IndexUtils indexUtils = new IndexUtils(client(), clientUtil, clusterService());
        String status = indexUtils.getIndexHealthStatus(indexName);
        assertTrue(status.equals("green") || status.equals("yellow"));
    }

    @Test
    public void testGetIndexHealth_Alias() {
        String indexName = "test-2";
        String aliasName = "alias";
        createIndex(indexName);
        flush();
        AcknowledgedResponse response = client().admin().indices().prepareAliases().addAlias(indexName, aliasName).execute().actionGet();
        assertTrue(response.isAcknowledged());
        IndexUtils indexUtils = new IndexUtils(client(), clientUtil, clusterService());
        String status = indexUtils.getIndexHealthStatus(aliasName);
        assertTrue(status.equals("green") || status.equals("yellow"));
    }

    @Test
    public void testGetNumberOfDocumentsInIndex_NonExistentIndex() {
        IndexUtils indexUtils = new IndexUtils(client(), clientUtil, clusterService());
        assertEquals((Long) 0L, indexUtils.getNumberOfDocumentsInIndex("index"));
    }

    @Test
    public void testGetNumberOfDocumentsInIndex_RegularIndex() {
        String indexName = "test-2";
        createIndex(indexName);
        flush();

        long count = 2100;
        for (int i = 0; i < count; i++) {
            index(indexName, "_doc", String.valueOf(i), "{}");
        }
        flushAndRefresh(indexName);
        IndexUtils indexUtils = new IndexUtils(client(), clientUtil, clusterService());
        assertEquals((Long) count, indexUtils.getNumberOfDocumentsInIndex(indexName));
    }
}
