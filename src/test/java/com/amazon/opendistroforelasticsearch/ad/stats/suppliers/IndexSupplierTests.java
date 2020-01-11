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

package com.amazon.opendistroforelasticsearch.ad.stats.suppliers;

import com.amazon.opendistroforelasticsearch.ad.util.IndexUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexSupplierTests extends ESTestCase {
    private IndexUtils indexUtils;
    private String indexStatus;
    private String indexName;

    @Before
    public void setup() {
        indexUtils = mock(IndexUtils.class);
        indexStatus = "yellow";
        indexName = "test-index";
        when(indexUtils.getIndexHealthStatus(indexName)).thenReturn(indexStatus);
    }

    @Test
    public void testGet() {
        IndexStatusSupplier indexStatusSupplier1 = new IndexStatusSupplier(indexUtils, indexName);
        assertEquals("Get method for IndexSupplier does not work", indexStatus, indexStatusSupplier1.get());

        String invalidIndex = "invalid";
        when(indexUtils.getIndexHealthStatus(invalidIndex)).thenThrow(IllegalArgumentException.class);
        IndexStatusSupplier indexStatusSupplier2 = new IndexStatusSupplier(indexUtils, invalidIndex);
        assertEquals(
            "Get method does not return correct response onf exception",
            IndexStatusSupplier.UNABLE_TO_RETRIEVE_HEALTH_MESSAGE,
            indexStatusSupplier2.get()
        );
    }
}
