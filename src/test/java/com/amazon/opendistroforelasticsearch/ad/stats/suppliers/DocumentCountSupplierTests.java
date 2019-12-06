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

import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.when;

public class DocumentCountSupplierTests extends ESTestCase {

    private Long count;
    private String indexName;

    @Mock
    private AnomalyDetectionIndices indices;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        count = 15L;
        indexName = "test-index";
        when(indices.getNumberOfDocumentsInIndex(indexName)).thenReturn(count);
    }

    @Test
    public void testGet() {
        DocumentCountSupplier documentCountSupplier = new DocumentCountSupplier(indices, indexName);
        assertEquals("Get fails", count, documentCountSupplier.get());
    }
}