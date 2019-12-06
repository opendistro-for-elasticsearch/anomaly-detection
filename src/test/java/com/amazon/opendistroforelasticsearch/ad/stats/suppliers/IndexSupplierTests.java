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
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexSupplierTests extends ESTestCase {
    private AnomalyDetectionIndices indices;
    private String indexStatus;

    @Before
    public void setup() {
        indexStatus = "yellow";
        indices = mock(AnomalyDetectionIndices.class);
        when(indices.getIndexHealthStatus(anyString())).thenReturn(indexStatus);
    }

    @Test
    public void testGet() {
        IndexStatusSupplier indexStatusSupplier = new IndexStatusSupplier(indices, AnomalyDetector.ANOMALY_DETECTORS_INDEX);
        assertEquals("Get method for Anomaly Result Index Health does not work", indexStatus, indexStatusSupplier.get());
    }
}