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

import com.amazon.opendistroforelasticsearch.ad.ml.ModelInformation;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ModelsOnNodeSupplierTests extends ESTestCase {
    private ModelManager modelManager;
    List<ModelInformation> expectedResults;

    @Before
    public void setup() {
        expectedResults = new ArrayList<>(Arrays.asList(
                new ModelInformation("rcf-model-1", "detector-1", ModelInformation.RCF_TYPE_VALUE),
                new ModelInformation("thr-model-1",  "detector-1", ModelInformation.THRESHOLD_TYPE_VALUE),
                new ModelInformation("rcf-model-2",  "detector-2", ModelInformation.RCF_TYPE_VALUE),
                new ModelInformation("thr-model-2", "detector-2",  ModelInformation.THRESHOLD_TYPE_VALUE)
            ));

        modelManager = mock(ModelManager.class);
        when(modelManager.getAllModelsInformation()).thenReturn(expectedResults);
    }

    @Test
    public void testGet() {
        ModelsOnNodeSupplier modelsOnNodeSupplier = new ModelsOnNodeSupplier(modelManager);
        List<Map<String, Object>> results = modelsOnNodeSupplier.get();
        assertEquals("get fails to return correct result",
                expectedResults.stream().map(ModelInformation::getModelInfoAsMap).collect(Collectors.toList()), results);
    }
}