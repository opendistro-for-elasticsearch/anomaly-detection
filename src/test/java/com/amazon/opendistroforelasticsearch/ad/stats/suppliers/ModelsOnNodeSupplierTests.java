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

package com.amazon.opendistroforelasticsearch.ad.stats.suppliers;

import static com.amazon.opendistroforelasticsearch.ad.stats.suppliers.ModelsOnNodeSupplier.MODEL_STATE_STAT_KEYS;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazon.opendistroforelasticsearch.ad.ml.HybridThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelState;
import com.amazon.randomcutforest.RandomCutForest;

public class ModelsOnNodeSupplierTests extends ESTestCase {
    private RandomCutForest rcf;
    private HybridThresholdingModel thresholdingModel;
    private List<ModelState<?>> expectedResults;
    private Clock clock;

    @Mock
    private ModelManager modelManager;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        clock = Clock.systemUTC();
        rcf = RandomCutForest.builder().dimensions(1).sampleSize(1).numberOfTrees(1).build();
        thresholdingModel = new HybridThresholdingModel(1e-8, 1e-5, 200, 10_000, 2, 5_000_000);

        expectedResults = new ArrayList<>(
            Arrays
                .asList(
                    new ModelState<>(rcf, "rcf-model-1", "detector-1", ModelManager.ModelType.RCF.getName(), clock, 0f),
                    new ModelState<>(thresholdingModel, "thr-model-1", "detector-1", ModelManager.ModelType.RCF.getName(), clock, 0f),
                    new ModelState<>(rcf, "rcf-model-2", "detector-2", ModelManager.ModelType.THRESHOLD.getName(), clock, 0f),
                    new ModelState<>(thresholdingModel, "thr-model-2", "detector-2", ModelManager.ModelType.THRESHOLD.getName(), clock, 0f)
                )
        );

        when(modelManager.getAllModels()).thenReturn(expectedResults);
    }

    @Test
    public void testGet() {
        ModelsOnNodeSupplier modelsOnNodeSupplier = new ModelsOnNodeSupplier(modelManager);
        List<Map<String, Object>> results = modelsOnNodeSupplier.get();
        assertEquals(
            "get fails to return correct result",
            expectedResults
                .stream()
                .map(
                    modelState -> modelState
                        .getModelStateAsMap()
                        .entrySet()
                        .stream()
                        .filter(entry -> MODEL_STATE_STAT_KEYS.contains(entry.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                )
                .collect(Collectors.toList()),
            results
        );
    }
}
