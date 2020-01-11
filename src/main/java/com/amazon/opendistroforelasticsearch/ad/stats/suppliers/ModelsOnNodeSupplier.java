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

import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.amazon.opendistroforelasticsearch.ad.ml.ModelState.DETECTOR_ID_KEY;
import static com.amazon.opendistroforelasticsearch.ad.ml.ModelState.MODEL_ID_KEY;
import static com.amazon.opendistroforelasticsearch.ad.ml.ModelState.MODEL_TYPE_KEY;

/**
 * ModelsOnNodeSupplier provides a List of ModelStates info for the models the nodes contains
 */
public class ModelsOnNodeSupplier implements Supplier<List<Map<String, Object>>> {
    private ModelManager modelManager;

    /**
     * Set that contains the model stats that should be exposed.
     */
    public static Set<String> MODEL_STATE_STAT_KEYS = new HashSet<>(Arrays.asList(MODEL_ID_KEY, DETECTOR_ID_KEY, MODEL_TYPE_KEY));

    /**
     * Constructor
     *
     * @param modelManager object that manages the model partitions hosted on the node
     */
    public ModelsOnNodeSupplier(ModelManager modelManager) {
        this.modelManager = modelManager;
    }

    @Override
    public List<Map<String, Object>> get() {
        List<Map<String, Object>> values = new ArrayList<>();
        modelManager
            .getAllModels()
            .forEach(
                modelState -> values
                    .add(
                        modelState
                            .getModelStateAsMap()
                            .entrySet()
                            .stream()
                            .filter(entry -> MODEL_STATE_STAT_KEYS.contains(entry.getKey()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                    )
            );

        return values;
    }
}
