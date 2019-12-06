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
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * ModelsOnNodeSupplier provides a List of ModelStates info for the models the nodes contains
 */
public class ModelsOnNodeSupplier implements Supplier<List<Map<String, Object>>> {
    private ModelManager modelManager;

    public ModelsOnNodeSupplier(ModelManager modelManager) {
        this.modelManager = modelManager;
    }

    @Override
    public List<Map<String, Object>> get() {
        List<Map<String, Object>> values = new ArrayList<>();
        modelManager.getAllModels().forEach(
                modelState -> values.add(modelState.getModelStateAsMap())
        );

        return values;
    }
}
