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

package com.amazon.opendistroforelasticsearch.ad.ml;

import java.util.concurrent.ConcurrentHashMap;

import com.amazon.opendistroforelasticsearch.ad.MemoryTracker;
import com.amazon.opendistroforelasticsearch.ad.MemoryTracker.Origin;
import com.amazon.randomcutforest.RandomCutForest;

/**
 * A customized ConcurrentHashMap that can automatically consume and release memory.
 * This enables minimum change to our single-entity code as we just have to replace
 * the map implementation.
 *
 * Note: this is mainly used for single-entity detectors.
 */
public class RCFMemoryAwareConcurrentHashmap<K> extends ConcurrentHashMap<K, ModelState<RandomCutForest>> {
    private final MemoryTracker memoryTracker;

    public RCFMemoryAwareConcurrentHashmap(MemoryTracker memoryTracker) {
        this.memoryTracker = memoryTracker;
    }

    @Override
    public ModelState<RandomCutForest> remove(Object key) {
        ModelState<RandomCutForest> deletedModelState = super.remove(key);
        if (deletedModelState != null && deletedModelState.getModel() != null) {
            long memoryToRelease = memoryTracker.estimateModelSize(deletedModelState.getModel());
            memoryTracker.releaseMemory(memoryToRelease, true, Origin.SINGLE_ENTITY_DETECTOR);
        }
        return deletedModelState;
    }

    @Override
    public ModelState<RandomCutForest> put(K key, ModelState<RandomCutForest> value) {
        ModelState<RandomCutForest> previousAssociatedState = super.put(key, value);
        if (value != null && value.getModel() != null) {
            long memoryToConsume = memoryTracker.estimateModelSize(value.getModel());
            memoryTracker.consumeMemory(memoryToConsume, true, Origin.SINGLE_ENTITY_DETECTOR);
        }
        return previousAssociatedState;
    }
}
