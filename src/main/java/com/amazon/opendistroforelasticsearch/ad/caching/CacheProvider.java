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

package com.amazon.opendistroforelasticsearch.ad.caching;

import com.amazon.opendistroforelasticsearch.ad.ml.EntityModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelState;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;

/**
 * A wrapper to call concrete implementation of caching.  Used in transport
 * action.  Don't use interface because transport action handler constructor
 * requires a concrete class as input.
 *
 */
public class CacheProvider implements EntityCache {
    private EntityCache delegate;

    public CacheProvider(EntityCache delegate) {
        this.delegate = delegate;
    }

    @Override
    public ModelState<EntityModel> get(String modelId, AnomalyDetector detector, double[] datapoint, String entityName) {
        return delegate.get(modelId, detector, datapoint, entityName);
    }

    @Override
    public void maintenance() {
        delegate.maintenance();
    }

    @Override
    public void clear(String detectorId) {
        delegate.clear(detectorId);
    }

    @Override
    public int getActiveEntities(String detector) {
        return delegate.getActiveEntities(detector);
    }

    @Override
    public boolean isActive(String detectorId, String entityId) {
        return delegate.isActive(detectorId, entityId);
    }

    @Override
    public long getTotalUpdates(String detectorId) {
        return delegate.getTotalUpdates(detectorId);
    }

    @Override
    public long getTotalUpdates(String detectorId, String entityId) {
        return delegate.getTotalUpdates(detectorId, entityId);
    }
}
