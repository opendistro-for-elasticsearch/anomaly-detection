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

import com.amazon.opendistroforelasticsearch.ad.CleanState;
import com.amazon.opendistroforelasticsearch.ad.MaintenanceState;
import com.amazon.opendistroforelasticsearch.ad.ml.EntityModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelState;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;

public interface EntityCache extends MaintenanceState, CleanState {
    /**
     * Get the ModelState associated with the entity.  May or may not load the
     * ModelState depending on the underlying cache's eviction policy.
     *
     * @param modelId Model Id
     * @param detector Detector config object
     * @param datapoint The most recent data point
     * @param entityName The Entity's name
     * @return the ModelState associated with the model or null if no cached item
     * for the entity
     */
    ModelState<EntityModel> get(String modelId, AnomalyDetector detector, double[] datapoint, String entityName);

    /**
     * Get the number of active entities of a detector
     * @param detector Detector Id
     * @return The number of active entities
     */
    int getActiveEntities(String detector);

    /**
     * Whether an entity is active or not
     * @param detectorId The Id of the detector that an entity belongs to
     * @param entityId Entity Id
     * @return Whether an entity is active or not
     */
    boolean isActive(String detectorId, String entityId);

    /**
     * Get total updates of detector's most active entity's RCF model.
     *
     * @param detectorId detector id
     * @return RCF model total updates of most active entity
     */
    long getTotalUpdates(String detectorId);

    /**
     * Get RCF model total updates of specific entity
     *
     * @param detectorId detector id
     * @param entityId  entity id
     * @return RCF model total updates of specific entity
     */
    long getTotalUpdates(String detectorId, String entityId);
}
