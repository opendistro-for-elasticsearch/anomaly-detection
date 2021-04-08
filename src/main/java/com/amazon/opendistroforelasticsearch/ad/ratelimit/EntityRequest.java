/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.ad.ratelimit;

public class EntityRequest extends QueuedRequest {
    private final String entityName;
    private final String modelId;

    /**
     *
     * @param expirationEpochMs Expiry time of the request
     * @param entityName Entity name
     * @param modelId Model Id
     */
    public EntityRequest(long expirationEpochMs, String detectorId, SegmentPriority priority, String entityName, String modelId) {
        super(expirationEpochMs, detectorId, priority);
        this.entityName = entityName;
        this.modelId = modelId;
    }

    public String getEntityName() {
        return entityName;
    }

    public String getModelId() {
        return modelId;
    }
}
