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

public class EntityFeatureRequest extends EntityRequest {
    private final double[] currentFeature;
    private final long dataStartTimeMillis;

    public EntityFeatureRequest(
        long expirationEpochMs,
        String detectorId,
        SegmentPriority priority,
        String entityName,
        String modelId,
        double[] currentFeature,
        long dataStartTimeMs
    ) {
        super(expirationEpochMs, detectorId, priority, entityName, modelId);
        this.currentFeature = currentFeature;
        this.dataStartTimeMillis = dataStartTimeMs;
    }

    public double[] getCurrentFeature() {
        return currentFeature;
    }

    public long getDataStartTimeMillis() {
        return dataStartTimeMillis;
    }
}
