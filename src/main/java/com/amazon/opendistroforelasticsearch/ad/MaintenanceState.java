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

package com.amazon.opendistroforelasticsearch.ad;

import java.time.Duration;
import java.util.Map;

/**
 * Represent a state that needs to maintain its metadata regularly
 *
 *
 */
public interface MaintenanceState {
    default <K, V extends ExpiringState> void maintenance(Map<K, V> stateToClean, Duration stateTtl) {
        stateToClean.entrySet().stream().forEach(entry -> {
            K detectorId = entry.getKey();

            V state = entry.getValue();
            if (state.expired(stateTtl)) {
                stateToClean.remove(detectorId);
            }

        });
    }

    void maintenance();
}
