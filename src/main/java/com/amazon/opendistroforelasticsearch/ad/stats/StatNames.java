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

package com.amazon.opendistroforelasticsearch.ad.stats;

import java.util.HashSet;
import java.util.Set;

/**
 * Enum containing names of all stats
 */
public enum StatNames {
    AD_EXECUTE_REQUEST_COUNT("ad_execute_request_count"),
    AD_EXECUTE_FAIL_COUNT("ad_execute_failure_count"),
    DETECTOR_COUNT("detector_count"),
    ANOMALY_DETECTORS_INDEX_STATUS("anomaly_detectors_index_status"),
    ANOMALY_RESULTS_INDEX_STATUS("anomaly_results_index_status"),
    MODELS_CHECKPOINT_INDEX_STATUS("models_checkpoint_index_status"),
    ANOMALY_DETECTION_JOB_INDEX_STATUS("anomaly_detection_job_index_status"),
    ANOMALY_DETECTION_STATE_STATUS("anomaly_detection_state_status"),
    MODEL_INFORMATION("models");

    private String name;

    StatNames(String name) {
        this.name = name;
    }

    /**
     * Get stat name
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     * Get set of stat names
     *
     * @return set of stat names
     */
    public static Set<String> getNames() {
        Set<String> names = new HashSet<>();

        for (StatNames statName : StatNames.values()) {
            names.add(statName.getName());
        }
        return names;
    }
}
