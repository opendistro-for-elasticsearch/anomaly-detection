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

package com.amazon.opendistroforelasticsearch.ad.rest;

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;

/**
 * This class consists of the REST handler to search anomaly detectors.
 */
public class RestSearchAnomalyDetectorAction extends AbstractSearchAction<AnomalyDetector> {

    private static final String URL_PATH = AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI + "/_search";
    private final String SEARCH_ANOMALY_DETECTOR_ACTION = "search_anomaly_detector";

    public RestSearchAnomalyDetectorAction() {
        super(URL_PATH, ANOMALY_DETECTORS_INDEX, AnomalyDetector.class);
    }

    @Override
    public String getName() {
        return SEARCH_ANOMALY_DETECTOR_ACTION;
    }
}
