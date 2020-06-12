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

package com.amazon.opendistroforelasticsearch.ad.constant;

public class CommonName {
    // ======================================
    // Index name
    // ======================================
    // index name for anomaly checkpoint of each model. One model one document.
    public static final String CHECKPOINT_INDEX_NAME = ".opendistro-anomaly-checkpoints";

    // ======================================
    // Format name
    // ======================================
    public static final String EPOCH_MILLIS_FORMAT = "epoch_millis";

    // ======================================
    // Anomaly Detector name for X-Opaque-Id header
    // ======================================
    public static final String ANOMALY_DETECTOR = "[Anomaly Detector]";

    // ======================================
    // Ultrawarm node attributes
    // ======================================

    // hot node
    public static String HOT_BOX_TYPE = "hot";

    // warm node
    public static String WARM_BOX_TYPE = "warm";

    // box type
    public static final String BOX_TYPE_KEY = "box_type";

    // ======================================
    // Profile name
    // ======================================
    public static final String STATE = "state";
    public static final String ERROR = "error";
    public static final String COORDINATING_NODE = "coordinating_node";
    public static final String SHINGLE_SIZE = "shingle_size";
    public static final String TOTAL_SIZE_IN_BYTES = "total_size_in_bytes";
    public static final String MODELS = "models";
    public static final String INIT_PROGRESS = "init_progress";
}
