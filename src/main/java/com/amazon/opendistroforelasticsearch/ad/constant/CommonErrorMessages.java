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

public class CommonErrorMessages {
    public static final String AD_ID_MISSING_MSG = "AD ID is missing";
    public static final String MODEL_ID_MISSING_MSG = "Model ID is missing";
    public static final String WAIT_ERR_MSG = "Exception in waiting for result";
    public static final String HASH_ERR_MSG = "Cannot find an RCF node.  Hashing does not work.";
    public static final String NO_CHECKPOINT_ERR_MSG = "No checkpoints found for model id ";
    public static final String MEMORY_LIMIT_EXCEEDED_ERR_MSG = "AD models memory usage exceeds our limit.";
    public static final String FEATURE_NOT_AVAILABLE_ERR_MSG = "No Feature in current detection window.";
    public static final String MEMORY_CIRCUIT_BROKEN_ERR_MSG = "AD memory circuit is broken.";
    public static final String DISABLED_ERR_MSG = "AD plugin is disabled. To enable update opendistro.anomaly_detection.enabled to true";
    public static final String INVALID_SEARCH_QUERY_MSG = "Invalid search query.";
    public static final String ALL_FEATURES_DISABLED_ERR_MSG =
        "Having trouble querying data because all of your features have been disabled.";
    public static final String INVALID_TIMESTAMP_ERR_MSG = "timestamp is invalid";
}
