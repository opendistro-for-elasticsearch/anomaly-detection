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

package com.amazon.opendistroforelasticsearch.ad.common.exception;

/**
 * Exception for root cause unknown failure. Maybe transient. Client can continue the detector running.
 *
 */
public class InternalFailure extends ClientException {

    public InternalFailure(String anomalyDetectorId, String message) {
        super(anomalyDetectorId, message);
    }

    public InternalFailure(String anomalyDetectorId, String message, Throwable cause) {
        super(anomalyDetectorId, message, cause);
    }

    public InternalFailure(String anomalyDetectorId, Throwable cause) {
        super(anomalyDetectorId, cause);
    }

    public InternalFailure(AnomalyDetectionException cause) {
        super(cause.getAnomalyDetectorId(), cause);
    }
}
