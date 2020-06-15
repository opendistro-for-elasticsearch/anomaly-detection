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
 * Base exception for exceptions thrown from Anomaly Detection.
 */
public class AnomalyDetectionException extends RuntimeException {

    private final String anomalyDetectorId;

    /**
     * Constructor with an anomaly detector ID and a message.
     *
     * @param anomalyDetectorId anomaly detector ID
     * @param message message of the exception
     */
    public AnomalyDetectionException(String anomalyDetectorId, String message) {
        super(message);
        this.anomalyDetectorId = anomalyDetectorId;
    }

    public AnomalyDetectionException(String adID, String message, Throwable cause) {
        super(message, cause);
        this.anomalyDetectorId = adID;
    }

    public AnomalyDetectionException(String adID, Throwable cause) {
        super(cause);
        this.anomalyDetectorId = adID;
    }

    /**
     * Returns the ID of the anomaly detector.
     *
     * @return anomaly detector ID
     */
    public String getAnomalyDetectorId() {
        return this.anomalyDetectorId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Anomaly Detector ");
        sb.append(anomalyDetectorId);
        sb.append(' ');
        sb.append(super.toString());
        return sb.toString();
    }
}
