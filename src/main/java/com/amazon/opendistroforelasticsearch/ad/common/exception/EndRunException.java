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
 * Exception for failures that might impact the customer.
 *
 */
public class EndRunException extends ClientException {
    private boolean endNow;

    public EndRunException(String message, boolean endNow) {
        super(message);
        this.endNow = endNow;
    }

    public EndRunException(String anomalyDetectorId, String message, boolean endNow) {
        super(anomalyDetectorId, message);
        this.endNow = endNow;
    }

    public EndRunException(String anomalyDetectorId, String message, Throwable throwable, boolean endNow) {
        super(anomalyDetectorId, message, throwable);
        this.endNow = endNow;
    }

    /**
     * @return true for "unrecoverable issue". We want to terminate the detector run immediately.
     *         false for "maybe unrecoverable issue but worth retrying a few more times." We want
     *          to wait for a few more times on different requests before terminating the detector run.
     */
    public boolean isEndNow() {
        return endNow;
    }
}
