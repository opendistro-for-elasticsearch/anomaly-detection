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

package com.amazon.opendistroforelasticsearch.ad.transport;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;

/**
 * Storing intermediate state during the execution of transport action
 *
 */
public class TransportState {
    private String detectorId;
    // detector definition
    private AnomalyDetector detectorDef;
    // number of partitions
    private int partitonNumber;
    // checkpoint fetch time
    private Instant lastAccessTime;
    // last detection error. Used by DetectorStateHandler to check if the error for a
    // detector has changed or not. If changed, trigger indexing.
    private Optional<String> lastDetectionError;
    // last training error. Used to save cold start error by a concurrent cold start thread.
    private Optional<AnomalyDetectionException> lastColdStartException;
    // flag indicating whether checkpoint for the detector exists
    private boolean checkPointExists;
    // clock to get current time
    private final Clock clock;
    // cold start running flag to prevent concurrent cold start
    private boolean coldStartRunning;

    public TransportState(String detectorId, Clock clock) {
        this.detectorId = detectorId;
        this.detectorDef = null;
        this.partitonNumber = -1;
        this.lastAccessTime = clock.instant();
        this.lastDetectionError = Optional.empty();
        this.lastColdStartException = Optional.empty();
        this.checkPointExists = false;
        this.clock = clock;
        this.coldStartRunning = false;
    }

    public String getDetectorId() {
        return detectorId;
    }

    public AnomalyDetector getDetectorDef() {
        refreshLastUpdateTime();
        return detectorDef;
    }

    public void setDetectorDef(AnomalyDetector detectorDef) {
        this.detectorDef = detectorDef;
        refreshLastUpdateTime();
    }

    public int getPartitonNumber() {
        refreshLastUpdateTime();
        return partitonNumber;
    }

    public void setPartitonNumber(int partitonNumber) {
        this.partitonNumber = partitonNumber;
        refreshLastUpdateTime();
    }

    public boolean doesCheckpointExists() {
        refreshLastUpdateTime();
        return checkPointExists;
    }

    public void setCheckpointExists(boolean checkpointExists) {
        refreshLastUpdateTime();
        this.checkPointExists = checkpointExists;
    };

    public Optional<String> getLastDetectionError() {
        refreshLastUpdateTime();
        return lastDetectionError;
    }

    public void setLastDetectionError(String lastError) {
        this.lastDetectionError = Optional.ofNullable(lastError);
        refreshLastUpdateTime();
    }

    public Optional<AnomalyDetectionException> getLastColdStartException() {
        refreshLastUpdateTime();
        return lastColdStartException;
    }

    public void setLastColdStartException(AnomalyDetectionException lastColdStartError) {
        this.lastColdStartException = Optional.ofNullable(lastColdStartError);
        refreshLastUpdateTime();
    }

    public boolean isColdStartRunning() {
        refreshLastUpdateTime();
        return coldStartRunning;
    }

    public void setColdStartRunning(boolean coldStartRunning) {
        this.coldStartRunning = coldStartRunning;
        refreshLastUpdateTime();
    }

    private void refreshLastUpdateTime() {
        lastAccessTime = clock.instant();
    }

    public boolean expired(Duration stateTtl) {
        return lastAccessTime.plus(stateTtl).isBefore(clock.instant());
    }
}
