/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazon.opendistroforelasticsearch.ad.util;

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;

import java.time.Clock;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.elasticsearch.action.ActionRequest;

/**
 * Utility functions for throttling query.
 */
public class Throttler {
    // negativeCache is used to reject search query if given detector already has one query running
    // key is detectorId, value is an entry. Key is ActionRequest and value is the timestamp
    private final ConcurrentHashMap<String, Map.Entry<ActionRequest, Instant>> negativeCache;
    private final Clock clock;

    /**
     * Getter for clock
     * @return clock
     */
    public Clock getClock() {
        return clock;
    }


    /**
     * Getter for negativeCache
     * @return negative cache map ConcurrentHashMap
     */
    public ConcurrentHashMap<String, Map.Entry<ActionRequest, Instant>> getNegativeCache() {
        return negativeCache;
    }

    public Throttler(Clock clock) {
        this.negativeCache = new ConcurrentHashMap<>();
        this.clock = clock;
    }

    /**
     * Get negative cache value(ActionRequest, Instant) for given detector
     * @param detectorId AnomalyDetector Id
     * @return negative cache value(ActionRequest, Instant)
     */
    public Optional<Map.Entry<ActionRequest, Instant>> getFilteredQuery(String detectorId) {
        return Optional.ofNullable(negativeCache.get(detectorId));
    }

    /**
     * Insert the negative cache entry for given detector
     * @param detectorId AnomalyDetector Id
     * @param request ActionRequest
     */
    public void insertFilteredQuery(String detectorId, ActionRequest request) {
        negativeCache.put(detectorId, new AbstractMap.SimpleEntry<>(request, clock.instant()));
    }

    /**
     * Clear the negative cache for given detector.
     * If detectorId is null, do nothing
     * @param detectorId AnomalyDetector Id
     */
    public void clearFilteredQuery(String detectorId) {
        negativeCache.keySet().removeIf(key -> key.equals(detectorId));
    }
}
