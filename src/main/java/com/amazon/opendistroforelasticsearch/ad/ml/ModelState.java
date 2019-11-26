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

package com.amazon.opendistroforelasticsearch.ad.ml;

import java.time.Instant;

/**
 * A ML model and states such as usage.
 */
public class ModelState<T> {

    private T model;
    private Instant lastUsedTime;
    private Instant lastCheckpointTime;

    /**
     * Constructor.
     *
     * @param model ML model
     * @param lastUsedTime time when the ML model was used last time
     */
    public ModelState(T model, Instant lastUsedTime) {
        this.model = model;
        this.lastUsedTime = lastUsedTime;
        this.lastCheckpointTime = Instant.MIN;
    }

    /**
     * Returns the ML model.
     *
     * @return the ML model.
     */
    public T getModel() {
        return this.model;
    }

    /**
     * Returns the time when the ML model was used last time.
     *
     * @return the time when the ML model was used last time
     */
    public Instant getLastUsedTime() {
        return this.lastUsedTime;
    }

    /**
     * Sets the time when ML model was used last time.
     *
     * @param lastUsedTime time when the ML model was used last time
     */
    public void setLastUsedTime(Instant lastUsedTime) {
        this.lastUsedTime = lastUsedTime;
    }

    /**
     * Returns the time when a checkpoint for the ML model was made last time.
     *
     * @return the time when a checkpoint for the ML model was made last time.
     */
    public Instant getLastCheckpointTime() {
        return this.lastCheckpointTime;
    }

    /**
     * Sets the time when a checkpoint for the ML model was made last time.
     *
     * @param lastCheckpointTime time when a checkpoint for the ML model was made last time.
     */
    public void setLastCheckpointTime(Instant lastCheckpointTime) {
        this.lastCheckpointTime = lastCheckpointTime;
    }
}
