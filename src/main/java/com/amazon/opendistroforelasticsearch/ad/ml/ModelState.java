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
import java.util.HashMap;
import java.util.Map;

/**
 * A ML model and states such as usage.
 */
public class ModelState<T> {

    public static String MODEL_ID_KEY = "model_id";
    public static String DETECTOR_ID_KEY = "detector_id";
    public static String MODEL_TYPE_KEY = "model_type";

    private T model;
    private String modelId;
    private String detectorId;
    private String modelType;
    private Instant lastUsedTime;
    private Instant lastCheckpointTime;

    /**
     * Constructor.
     *
     * @param model ML model
     * @param modelId Id of Model this model partition is a part of
     * @param detectorId Id of detector this model partition is used for
     * @param modelType type of model
     * @param lastUsedTime time when the ML model was used last time
     */
    public ModelState(T model, String modelId, String detectorId, String modelType, Instant lastUsedTime) {
        this.model = model;
        this.modelId = modelId;
        this.detectorId = detectorId;
        this.modelType = modelType;
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
     * getModelId
     * @return modelId of model
     */
    public String getModelId() {
        return modelId;
    }

    /**
     * getDetectorId
     * @return detectorId associated with the model
     */
    public String getDetectorId() {
        return detectorId;
    }

    /**
     * getModelType
     * @return modelType of the model
     */
    public String getModelType() {
        return modelType;
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

    /**
     * getModelStateAsMap
     * @return Map of ModelStates
     */
    public Map<String, Object> getModelStateAsMap() {
        return new HashMap<String, Object>() {{
            put(MODEL_ID_KEY, modelId);
            put(DETECTOR_ID_KEY, detectorId);
            put(MODEL_TYPE_KEY, modelType);
        }};
    }
}
