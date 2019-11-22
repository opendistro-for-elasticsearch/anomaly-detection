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

import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * ModelInformation keeps track of different information on a per model basis
 */
public class ModelInformation implements ToXContentObject {

    private String modelId;
    private String detectorId;
    private String modelType;

    public static String MODEL_ID_KEY = "model_id";
    public static String DETECTOR_ID_KEY = "detector_id";
    public static String MODEL_TYPE_KEY = "model_type";

    public static String RCF_TYPE_VALUE = "rcf";
    public static String THRESHOLD_TYPE_VALUE = "threshold";

    /**
     * Constructor
     * @param modelId id of the model
     * @param detectorId id of the detector corresponding to the model
     * @param modelType type of the model (either rcf or threshold)
     */
    public ModelInformation(String modelId, String detectorId, String modelType) {
        this.modelId = modelId;
        this.detectorId = detectorId;
        this.modelType = modelType;
    }

    public XContentBuilder toXContent(XContentBuilder xContentBuilder, Params params) throws IOException {

        xContentBuilder.startObject();
        xContentBuilder.field(MODEL_ID_KEY, modelId);
        xContentBuilder.field(DETECTOR_ID_KEY, detectorId);
        xContentBuilder.field(MODEL_TYPE_KEY, modelType);
        xContentBuilder.endObject();

        return xContentBuilder;
    }

    /**
     * getModelId
     * @return modelId of model
     */
    public String getModelId() { return modelId; }

    /**
     * getDetectorId
     * @return detectorId associated with the model
     */
    public String getDetectorId() { return detectorId; }

    /**
     * getModelType
     * @return modelType of the model
     */
    public String getModelType() { return modelType; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ModelInformation that = (ModelInformation) o;
        return getModelId().equals(that.getModelId()) &&
                getDetectorId().equals(that.getDetectorId()) &&
                getModelType().equals(that.getModelType());
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, detectorId, modelType);
    }
}
