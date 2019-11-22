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

import com.amazon.opendistroforelasticsearch.ad.common.exception.JsonPathNotFoundException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import test.com.amazon.opendistroforelasticsearch.ad.util.JsonDeserializer;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ModelInformationTests extends ESTestCase {
    private String modelId;
    private String detectorId;

    ModelInformation modelInformation;

    @Before
    public void setup() {
        modelId = "modelId";
        detectorId = "detectorId";
        modelInformation = new ModelInformation(modelId, detectorId, ModelInformation.THRESHOLD_TYPE_VALUE);
    }

    public void testGetModelId() {
        assertEquals("getModelId returns incorrect modelId", modelId, modelInformation.getModelId());
    }

    public void testGetDetectorId() {
        assertEquals("getDetectorId returns incorrect detectorId", detectorId, modelInformation.getDetectorId());
    }

    public void testIsThreshold() {
        assertEquals("isThreshold returns incorrect value", ModelInformation.THRESHOLD_TYPE_VALUE,
                modelInformation.getModelType());
    }

    public void testToXContent() throws IOException, JsonPathNotFoundException {
        XContentBuilder builder = jsonBuilder();
        modelInformation.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = Strings.toString(builder);
        assertEquals(JsonDeserializer.getTextValue(json, ModelInformation.MODEL_ID_KEY),
                modelId);
        assertEquals(JsonDeserializer.getTextValue(json, ModelInformation.DETECTOR_ID_KEY),
                detectorId);
        assertEquals(JsonDeserializer.getTextValue(json, ModelInformation.MODEL_TYPE_KEY),
                modelInformation.getModelType());

        // Check that toXContent also returns the correct model type when isThreshold is flipped
        modelInformation = new ModelInformation(modelId, detectorId, ModelInformation.RCF_TYPE_VALUE);
        builder = jsonBuilder();
        modelInformation.toXContent(builder, ToXContent.EMPTY_PARAMS);
        json = Strings.toString(builder);
        assertEquals(JsonDeserializer.getTextValue(json, ModelInformation.MODEL_TYPE_KEY),
                modelInformation.getModelType());
    }

    public void testEquals() {
        ModelInformation modelInformation1 = new ModelInformation(modelId, detectorId, ModelInformation.THRESHOLD_TYPE_VALUE);
        ModelInformation modelInformation2 = new ModelInformation(modelId, detectorId, ModelInformation.THRESHOLD_TYPE_VALUE);
        ModelInformation modelInformation3 = new ModelInformation(modelId, detectorId, ModelInformation.RCF_TYPE_VALUE);
        ModelInformation modelInformation4 = null;
        Long modelInformation5 = 0L;

        assertEquals("equals does not work", modelInformation1, modelInformation2);
        assertNotEquals("equals does not work for different ModelInformation Objects", modelInformation1, modelInformation3);
        assertNotEquals("equals does not work for null", modelInformation1, modelInformation4);
        assertNotEquals("equals does not work for different objects", modelInformation1, modelInformation5);
        assertEquals("hashCode does not work", modelInformation1.hashCode(), modelInformation2.hashCode());
    }
}
