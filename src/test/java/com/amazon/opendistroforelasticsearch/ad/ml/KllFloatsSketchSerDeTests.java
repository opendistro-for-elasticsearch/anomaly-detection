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

import java.util.Random;

import com.google.gson.Gson;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KllFloatsSketchSerDeTests {

    private Gson gson;

    private HybridThresholdingModel hybridModel;

    @Before
    public void setup() {
        gson = new Gson();

        hybridModel = new HybridThresholdingModel(/*minPvalueThreshold*/ 0.95,
            /*maxRankError*/ 1e-4,
            /*maxScore*/ 4,
            /*numLogNormalQuantiles*/ 10000,
            /*downsampleNumSamples*/ 100_000,
            /*downsampleMaxNumObservations*/ 200_000L
        );
    }

    @Test
    public void serialize_deserialize_returnOriginalModel() {
        hybridModel.train(new Random().doubles(10_000L, 0.1, 3.9).toArray());

        String json = gson.toJson(hybridModel);
        HybridThresholdingModel deserialized = gson.fromJson(json, HybridThresholdingModel.class);

        double delta = 1e-6;
        assertEquals(hybridModel.getMinPvalueThreshold(), deserialized.getMinPvalueThreshold(), delta);
        assertEquals(hybridModel.getMaxRankError(), deserialized.getMaxRankError(), delta);
        assertEquals(hybridModel.getMaxScore(), deserialized.getMaxScore(), delta);
        assertEquals(hybridModel.getNumLogNormalQuantiles(), deserialized.getNumLogNormalQuantiles());
        for (double score : new Random().doubles(1000L, 0.1, 3.9).toArray()) {
            assertEquals(hybridModel.grade(score), deserialized.grade(score), delta);
            assertEquals(hybridModel.confidence(), deserialized.confidence(), delta);
            hybridModel.update(score);
            deserialized.update(score);
        }
    }
}
