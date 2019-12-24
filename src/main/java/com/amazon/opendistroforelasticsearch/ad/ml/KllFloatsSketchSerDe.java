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

import java.lang.reflect.Type;
import java.util.Base64;

import com.google.gson.JsonDeserializer;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import com.google.gson.JsonSerializationContext;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.kll.KllFloatsSketch;

/**
 * Serializes/deserailizes KllFloatsSketch.
 *
 * A sketch is serialized to a byte array and then encoded in Base64.
 */
public class KllFloatsSketchSerDe implements JsonSerializer<KllFloatsSketch>, JsonDeserializer<KllFloatsSketch> {

    @Override
    public JsonElement serialize(KllFloatsSketch src, Type typeOfSrc, JsonSerializationContext context) {
        return new JsonPrimitive(Base64.getEncoder().encodeToString(src.toByteArray()));
    }

    @Override
    public KllFloatsSketch deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) {
        return KllFloatsSketch.heapify(Memory.wrap(Base64.getDecoder().decode(json.getAsString())));
    }
}
