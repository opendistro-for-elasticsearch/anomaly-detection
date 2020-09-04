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

package com.amazon.opendistroforelasticsearch.ad.model;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.test.ESTestCase;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;

public class ValidationTests extends ESTestCase {

    public void testValidationSuggestedChanges() {
        assertEquals("others", ValidationSuggestedChanges.OTHERS.getName());
    }

    public void testValidationFailures() {
        assertEquals("missing", ValidationFailures.MISSING.getName());
    }

    public void testValidationResponse() throws IOException {
        Map<String, List<String>> failuresMap = new HashMap<>();
        Map<String, List<String>> suggestedChanges = new HashMap<>();
        failuresMap.put("missing", Arrays.asList("name"));
        suggestedChanges.put("detection_interval", Arrays.asList("200000"));
        ValidateResponse responseValidate = new ValidateResponse();
        responseValidate.setFailures(failuresMap);
        responseValidate.setSuggestedChanges(suggestedChanges);
        String validation = TestHelpers
            .xContentBuilderToString(responseValidate.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        System.out.println(validation);
        assertEquals("{\"failures\":{\"missing\":[\"name\"]}," + "\"suggestedChanges\":{\"detection_interval\":[\"200000\"]}}", validation);
        assertEquals(failuresMap, responseValidate.getFailures());
        assertEquals(suggestedChanges, responseValidate.getSuggestedChanges());
    }

}
