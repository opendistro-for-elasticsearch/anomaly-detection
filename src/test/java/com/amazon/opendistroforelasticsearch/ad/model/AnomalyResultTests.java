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
import java.util.Locale;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.test.ESTestCase;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;

public class AnomalyResultTests extends ESTestCase {

    public void testParseAnomalyDetector() throws IOException {
        AnomalyResult detectResult = TestHelpers.randomAnomalyDetectResult(0.8, randomAlphaOfLength(5), null);
        String detectResultString = TestHelpers
            .xContentBuilderToString(detectResult.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        detectResultString = detectResultString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyResult parsedDetectResult = AnomalyResult.parse(TestHelpers.parser(detectResultString));
        assertEquals("Parsing anomaly detect result doesn't work", detectResult, parsedDetectResult);
    }

    public void testParseAnomalyDetectorWithTaskId() throws IOException {
        AnomalyResult detectResult = TestHelpers.randomAnomalyDetectResult(0.8, randomAlphaOfLength(5), randomAlphaOfLength(5));
        String detectResultString = TestHelpers
            .xContentBuilderToString(detectResult.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        detectResultString = detectResultString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyResult parsedDetectResult = AnomalyResult.parse(TestHelpers.parser(detectResultString));
        assertEquals("Parsing anomaly detect result doesn't work", detectResult, parsedDetectResult);
    }

    public void testParseAnomalyDetectorWithEntity() throws IOException {
        AnomalyResult detectResult = TestHelpers.randomMultiEntityAnomalyDetectResult(0.8, 0.5);
        String detectResultString = TestHelpers
            .xContentBuilderToString(detectResult.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        detectResultString = detectResultString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyResult parsedDetectResult = AnomalyResult.parse(TestHelpers.parser(detectResultString));
        assertEquals("Parsing anomaly detect result doesn't work", detectResult, parsedDetectResult);
    }
}
