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

package com.amazon.opendistroforelasticsearch.ad.model;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Locale;

public class FeatureTests extends ESTestCase {

    public void testParseFeature() throws IOException {
        Feature feature = TestHelpers.randomFeature();
        String featureString = TestHelpers.xContentBuilderToString(feature.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        featureString = featureString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        Feature parsedFeature = Feature.parse(TestHelpers.parser(featureString));
        assertEquals("Parsing feature doesn't work", feature, parsedFeature);
    }

    public void testNullName() throws Exception {
        TestHelpers
            .assertFailWith(
                IllegalArgumentException.class,
                () -> new Feature(randomAlphaOfLength(5), null, true, TestHelpers.randomAggregation())
            );
    }

}
