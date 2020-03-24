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

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.io.IOException;

import static com.amazon.opendistroforelasticsearch.ad.TestHelpers.builder;
import static com.amazon.opendistroforelasticsearch.ad.TestHelpers.randomFeature;

public class RestHandlerUtilsTests extends ESTestCase {

    public void testGetSourceContext() {
        RestRequest request = new FakeRestRequest();
        FetchSourceContext context = RestHandlerUtils.getSourceContext(request);
        assertArrayEquals(new String[] { "ui_metadata" }, context.excludes());
    }

    public void testGetSourceContextForKibana() {
        FakeRestRequest.Builder builder = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY);
        builder.withHeaders(ImmutableMap.of("User-Agent", ImmutableList.of("Kibana", randomAlphaOfLength(10))));
        FetchSourceContext context = RestHandlerUtils.getSourceContext(builder.build());
        assertNull(context);
    }

    public void testCreateXContentParser() throws IOException {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = new FakeRestChannel(request, false, 1);
        XContentBuilder builder = builder().startObject().field("test", "value").endObject();
        BytesReference bytesReference = BytesReference.bytes(builder);
        XContentParser parser = RestHandlerUtils.createXContentParser(channel, bytesReference);
        parser.close();
    }

    public void testValidateAnomalyDetectorWithNullFeatures() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(null);
        String error = RestHandlerUtils.validateAnomalyDetector(detector, 1);
        assertNull(error);
    }

    public void testValidateAnomalyDetectorWithTooManyFeatures() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableList.of(randomFeature(), randomFeature()));
        String error = RestHandlerUtils.validateAnomalyDetector(detector, 1);
        assertEquals("Can't create anomaly features more than 1", error);
    }

    public void testValidateAnomalyDetectorWithDuplicateFeatureNames() throws IOException {
        String featureName = randomAlphaOfLength(5);
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetector(
                ImmutableList.of(randomFeature(featureName, randomAlphaOfLength(5)), randomFeature(featureName, randomAlphaOfLength(5)))
            );
        String error = RestHandlerUtils.validateAnomalyDetector(detector, 2);
        assertEquals("Detector has duplicate feature names: " + featureName + "\n", error);
    }

    public void testValidateAnomalyDetectorWithDuplicateAggregationNames() throws IOException {
        String aggregationName = randomAlphaOfLength(5);
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetector(
                ImmutableList
                    .of(randomFeature(randomAlphaOfLength(5), aggregationName), randomFeature(randomAlphaOfLength(5), aggregationName))
            );
        String error = RestHandlerUtils.validateAnomalyDetector(detector, 2);
        assertEquals("Detector has duplicate feature aggregation query names: " + aggregationName, error);
    }
}
