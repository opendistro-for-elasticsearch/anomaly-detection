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

package com.amazon.opendistroforelasticsearch.ad.transport;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazon.opendistroforelasticsearch.ad.stats.ADStatsResponse;

public class StatsAnomalyDetectorActionTests extends ESTestCase {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void testStatsAction() {
        Assert.assertNotNull(StatsAnomalyDetectorAction.INSTANCE.name());
        Assert.assertEquals(StatsAnomalyDetectorAction.INSTANCE.name(), StatsAnomalyDetectorAction.NAME);
    }

    @Test
    public void testStatsResponse() throws IOException {
        ADStatsResponse adStatsResponse = new ADStatsResponse();
        Map<String, Object> testClusterStats = new HashMap<>();
        testClusterStats.put("test_response", 1);
        adStatsResponse.setClusterStats(testClusterStats);
        List<ADStatsNodeResponse> responses = Collections.emptyList();
        List<FailedNodeException> failures = Collections.emptyList();
        ADStatsNodesResponse adStatsNodesResponse = new ADStatsNodesResponse(ClusterName.DEFAULT, responses, failures);
        adStatsResponse.setADStatsNodesResponse(adStatsNodesResponse);

        StatsAnomalyDetectorResponse response = new StatsAnomalyDetectorResponse(adStatsResponse);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        StatsAnomalyDetectorResponse newResponse = new StatsAnomalyDetectorResponse(input);
        assertNotNull(newResponse);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder = newResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = createParser(builder);
        assertEquals(1, parser.map().get("test_response"));
    }
}
