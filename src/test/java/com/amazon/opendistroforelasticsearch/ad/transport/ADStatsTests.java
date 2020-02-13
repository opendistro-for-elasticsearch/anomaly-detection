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

package com.amazon.opendistroforelasticsearch.ad.transport;

import com.amazon.opendistroforelasticsearch.ad.common.exception.JsonPathNotFoundException;
import com.amazon.opendistroforelasticsearch.ad.stats.StatNames;
import org.elasticsearch.Version;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;
import test.com.amazon.opendistroforelasticsearch.ad.util.JsonDeserializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ADStatsTests extends ESTestCase {
    String node1, nodeName1, clusterName;
    Map<String, Object> clusterStats;
    DiscoveryNode discoveryNode1;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        node1 = "node1";
        nodeName1 = "nodename1";
        clusterName = "test-cluster-name";
        discoveryNode1 = new DiscoveryNode(
            nodeName1,
            node1,
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );
        clusterStats = new HashMap<>();
    }

    @Test
    public void testADStatsNodeRequest() throws IOException {
        ADStatsNodeRequest adStatsNodeRequest1 = new ADStatsNodeRequest();
        assertNull("ADStatsNodeRequest default constructor failed", adStatsNodeRequest1.getADStatsRequest());

        ADStatsRequest adStatsRequest = new ADStatsRequest();
        ADStatsNodeRequest adStatsNodeRequest2 = new ADStatsNodeRequest(adStatsRequest);
        assertEquals("ADStatsNodeRequest has the wrong ADStatsRequest", adStatsNodeRequest2.getADStatsRequest(), adStatsRequest);

        // Test serialization
        BytesStreamOutput output = new BytesStreamOutput();
        adStatsNodeRequest2.writeTo(output);
        StreamInput streamInput = output.bytes().streamInput();
        adStatsNodeRequest1.readFrom(streamInput);
        assertEquals(
            "readStats failed",
            adStatsNodeRequest2.getADStatsRequest().getStatsToBeRetrieved(),
            adStatsNodeRequest1.getADStatsRequest().getStatsToBeRetrieved()
        );
    }

    @Test
    public void testADStatsNodeResponse() throws IOException, JsonPathNotFoundException {
        Map<String, Object> stats = new HashMap<String, Object>() {
            {
                put("testKey", "testValue");
            }
        };

        // Test serialization
        ADStatsNodeResponse adStatsNodeResponse = new ADStatsNodeResponse(discoveryNode1, stats);
        BytesStreamOutput output = new BytesStreamOutput();
        adStatsNodeResponse.writeTo(output);
        StreamInput streamInput = output.bytes().streamInput();
        ADStatsNodeResponse readResponse = ADStatsNodeResponse.readStats(streamInput);
        assertEquals("readStats failed", readResponse.getStatsMap(), adStatsNodeResponse.getStatsMap());

        // Test toXContent
        XContentBuilder builder = jsonBuilder();
        adStatsNodeResponse.toXContent(builder.startObject(), ToXContent.EMPTY_PARAMS).endObject();
        String json = Strings.toString(builder);

        for (Map.Entry<String, Object> stat : stats.entrySet()) {
            assertEquals("toXContent does not work", JsonDeserializer.getTextValue(json, stat.getKey()), stat.getValue());
        }
    }

    @Test
    public void testADStatsRequest() throws IOException {
        List<String> allStats = Arrays.stream(StatNames.values()).map(StatNames::getName).collect(Collectors.toList());
        ADStatsRequest adStatsRequest = new ADStatsRequest();

        // Test clear()
        adStatsRequest.clear();
        for (String stat : allStats) {
            assertTrue("clear() fails", !adStatsRequest.getStatsToBeRetrieved().contains(stat));
        }

        // Test all()
        adStatsRequest.addAll(new HashSet<>(allStats));
        for (String stat : allStats) {
            assertTrue("all() fails", adStatsRequest.getStatsToBeRetrieved().contains(stat));
        }

        // Test add stat
        adStatsRequest.clear();
        adStatsRequest.addStat(StatNames.AD_EXECUTE_REQUEST_COUNT.getName());
        assertTrue("addStat fails", adStatsRequest.getStatsToBeRetrieved().contains(StatNames.AD_EXECUTE_REQUEST_COUNT.getName()));

        // Test Serialization
        BytesStreamOutput output = new BytesStreamOutput();
        adStatsRequest.writeTo(output);
        StreamInput streamInput = output.bytes().streamInput();
        ADStatsRequest readRequest = new ADStatsRequest(streamInput);
        assertEquals("Serialization fails", readRequest.getStatsToBeRetrieved(), adStatsRequest.getStatsToBeRetrieved());
    }

    @Test
    public void testADStatsResponse() throws IOException, JsonPathNotFoundException {
        Map<String, Object> nodeStats = new HashMap<String, Object>() {
            {
                put("testNodeKey", "testNodeValue");
            }
        };

        Map<String, Object> clusterStats = new HashMap<String, Object>() {
            {
                put("testClusterKey", "testClusterValue");
            }
        };
        ADStatsNodeResponse adStatsNodeResponse = new ADStatsNodeResponse(discoveryNode1, nodeStats);
        List<ADStatsNodeResponse> adStatsNodeResponses = Collections.singletonList(adStatsNodeResponse);
        List<FailedNodeException> failures = Collections.emptyList();
        ADStatsResponse adStatsResponse = new ADStatsResponse(new ClusterName(clusterName), adStatsNodeResponses, failures, clusterStats);

        // Test toXContent
        XContentBuilder builder = jsonBuilder();
        adStatsResponse.toXContent(builder.startObject(), ToXContent.EMPTY_PARAMS).endObject();
        String json = Strings.toString(builder);

        logger.info("JSON: " + json);

        // clusterStats
        for (Map.Entry<String, Object> stat : clusterStats.entrySet()) {
            assertEquals("toXContent does not work for cluster stats", JsonDeserializer.getTextValue(json, stat.getKey()), stat.getValue());
        }

        // nodeStats
        String nodesJson = JsonDeserializer.getChildNode(json, "nodes").toString();
        String node1Json = JsonDeserializer.getChildNode(nodesJson, node1).toString();

        for (Map.Entry<String, Object> stat : nodeStats.entrySet()) {
            assertEquals(
                "toXContent does not work for node stats",
                JsonDeserializer.getTextValue(node1Json, stat.getKey()),
                stat.getValue()
            );
        }

        // Test Serialization
        BytesStreamOutput output = new BytesStreamOutput();

        adStatsResponse.writeTo(output);
        StreamInput streamInput = output.bytes().streamInput();
        ADStatsResponse readRequest = new ADStatsResponse(streamInput);

        builder = jsonBuilder();
        String readJson = Strings.toString(readRequest.toXContent(builder.startObject(), ToXContent.EMPTY_PARAMS).endObject());
        assertEquals("Serialization fails", readJson, json);
    }
}
