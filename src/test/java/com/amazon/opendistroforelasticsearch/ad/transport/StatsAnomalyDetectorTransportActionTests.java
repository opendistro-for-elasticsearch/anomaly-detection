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

import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.XCONTENT_WITH_TYPE;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorType;
import com.amazon.opendistroforelasticsearch.ad.stats.InternalStatNames;
import com.amazon.opendistroforelasticsearch.ad.stats.StatNames;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class StatsAnomalyDetectorTransportActionTests extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(AnomalyDetectorPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singletonList(AnomalyDetectorPlugin.class);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        createTestDetector();
    }

    private void createTestDetector() throws IOException {
        CreateIndexResponse createIndexResponse = TestHelpers
            .createIndex(admin(), AnomalyDetector.ANOMALY_DETECTORS_INDEX, AnomalyDetectionIndices.getAnomalyDetectorMappings());
        assertEquals(true, createIndexResponse.isAcknowledged());

        IndexRequest indexRequest = new IndexRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .source(
                TestHelpers
                    .randomAnomalyDetector(ImmutableMap.of(), Instant.now())
                    .toXContent(XContentFactory.jsonBuilder(), XCONTENT_WITH_TYPE)
            );
        IndexResponse indexResponse = client().index(indexRequest).actionGet(5_000);
        assertEquals(RestStatus.CREATED, indexResponse.status());

        indexRequest = new IndexRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .source(
                TestHelpers
                    .randomAnomalyDetector(
                        ImmutableList.of(TestHelpers.randomFeature()),
                        ImmutableMap.of(),
                        Instant.now(),
                        AnomalyDetectorType.HISTORICAL_SINGLE_ENTITY.name(),
                        TestHelpers.randomDetectionDateRange(),
                        true
                    )
                    .toXContent(XContentFactory.jsonBuilder(), XCONTENT_WITH_TYPE)
            );
        indexResponse = client().index(indexRequest).actionGet(5_000);
        assertEquals(RestStatus.CREATED, indexResponse.status());
    }

    public void testStatsAnomalyDetectorWithNodeLevelStats() {
        ADStatsRequest adStatsRequest = new ADStatsRequest(clusterService().localNode());
        adStatsRequest.addStat(InternalStatNames.JVM_HEAP_USAGE.getName());
        StatsAnomalyDetectorResponse response = client().execute(StatsAnomalyDetectorAction.INSTANCE, adStatsRequest).actionGet(5_000);
        assertEquals(1, response.getAdStatsResponse().getADStatsNodesResponse().getNodes().size());
        assertTrue(
            response
                .getAdStatsResponse()
                .getADStatsNodesResponse()
                .getNodes()
                .get(0)
                .getStatsMap()
                .containsKey(InternalStatNames.JVM_HEAP_USAGE.getName())
        );
    }

    public void testStatsAnomalyDetectorWithClusterLevelStats() throws IOException {
        ADStatsRequest adStatsRequest = new ADStatsRequest(clusterService().localNode());
        adStatsRequest.addStat(StatNames.DETECTOR_COUNT.getName());
        adStatsRequest.addStat(StatNames.HISTORICAL_SINGLE_ENTITY_DETECTOR_COUNT.getName());
        StatsAnomalyDetectorResponse response = client().execute(StatsAnomalyDetectorAction.INSTANCE, adStatsRequest).actionGet(5_000);
        assertEquals(1, response.getAdStatsResponse().getADStatsNodesResponse().getNodes().size());
        Map<String, Object> statsMap = response.getAdStatsResponse().getADStatsNodesResponse().getNodes().get(0).getStatsMap();
        Map<String, Object> clusterStats = response.getAdStatsResponse().getClusterStats();
        assertEquals(0, statsMap.size());
        assertEquals(2L, clusterStats.get(StatNames.DETECTOR_COUNT.getName()));
        assertEquals(1L, clusterStats.get(StatNames.HISTORICAL_SINGLE_ENTITY_DETECTOR_COUNT.getName()));
    }

    public void testStatsAnomalyDetectorWithDetectorCount() throws IOException {
        ADStatsRequest adStatsRequest = new ADStatsRequest(clusterService().localNode());
        adStatsRequest.addStat(StatNames.DETECTOR_COUNT.getName());
        StatsAnomalyDetectorResponse response = client().execute(StatsAnomalyDetectorAction.INSTANCE, adStatsRequest).actionGet(5_000);
        assertEquals(1, response.getAdStatsResponse().getADStatsNodesResponse().getNodes().size());
        Map<String, Object> statsMap = response.getAdStatsResponse().getADStatsNodesResponse().getNodes().get(0).getStatsMap();
        Map<String, Object> clusterStats = response.getAdStatsResponse().getClusterStats();
        assertEquals(0, statsMap.size());
        assertEquals(2L, clusterStats.get(StatNames.DETECTOR_COUNT.getName()));
        assertFalse(clusterStats.containsKey(StatNames.HISTORICAL_SINGLE_ENTITY_DETECTOR_COUNT.getName()));
    }

    public void testStatsAnomalyDetectorWithHistoricalDetectorCount() throws IOException {
        ADStatsRequest adStatsRequest = new ADStatsRequest(clusterService().localNode());
        adStatsRequest.addStat(StatNames.HISTORICAL_SINGLE_ENTITY_DETECTOR_COUNT.getName());
        StatsAnomalyDetectorResponse response = client().execute(StatsAnomalyDetectorAction.INSTANCE, adStatsRequest).actionGet(5_000);
        assertEquals(1, response.getAdStatsResponse().getADStatsNodesResponse().getNodes().size());
        Map<String, Object> statsMap = response.getAdStatsResponse().getADStatsNodesResponse().getNodes().get(0).getStatsMap();
        Map<String, Object> clusterStats = response.getAdStatsResponse().getClusterStats();
        assertEquals(0, statsMap.size());
        assertEquals(1L, clusterStats.get(StatNames.HISTORICAL_SINGLE_ENTITY_DETECTOR_COUNT.getName()));
        assertFalse(clusterStats.containsKey(StatNames.DETECTOR_COUNT.getName()));
    }

}
