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

package com.amazon.opendistroforelasticsearch.ad.indices;

import static org.mockito.Mockito.mock;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import com.amazon.opendistroforelasticsearch.ad.util.Throttler;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.time.Clock;
import java.util.HashSet;
import java.util.Set;

public class AnomalyDetectionIndicesTests extends ESIntegTestCase {

    private AnomalyDetectionIndices indices;
    private ClusterSettings clusterSetting;
    private ClientUtil requestUtil;
    private Settings settings;
    private ClusterService clusterService;
    private Client client;
    private ThreadPool context;

    @Before
    public void setup() {
        settings = Settings
            .builder()
            .put("opendistro.anomaly_detection.ad_result_history_rollover_period", TimeValue.timeValueHours(12))
            .put("opendistro.anomaly_detection.ad_result_history_max_age", TimeValue.timeValueHours(24))
            .put("opendistro.anomaly_detection.ad_result_history_max_docs", 10000L)
            .put("opendistro.anomaly_detection.request_timeout", TimeValue.timeValueSeconds(10))
            .build();

        Set<Setting<?>> clusterSettings = new HashSet<>();
        clusterSettings.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettings.add(AnomalyDetectorSettings.AD_RESULT_HISTORY_INDEX_MAX_AGE);
        clusterSettings.add(AnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS);
        clusterSettings.add(AnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD);
        clusterSettings.add(AnomalyDetectorSettings.REQUEST_TIMEOUT);
        clusterSetting = new ClusterSettings(settings, clusterSettings);
        clusterService = TestHelpers.createClusterService(client().threadPool(), clusterSetting);
        context = TestHelpers.createThreadPool();
        client = mock(Client.class);
        Clock clock = Clock.systemUTC();
        Throttler throttler = new Throttler(clock);
        requestUtil = new ClientUtil(settings, client, throttler, context);
        indices = new AnomalyDetectionIndices(client(), clusterService, client().threadPool(), settings, requestUtil);
    }

    public void testAnomalyDetectorIndexNotExists() {
        boolean exists = indices.doesAnomalyDetectorIndexExist();
        assertFalse(exists);
    }

    public void testAnomalyDetectorIndexExists() throws IOException {
        indices.initAnomalyDetectorIndexIfAbsent(TestHelpers.createActionListener(response -> {
            boolean acknowledged = response.isAcknowledged();
            assertTrue(acknowledged);
        }, failure -> { throw new RuntimeException("should not recreate index"); }));
        TestHelpers.waitForIndexCreationToComplete(client(), AnomalyDetector.ANOMALY_DETECTORS_INDEX);
    }

    public void testAnomalyDetectorIndexExistsAndNotRecreate() throws IOException {
        indices
            .initAnomalyDetectorIndexIfAbsent(
                TestHelpers
                    .createActionListener(
                        response -> response.isAcknowledged(),
                        failure -> { throw new RuntimeException("should not recreate index"); }
                    )
            );
        TestHelpers.waitForIndexCreationToComplete(client(), AnomalyDetector.ANOMALY_DETECTORS_INDEX);
        if (client().admin().indices().prepareExists(AnomalyDetector.ANOMALY_DETECTORS_INDEX).get().isExists()) {
            indices
                .initAnomalyDetectorIndexIfAbsent(
                    TestHelpers
                        .createActionListener(
                            response -> {
                                throw new RuntimeException("should not recreate index " + AnomalyDetector.ANOMALY_DETECTORS_INDEX);
                            },
                            failure -> {
                                throw new RuntimeException("should not recreate index " + AnomalyDetector.ANOMALY_DETECTORS_INDEX);
                            }
                        )
                );
        }
    }

    public void testAnomalyResultIndexNotExists() {
        boolean exists = indices.doesAnomalyResultIndexExist();
        assertFalse(exists);
    }

    public void testAnomalyResultIndexExists() throws IOException {
        indices.initAnomalyResultIndexIfAbsent(TestHelpers.createActionListener(response -> {
            boolean acknowledged = response.isAcknowledged();
            assertTrue(acknowledged);
        }, failure -> { throw new RuntimeException("should not recreate index"); }));
        TestHelpers.waitForIndexCreationToComplete(client(), AnomalyResult.ANOMALY_RESULT_INDEX);
    }

    public void testAnomalyResultIndexExistsAndNotRecreate() throws IOException {
        indices
            .initAnomalyResultIndexIfAbsent(
                TestHelpers
                    .createActionListener(
                        response -> response.isAcknowledged(),
                        failure -> { throw new RuntimeException("should not recreate index"); }
                    )
            );
        TestHelpers.waitForIndexCreationToComplete(client(), AnomalyResult.ANOMALY_RESULT_INDEX);
        if (client().admin().indices().prepareExists(AnomalyResult.ANOMALY_RESULT_INDEX).get().isExists()) {
            indices
                .initAnomalyResultIndexIfAbsent(
                    TestHelpers
                        .createActionListener(
                            response -> { throw new RuntimeException("should not recreate index " + AnomalyResult.ANOMALY_RESULT_INDEX); },
                            failure -> { throw new RuntimeException("should not recreate index " + AnomalyResult.ANOMALY_RESULT_INDEX); }
                        )
                );
        }
    }

    private void createRandomDetector(String indexName) throws IOException {
        // creates a random anomaly detector and indexes it
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null);

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        detector.toXContent(xContentBuilder, RestHandlerUtils.XCONTENT_WITH_TYPE);

        IndexResponse indexResponse = client().index(new IndexRequest(indexName).source(xContentBuilder)).actionGet();
        assertEquals("Doc was not created", RestStatus.CREATED, indexResponse.status());
    }
}
