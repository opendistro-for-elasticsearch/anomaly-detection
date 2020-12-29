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

package com.amazon.opendistroforelasticsearch.ad;

import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.XCONTENT_WITH_TYPE;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;

public abstract class ADIntegTestCase extends ESIntegTestCase {

    private long timeout = 5_000;

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
    }

    public void createDetectors(List<AnomalyDetector> detectors, boolean createIndexFirst) throws IOException {
        if (createIndexFirst) {
            createIndex(AnomalyDetector.ANOMALY_DETECTORS_INDEX, AnomalyDetectionIndices.getAnomalyDetectorMappings());
        }

        for (AnomalyDetector detector : detectors) {
            indexDoc(AnomalyDetector.ANOMALY_DETECTORS_INDEX, detector.toXContent(XContentFactory.jsonBuilder(), XCONTENT_WITH_TYPE));
        }
    }

    public void createDetectorIndex() throws IOException {
        createIndex(AnomalyDetector.ANOMALY_DETECTORS_INDEX, AnomalyDetectionIndices.getAnomalyDetectorMappings());
    }

    public String createDetectors(AnomalyDetector detector) throws IOException {
        return indexDoc(AnomalyDetector.ANOMALY_DETECTORS_INDEX, detector.toXContent(XContentFactory.jsonBuilder(), XCONTENT_WITH_TYPE));
    }

    public void createIndex(String indexName, String mappings) {
        CreateIndexResponse createIndexResponse = TestHelpers.createIndex(admin(), indexName, mappings);
        assertEquals(true, createIndexResponse.isAcknowledged());
    }

    public String indexDoc(String indexName, XContentBuilder source) {
        IndexRequest indexRequest = new IndexRequest(indexName).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).source(source);
        IndexResponse indexResponse = client().index(indexRequest).actionGet(timeout);
        assertEquals(RestStatus.CREATED, indexResponse.status());
        return indexResponse.getId();
    }

    public String indexDoc(String indexName, Map<String, ?> source) {
        IndexRequest indexRequest = new IndexRequest(indexName).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).source(source);
        IndexResponse indexResponse = client().index(indexRequest).actionGet(timeout);
        assertEquals(RestStatus.CREATED, indexResponse.status());
        return indexResponse.getId();
    }

    public GetResponse getDoc(String indexName, String id) {
        GetRequest getRequest = new GetRequest(indexName).id(id);
        return client().get(getRequest).actionGet(timeout);
    }
}
