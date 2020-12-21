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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.TransportService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorRunner;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.feature.FeatureManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class PreviewAnomalyDetectorActionTests extends ESSingleNodeTestCase {
    private ActionListener<PreviewAnomalyDetectorResponse> response;
    private PreviewAnomalyDetectorTransportAction action;
    private AnomalyDetectorRunner runner;
    private ClusterService clusterService;
    private Task task;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        task = mock(Task.class);
        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.MAX_ANOMALY_FEATURES)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        runner = new AnomalyDetectorRunner(
            mock(ModelManager.class),
            mock(FeatureManager.class),
            AnomalyDetectorSettings.MAX_PREVIEW_RESULTS
        );
        action = new PreviewAnomalyDetectorTransportAction(
            Settings.EMPTY,
            mock(TransportService.class),
            clusterService,
            mock(ActionFilters.class),
            client(),
            runner,
            xContentRegistry()
        );
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    @Test
    public void testPreviewTransportActionWithNoFeature() throws IOException {
        // Detector with no feature, Preview should fail
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(Collections.emptyList());
        PreviewAnomalyDetectorRequest request = new PreviewAnomalyDetectorRequest(
            detector,
            detector.getDetectorId(),
            Instant.now(),
            Instant.now()
        );
        ActionListener<PreviewAnomalyDetectorResponse> previewResponse = new ActionListener<PreviewAnomalyDetectorResponse>() {
            @Override
            public void onResponse(PreviewAnomalyDetectorResponse response) {
                Assert.assertTrue(false);
            }

            @Override
            public void onFailure(Exception e) {
                Assert.assertTrue(e.getMessage().contains("Can't preview detector without feature"));
            }
        };
        action.doExecute(task, request, previewResponse);
    }

    @Test
    public void testPreviewTransportActionWithNoDetector() throws IOException {
        // When detectorId is null, preview should fail
        PreviewAnomalyDetectorRequest request = new PreviewAnomalyDetectorRequest(null, "", Instant.now(), Instant.now());
        ActionListener<PreviewAnomalyDetectorResponse> previewResponse = new ActionListener<PreviewAnomalyDetectorResponse>() {
            @Override
            public void onResponse(PreviewAnomalyDetectorResponse response) {
                Assert.assertTrue(false);
            }

            @Override
            public void onFailure(Exception e) {
                Assert.assertTrue(e.getMessage().contains("Wrong input, no detector id"));
            }
        };
        action.doExecute(task, request, previewResponse);
    }

    @Test
    public void testPreviewTransportActionWithDetectorID() throws IOException {
        // When AD index does not exist, cannot query the detector
        PreviewAnomalyDetectorRequest request = new PreviewAnomalyDetectorRequest(null, "1234", Instant.now(), Instant.now());
        ActionListener<PreviewAnomalyDetectorResponse> previewResponse = new ActionListener<PreviewAnomalyDetectorResponse>() {
            @Override
            public void onResponse(PreviewAnomalyDetectorResponse response) {
                Assert.assertTrue(false);
            }

            @Override
            public void onFailure(Exception e) {
                Assert.assertTrue(e.getMessage().contains("Could not execute get query to find detector"));
            }
        };
        action.doExecute(task, request, previewResponse);
    }

    @Test
    public void testPreviewTransportActionWithIndex() throws IOException {
        // When AD index exists, and detector does not exist
        PreviewAnomalyDetectorRequest request = new PreviewAnomalyDetectorRequest(null, "1234", Instant.now(), Instant.now());
        Settings indexSettings = Settings.builder().put("index.number_of_shards", 5).put("index.number_of_replicas", 1).build();
        CreateIndexRequest indexRequest = new CreateIndexRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX, indexSettings);
        client().admin().indices().create(indexRequest).actionGet();
        ActionListener<PreviewAnomalyDetectorResponse> previewResponse = new ActionListener<PreviewAnomalyDetectorResponse>() {
            @Override
            public void onResponse(PreviewAnomalyDetectorResponse response) {
                Assert.assertTrue(false);
            }

            @Override
            public void onFailure(Exception e) {
                Assert.assertTrue(e.getMessage().contains("Can't find anomaly detector with id:1234"));
            }
        };
        action.doExecute(task, request, previewResponse);
    }

    @Test
    public void testPreviewTransportActionNoContext() throws IOException {
        Client client = mock(Client.class);
        PreviewAnomalyDetectorTransportAction previewAction = new PreviewAnomalyDetectorTransportAction(
            Settings.EMPTY,
            mock(TransportService.class),
            clusterService,
            mock(ActionFilters.class),
            client,
            runner,
            xContentRegistry()
        );
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of("testKey", "testValue"), Instant.now());
        PreviewAnomalyDetectorRequest request = new PreviewAnomalyDetectorRequest(
            detector,
            detector.getDetectorId(),
            Instant.now(),
            Instant.now()
        );
        ActionListener<PreviewAnomalyDetectorResponse> previewResponse = new ActionListener<PreviewAnomalyDetectorResponse>() {
            @Override
            public void onResponse(PreviewAnomalyDetectorResponse response) {
                Assert.assertTrue(false);
            }

            @Override
            public void onFailure(Exception e) {
                Assert.assertTrue(e.getClass() == NullPointerException.class);
            }
        };
        previewAction.doExecute(task, request, previewResponse);
    }

    @Test
    public void testPreviewRequest() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of("testKey", "testValue"), Instant.now());
        PreviewAnomalyDetectorRequest request = new PreviewAnomalyDetectorRequest(
            detector,
            "1234",
            Instant.now().minusSeconds(60),
            Instant.now()
        );
        request.writeTo(out);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        PreviewAnomalyDetectorRequest newRequest = new PreviewAnomalyDetectorRequest(input);
        Assert.assertEquals(request.getDetectorId(), newRequest.getDetectorId());
        Assert.assertEquals(request.getStartTime(), newRequest.getStartTime());
        Assert.assertEquals(request.getEndTime(), newRequest.getEndTime());
        Assert.assertNotNull(newRequest.getDetector());
        Assert.assertNull(newRequest.validate());
    }

    @Test
    public void testPreviewResponse() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of("testKey", "testValue"), Instant.now());
        AnomalyResult result = TestHelpers.randomMultiEntityAnomalyDetectResult(0.8d, 0d);
        PreviewAnomalyDetectorResponse response = new PreviewAnomalyDetectorResponse(ImmutableList.of(result), detector);
        response.writeTo(out);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        PreviewAnomalyDetectorResponse newResponse = new PreviewAnomalyDetectorResponse(input);
        Assert.assertNotNull(newResponse.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
    }

    @Test
    public void testPreviewAction() throws Exception {
        Assert.assertNotNull(PreviewAnomalyDetectorAction.INSTANCE.name());
        Assert.assertEquals(PreviewAnomalyDetectorAction.INSTANCE.name(), PreviewAnomalyDetectorAction.NAME);
    }
}
