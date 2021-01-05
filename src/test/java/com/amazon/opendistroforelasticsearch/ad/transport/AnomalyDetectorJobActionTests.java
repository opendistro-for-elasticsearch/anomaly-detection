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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.task.ADTaskManager;
import com.amazon.opendistroforelasticsearch.commons.ConfigConstants;

public class AnomalyDetectorJobActionTests extends ESIntegTestCase {
    private AnomalyDetectorJobTransportAction action;
    private Task task;
    private AnomalyDetectorJobRequest request;
    private ActionListener<AnomalyDetectorJobResponse> response;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES)))
        );

        Settings build = Settings.builder().build();
        ThreadContext threadContext = new ThreadContext(build);
        threadContext.putTransient(ConfigConstants.OPENDISTRO_SECURITY_USER_INFO_THREAD_CONTEXT, "alice|odfe,aes|engineering,operations");
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        Client client = mock(Client.class);
        org.elasticsearch.threadpool.ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(mockThreadPool);
        when(mockThreadPool.getThreadContext()).thenReturn(threadContext);

        action = new AnomalyDetectorJobTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            client,
            clusterService,
            indexSettings(),
            mock(AnomalyDetectionIndices.class),
            xContentRegistry(),
            mock(ADTaskManager.class)
        );
        task = mock(Task.class);
        request = new AnomalyDetectorJobRequest("1234", 4567, 7890, "_start");
        response = new ActionListener<AnomalyDetectorJobResponse>() {
            @Override
            public void onResponse(AnomalyDetectorJobResponse adResponse) {
                // Will not be called as there is no detector
                Assert.assertTrue(false);
            }

            @Override
            public void onFailure(Exception e) {
                // Will not be called as there is no detector
                Assert.assertTrue(true);
            }
        };
    }

    @Test
    public void testStartAdJobTransportAction() {
        action.doExecute(task, request, response);
    }

    @Test
    public void testStopAdJobTransportAction() {
        AnomalyDetectorJobRequest stopRequest = new AnomalyDetectorJobRequest("1234", 4567, 7890, "_stop");
        action.doExecute(task, stopRequest, response);
    }

    @Test
    public void testAdJobAction() {
        Assert.assertNotNull(AnomalyDetectorJobAction.INSTANCE.name());
        Assert.assertEquals(AnomalyDetectorJobAction.INSTANCE.name(), AnomalyDetectorJobAction.NAME);
    }

    @Test
    public void testAdJobRequest() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        AnomalyDetectorJobRequest newRequest = new AnomalyDetectorJobRequest(input);
        Assert.assertEquals(request.getDetectorID(), newRequest.getDetectorID());
    }

    @Test
    public void testAdJobResponse() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        AnomalyDetectorJobResponse response = new AnomalyDetectorJobResponse("1234", 45, 67, 890, RestStatus.OK);
        response.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        AnomalyDetectorJobResponse newResponse = new AnomalyDetectorJobResponse(input);
        Assert.assertEquals(response.getId(), newResponse.getId());
    }
}
