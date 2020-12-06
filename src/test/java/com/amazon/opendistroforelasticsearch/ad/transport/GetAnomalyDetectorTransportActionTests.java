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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;

public class GetAnomalyDetectorTransportActionTests extends ESIntegTestCase {
    private GetAnomalyDetectorTransportAction action;
    private Task task;
    private ActionListener<GetAnomalyDetectorResponse> response;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        action = new GetAnomalyDetectorTransportAction(
            Mockito.mock(TransportService.class),
            Mockito.mock(DiscoveryNodeFilterer.class),
            Mockito.mock(ActionFilters.class),
            clusterService,
            client(),
            Settings.EMPTY,
            xContentRegistry()
        );
        task = Mockito.mock(Task.class);
        response = new ActionListener<GetAnomalyDetectorResponse>() {
            @Override
            public void onResponse(GetAnomalyDetectorResponse getResponse) {
                // When no detectors exist, get response is not generated
                assertTrue(true);
            }

            @Override
            public void onFailure(Exception e) {}
        };
    }

    @Test
    public void testGetTransportAction() throws IOException {
        GetAnomalyDetectorRequest getAnomalyDetectorRequest = new GetAnomalyDetectorRequest(
            "1234",
            4321,
            false,
            "nonempty",
            "",
            false,
            null
        );
        action.doExecute(task, getAnomalyDetectorRequest, response);
    }

    @Test
    public void testGetTransportActionWithReturnJob() throws IOException {
        GetAnomalyDetectorRequest getAnomalyDetectorRequest = new GetAnomalyDetectorRequest("1234", 4321, true, "", "abcd", false, null);
        action.doExecute(task, getAnomalyDetectorRequest, response);
    }

    @Test
    public void testGetAction() {
        Assert.assertNotNull(GetAnomalyDetectorAction.INSTANCE.name());
        Assert.assertEquals(GetAnomalyDetectorAction.INSTANCE.name(), GetAnomalyDetectorAction.NAME);
    }
}
