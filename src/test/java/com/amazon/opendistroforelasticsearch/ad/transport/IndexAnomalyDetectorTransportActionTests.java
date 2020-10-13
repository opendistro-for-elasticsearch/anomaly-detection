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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;

public class IndexAnomalyDetectorTransportActionTests extends ESIntegTestCase {
    private IndexAnomalyDetectorTransportAction action;
    private Task task;
    private IndexAnomalyDetectorRequest request;
    private ActionListener<IndexAnomalyDetectorResponse> response;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        action = new IndexAnomalyDetectorTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            client(),
            clusterService(),
            indexSettings(),
            mock(AnomalyDetectionIndices.class),
            xContentRegistry()
        );
        task = mock(Task.class);
        request = new IndexAnomalyDetectorRequest(
            "1234",
            4567,
            7890,
            WriteRequest.RefreshPolicy.IMMEDIATE,
            mock(AnomalyDetector.class),
            RestRequest.Method.PUT
        );
        response = new ActionListener<IndexAnomalyDetectorResponse>() {
            @Override
            public void onResponse(IndexAnomalyDetectorResponse indexResponse) {
                Assert.assertTrue(true);
            }

            @Override
            public void onFailure(Exception e) {
                Assert.assertTrue(true);
            }
        };
    }

    @Test
    public void testIndexTransportAction() {
        action.doExecute(task, request, response);
    }

    @Test
    public void testIndexDetectorAction() {
        Assert.assertNotNull(IndexAnomalyDetectorAction.INSTANCE.name());
        Assert.assertEquals(IndexAnomalyDetectorAction.INSTANCE.name(), IndexAnomalyDetectorAction.NAME);
    }
}
