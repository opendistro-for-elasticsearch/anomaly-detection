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

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;

@RunWith(PowerMockRunner.class)
@PrepareForTest(IndexAnomalyDetectorRequest.class)
public class IndexAnomalyDetectorActionTests {
    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void testIndexRequest() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        AnomalyDetector detector = Mockito.mock(AnomalyDetector.class);
        Mockito.doNothing().when(detector).writeTo(out);
        IndexAnomalyDetectorRequest request = new IndexAnomalyDetectorRequest(
            "1234",
            4321,
            5678,
            WriteRequest.RefreshPolicy.NONE,
            detector,
            RestRequest.Method.PUT,
            TimeValue.timeValueSeconds(60),
            1000,
            10,
            5

        );
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        PowerMockito.whenNew(AnomalyDetector.class).withAnyArguments().thenReturn(detector);
        IndexAnomalyDetectorRequest newRequest = new IndexAnomalyDetectorRequest(input);
        Assert.assertEquals(request.getDetectorID(), newRequest.getDetectorID());

    }

    @Test
    public void testIndexResponse() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        IndexAnomalyDetectorResponse response = new IndexAnomalyDetectorResponse("1234", 56, 78, 90, RestStatus.OK);
        response.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        IndexAnomalyDetectorResponse newResponse = new IndexAnomalyDetectorResponse(input);
        Assert.assertEquals(response.getId(), newResponse.getId());
    }
}
