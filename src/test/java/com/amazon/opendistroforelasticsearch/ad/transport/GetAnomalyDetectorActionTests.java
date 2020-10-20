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

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
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
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfile;

@RunWith(PowerMockRunner.class)
@PrepareForTest(GetAnomalyDetectorResponse.class)
public class GetAnomalyDetectorActionTests {
    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void testGetRequest() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        GetAnomalyDetectorRequest request = new GetAnomalyDetectorRequest("1234", 4321, false, "nonempty", "", false, null);
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        GetAnomalyDetectorRequest newRequest = new GetAnomalyDetectorRequest(input);
        Assert.assertEquals(request.getDetectorID(), newRequest.getDetectorID());

    }

    @Test
    public void testGetResponse() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        AnomalyDetector detector = Mockito.mock(AnomalyDetector.class);
        AnomalyDetectorJob detectorJob = Mockito.mock(AnomalyDetectorJob.class);
        Mockito.doNothing().when(detector).writeTo(out);
        GetAnomalyDetectorResponse response = new GetAnomalyDetectorResponse(
            1234,
            "4567",
            9876,
            2345,
            detector,
            detectorJob,
            false,
            RestStatus.OK,
            Mockito.mock(DetectorProfile.class),
            null,
            false
        );
        response.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        PowerMockito.whenNew(AnomalyDetector.class).withAnyArguments().thenReturn(detector);
        GetAnomalyDetectorResponse newResponse = new GetAnomalyDetectorResponse(input);
        Assert.assertNotNull(newResponse);
    }
}
