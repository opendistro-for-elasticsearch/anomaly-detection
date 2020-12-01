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

import java.time.Instant;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
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
@PrepareForTest({ PreviewAnomalyDetectorRequest.class, PreviewAnomalyDetectorResponse.class })
public class PreviewAnomalyDetectorActionTests {
    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void testPreviewRequest() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        AnomalyDetector detector = Mockito.mock(AnomalyDetector.class);
        Mockito.doNothing().when(detector).writeTo(out);
        PreviewAnomalyDetectorRequest request = new PreviewAnomalyDetectorRequest(
            detector,
            "1234",
            Instant.now().minusSeconds(60),
            Instant.now()
        );
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        PowerMockito.whenNew(AnomalyDetector.class).withAnyArguments().thenReturn(detector);
        PreviewAnomalyDetectorRequest newRequest = new PreviewAnomalyDetectorRequest(input);
        Assert.assertEquals(request.getDetectorId(), newRequest.getDetectorId());
    }

    @Test
    public void testPreviewAction() throws Exception {
        Assert.assertNotNull(PreviewAnomalyDetectorAction.INSTANCE.name());
        Assert.assertEquals(PreviewAnomalyDetectorAction.INSTANCE.name(), PreviewAnomalyDetectorAction.NAME);
    }
}
