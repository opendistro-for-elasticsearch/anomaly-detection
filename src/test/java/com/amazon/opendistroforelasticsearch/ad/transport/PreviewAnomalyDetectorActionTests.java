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
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Assert;
import org.junit.Test;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class PreviewAnomalyDetectorActionTests extends ESSingleNodeTestCase {

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
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
