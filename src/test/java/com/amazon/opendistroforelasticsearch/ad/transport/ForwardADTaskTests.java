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
import java.time.Instant;
import java.util.Collection;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.google.common.collect.ImmutableMap;

public class ForwardADTaskTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, AnomalyDetectorPlugin.class);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    public void testForwardADTaskRequest() throws IOException {
        ForwardADTaskRequest request = new ForwardADTaskRequest(
            TestHelpers.randomAnomalyDetector(ImmutableMap.of(), Instant.now()),
            TestHelpers.randomUser()
        );
        testForwardADTaskRequest(request);
    }

    public void testForwardADTaskRequestWithoutUser() throws IOException {
        ForwardADTaskRequest request = new ForwardADTaskRequest(TestHelpers.randomAnomalyDetector(ImmutableMap.of(), Instant.now()), null);
        testForwardADTaskRequest(request);
    }

    public void testInvalidForwardADTaskRequest() {
        ForwardADTaskRequest request = new ForwardADTaskRequest(null, TestHelpers.randomUser());

        ActionRequestValidationException exception = request.validate();
        assertTrue(exception.getMessage().contains(CommonErrorMessages.DETECTOR_MISSING));
    }

    private void testForwardADTaskRequest(ForwardADTaskRequest request) throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        ForwardADTaskRequest parsedRequest = new ForwardADTaskRequest(input);
        if (request.getUser() != null) {
            assertTrue(request.getUser().equals(parsedRequest.getUser()));
        } else {
            assertNull(parsedRequest.getUser());
        }
    }
}
