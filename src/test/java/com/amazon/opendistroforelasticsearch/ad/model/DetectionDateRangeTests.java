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

package com.amazon.opendistroforelasticsearch.ad.model;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;

public class DetectionDateRangeTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, AnomalyDetectorPlugin.class);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    public void testParseDetectionDateRangeWithNullStartTime() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new DetectionDateRange(null, Instant.now())
        );
        assertEquals("Detection data range's start time must not be null", exception.getMessage());
    }

    public void testParseDetectionDateRangeWithNullEndTime() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new DetectionDateRange(Instant.now(), null)
        );
        assertEquals("Detection data range's end time must not be null", exception.getMessage());
    }

    public void testSerializeDetectoinDateRange() throws IOException {
        DetectionDateRange dateRange = TestHelpers.randomDetectionDateRange();
        BytesStreamOutput output = new BytesStreamOutput();
        dateRange.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        DetectionDateRange parsedDateRange = new DetectionDateRange(input);
        assertTrue(parsedDateRange.equals(dateRange));
    }
}
