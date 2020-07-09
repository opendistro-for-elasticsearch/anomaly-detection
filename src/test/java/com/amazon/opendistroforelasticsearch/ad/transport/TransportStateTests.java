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
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap.SimpleImmutableEntry;

import org.elasticsearch.test.ESTestCase;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;

public class TransportStateTests extends ESTestCase {
    private TransportState state;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        state = new TransportState("123");
    }

    private Duration duration = Duration.ofHours(1);

    public void testMaintenanceNotRemoveSingle() throws IOException {
        state
            .setDetectorDef(
                new SimpleImmutableEntry<>(
                    TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null),
                    Instant.ofEpochMilli(1000)
                )
            );

        assertTrue(!state.expired(duration, Instant.MIN));
    }

    public void testMaintenanceNotRemove() throws IOException {
        state
            .setDetectorDef(
                new SimpleImmutableEntry<>(
                    TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null),
                    Instant.ofEpochSecond(1000)
                )
            );
        state.setLastError(new SimpleImmutableEntry<>(null, Instant.ofEpochMilli(1000)));

        assertTrue(!state.expired(duration, Instant.ofEpochSecond(3700)));
    }

    public void testMaintenanceRemoveLastError() throws IOException {
        state
            .setDetectorDef(
                new SimpleImmutableEntry<>(
                    TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null),
                    Instant.ofEpochMilli(1000)
                )
            );
        state.setLastError(new SimpleImmutableEntry<>(null, Instant.ofEpochMilli(1000)));

        assertTrue(state.expired(duration, Instant.ofEpochSecond(3700)));
    }

    public void testMaintenancRemoveDetector() throws IOException {
        state
            .setDetectorDef(
                new SimpleImmutableEntry<>(TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null), Instant.MIN)
            );
        assertTrue(state.expired(duration, Instant.MAX));

    }

    public void testMaintenanceFlagNotRemove() throws IOException {
        state.setCheckpoint(Instant.ofEpochMilli(1000));
        assertTrue(!state.expired(duration, Instant.MIN));

    }

    public void testMaintenancFlagRemove() throws IOException {
        state.setCheckpoint(Instant.MIN);
        assertTrue(!state.expired(duration, Instant.MIN));

    }
}
