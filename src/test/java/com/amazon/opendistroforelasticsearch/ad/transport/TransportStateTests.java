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
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import org.elasticsearch.test.ESTestCase;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;

public class TransportStateTests extends ESTestCase {
    private TransportState state;
    private Clock clock;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clock = mock(Clock.class);
        state = new TransportState("123", clock);
    }

    private Duration duration = Duration.ofHours(1);

    public void testMaintenanceNotRemoveSingle() throws IOException {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(1000));
        state.setDetectorDef(TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null));

        assertTrue(!state.expired(duration, Instant.MIN));
    }

    public void testMaintenanceNotRemove() throws IOException {
        when(clock.instant()).thenReturn(Instant.ofEpochSecond(1000));
        state.setDetectorDef(TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null));
        state.setLastDetectionError(null);

        assertTrue(!state.expired(duration, Instant.ofEpochSecond(3700)));
    }

    public void testMaintenanceRemoveLastError() throws IOException {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(1000));
        state
            .setDetectorDef(

                TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null)
            );
        state.setLastDetectionError(null);

        assertTrue(state.expired(duration, Instant.ofEpochSecond(3700)));
    }

    public void testMaintenancRemoveDetector() throws IOException {
        when(clock.instant()).thenReturn(Instant.MIN);
        state.setDetectorDef(TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null));
        assertTrue(state.expired(duration, Instant.MAX));

    }

    public void testMaintenanceFlagNotRemove() throws IOException {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(1000));
        state.setCheckpointExists(true);
        assertTrue(!state.expired(duration, Instant.MIN));
    }

    public void testMaintenancFlagRemove() throws IOException {
        when(clock.instant()).thenReturn(Instant.MIN);
        state.setCheckpointExists(true);
        assertTrue(!state.expired(duration, Instant.MIN));
    }

    public void testMaintenanceLastColdStartRemoved() {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(1000));
        state.setLastColdStartException(new RuntimeException(""));
        assertTrue(state.expired(duration, Instant.ofEpochSecond(3700)));
    }

    public void testMaintenanceLastColdStartNotRemoved() {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(1_000_000L));
        state.setLastColdStartException(new RuntimeException(""));
        assertTrue(!state.expired(duration, Instant.ofEpochSecond(3700)));
    }
}
