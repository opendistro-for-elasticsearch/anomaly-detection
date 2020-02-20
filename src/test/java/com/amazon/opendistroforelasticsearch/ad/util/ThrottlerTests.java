/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.ad.util;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import static org.mockito.Mockito.mock;

public class ThrottlerTests extends ESTestCase {
    private Throttler throttler;

    @Before
    public void setup() {
        Clock clock = mock(Clock.class);
        this.throttler = new Throttler(clock);
    }

    @Test
    public void test() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of(), null);
        SearchRequest dummySearchRequest = new SearchRequest();
        throttler.insertFilteredQuery(detector.getDetectorId(), dummySearchRequest);
        Optional<Map.Entry<ActionRequest, Instant>> entry = throttler.getFilteredQuery(detector.getDetectorId());
        assertTrue(entry.isPresent());
        throttler.clearFilteredQuery(detector.getDetectorId());
        entry = throttler.getFilteredQuery(detector.getDetectorId());
        assertFalse(entry.isPresent());
        return;
    }
}
