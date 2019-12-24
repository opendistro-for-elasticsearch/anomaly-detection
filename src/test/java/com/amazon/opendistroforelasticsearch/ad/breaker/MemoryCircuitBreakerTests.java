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

package com.amazon.opendistroforelasticsearch.ad.breaker;

import org.elasticsearch.monitor.jvm.JvmService;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

public class MemoryCircuitBreakerTests {

    @Mock
    JvmService jvmService;

    @Mock
    JvmStats jvmStats;

    @Mock
    JvmStats.Mem mem;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(jvmService.stats()).thenReturn(jvmStats);
        when(jvmStats.getMem()).thenReturn(mem);
        when(mem.getHeapUsedPercent()).thenReturn((short) 50);
    }

    @Test
    public void testIsOpen() {
        CircuitBreaker breaker = new MemoryCircuitBreaker(jvmService);

        assertThat(breaker.isOpen(), equalTo(false));
    }

    @Test
    public void testIsOpen1() {
        CircuitBreaker breaker = new MemoryCircuitBreaker((short) 90, jvmService);

        assertThat(breaker.isOpen(), equalTo(false));
    }

    @Test
    public void testIsOpen2() {
        CircuitBreaker breaker = new MemoryCircuitBreaker(jvmService);

        when(mem.getHeapUsedPercent()).thenReturn((short) 95);
        assertThat(breaker.isOpen(), equalTo(true));
    }

    @Test
    public void testIsOpen3() {
        CircuitBreaker breaker = new MemoryCircuitBreaker((short) 90, jvmService);

        when(mem.getHeapUsedPercent()).thenReturn((short) 95);
        assertThat(breaker.isOpen(), equalTo(true));
    }
}
