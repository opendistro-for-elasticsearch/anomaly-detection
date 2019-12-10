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
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ADCircuitBreakerServiceTests {

    @InjectMocks
    private ADCircuitBreakerService adCircuitBreakerService;

    @Mock
    JvmService jvmService;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testRegisterBreaker() {
        adCircuitBreakerService.registerBreaker(BreakerName.MEM.getName(), new MemoryCircuitBreaker(jvmService));
        CircuitBreaker breaker = adCircuitBreakerService.getBreaker(BreakerName.MEM.getName());

        assertThat(breaker, is(notNullValue()));
    }

    @Test
    public void testRegisterBreakerNull() {
        CircuitBreaker breaker = adCircuitBreakerService.getBreaker(BreakerName.MEM.getName());

        assertThat(breaker, is(nullValue()));
    }

    @Test
    public void testUnregisterBreaker() {
        adCircuitBreakerService.registerBreaker(BreakerName.MEM.getName(), new MemoryCircuitBreaker(jvmService));
        CircuitBreaker breaker = adCircuitBreakerService.getBreaker(BreakerName.MEM.getName());
        assertThat(breaker, is(notNullValue()));
        adCircuitBreakerService.unregisterBreaker(BreakerName.MEM.getName());
        breaker = adCircuitBreakerService.getBreaker(BreakerName.MEM.getName());
        assertThat(breaker, is(nullValue()));
    }

    @Test
    public void testUnregisterBreakerNull() {
        adCircuitBreakerService.registerBreaker(BreakerName.MEM.getName(), new MemoryCircuitBreaker(jvmService));
        adCircuitBreakerService.unregisterBreaker(null);
        CircuitBreaker breaker = adCircuitBreakerService.getBreaker(BreakerName.MEM.getName());
        assertThat(breaker, is(notNullValue()));
    }

    @Test
    public void testClearBreakers() {
        adCircuitBreakerService.registerBreaker(BreakerName.CPU.getName(), new MemoryCircuitBreaker(jvmService));
        CircuitBreaker breaker = adCircuitBreakerService.getBreaker(BreakerName.CPU.getName());
        assertThat(breaker, is(notNullValue()));
        adCircuitBreakerService.clearBreakers();
        breaker = adCircuitBreakerService.getBreaker(BreakerName.CPU.getName());
        assertThat(breaker, is(nullValue()));
    }

    @Test
    public void testInit() {
        assertThat(adCircuitBreakerService.init(), is(notNullValue()));
    }

}
