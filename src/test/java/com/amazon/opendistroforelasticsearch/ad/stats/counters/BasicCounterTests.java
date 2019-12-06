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

package com.amazon.opendistroforelasticsearch.ad.stats.counters;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class BasicCounterTests extends ESTestCase {
    @Test
    public void testIncrement() {
        BasicCounter counter = new BasicCounter();
        long incCount = 4L;
        for (int i = 0; i < incCount; i++) {
            counter.increment();
        }

        assertEquals("Increment does not work", (Long) incCount, counter.getValue());
    }

    @Test
    public void testAddAndReset() {
        BasicCounter counter = new BasicCounter();
        long value = 4L;
        counter.add(value);
        assertEquals("Add does not work", (Long) value, counter.getValue());
        counter.reset();
        assertEquals("Reset does not work", (Long) 0L, counter.getValue());
    }

    @Test
    public void testGetValue() {
        BasicCounter counter = new BasicCounter();
        assertEquals("Increment does not work", (Long) 0L, counter.getValue());
    }
}