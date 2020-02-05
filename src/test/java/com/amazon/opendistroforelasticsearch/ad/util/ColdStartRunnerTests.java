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

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

public class ColdStartRunnerTests extends ESTestCase {
    private static final Logger LOG = LogManager.getLogger(ColdStartRunnerTests.class);
    private ColdStartRunner runner;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        runner = new ColdStartRunner();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        runner.shutDown();
        runner = null;
        super.tearDown();
    }

    public void testNullPointerException() throws InterruptedException {
        Future<Boolean> future = runner.compute(() -> {
            LOG.info("Execute..");
            throw new NullPointerException();
        });

        ExecutionException executionException = expectThrows(ExecutionException.class, () -> future.get());
        assertThat(executionException.getCause(), instanceOf(NullPointerException.class));

        Optional<Boolean> res = runner.checkResult();
        assertThat(res.isPresent(), is(false));
    }

    public void testADException() throws InterruptedException {

        String adID = "123";
        Future<Boolean> future = runner.compute(() -> {
            LOG.info("Execute..");
            throw new AnomalyDetectionException(adID, "blah");
        });

        ExecutionException executionException = expectThrows(ExecutionException.class, () -> future.get());
        assertThat(executionException.getCause(), instanceOf(AnomalyDetectionException.class));

        int retries = 10;
        Optional<AnomalyDetectionException> res = null;
        for (int i = 0; i < retries; i++) {
            res = runner.fetchException(adID);
            if (!res.isPresent()) {
                // wait for ExecutorCompletionService to get the completed task
                Thread.sleep(1000);
            } else {
                break;
            }
        }

        assertEquals(adID, res.get().getAnomalyDetectorId());
    }
}
