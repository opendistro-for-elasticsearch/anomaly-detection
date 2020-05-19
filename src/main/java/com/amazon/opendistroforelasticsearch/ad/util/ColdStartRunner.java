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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * The runner allows us to have a parallel thread start cold start in the
 * coordinating AD node. We can check the execution results and exceptions if
 * any when cold start finishes.
 *
 */
public class ColdStartRunner {
    private static final Logger LOG = LogManager.getLogger(ColdStartRunner.class);

    private ExecutorService exec;
    private ExecutorCompletionService<Boolean> runner;

    private Map<String, AnomalyDetectionException> currentExceptions;

    public ColdStartRunner() {
        // when the thread is daemon thread, it will end immediately when the application exits.
        exec = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("ad-thread-%d").setDaemon(true).build());
        this.runner = new ExecutorCompletionService<Boolean>(exec);
        this.currentExceptions = new ConcurrentHashMap<>();
    }

    public Future<Boolean> compute(Callable<Boolean> task) {
        return runner.submit(task);
    }

    public void shutDown() {
        exec.shutdown();
    }

    Optional<Boolean> checkResult() {
        try {
            Future<Boolean> result = runner.poll();
            if (result != null) {
                return Optional.of(result.get());
            }
        } catch (Throwable e) {
            LOG.error("Could not get result", e);
            Throwable cause = e.getCause();
            if (cause instanceof AnomalyDetectionException) {
                AnomalyDetectionException adException = (AnomalyDetectionException) cause;
                currentExceptions.put(adException.getAnomalyDetectorId(), adException);
                LOG.info("added cause for {}", adException.getAnomalyDetectorId());
            } else {
                LOG.error("Get an unexpected exception");
            }
        }
        return Optional.empty();
    }

    public Optional<AnomalyDetectionException> fetchException(String adID) {
        checkResult();

        AnomalyDetectionException ex = currentExceptions.get(adID);
        if (ex != null) {
            LOG.error("Found a matching exception for " + adID, ex);
            return Optional.of(currentExceptions.remove(adID));
        } else {
            LOG.info("Cannot find a matching exception for {}", adID);
        }
        return Optional.empty();
    }
}
