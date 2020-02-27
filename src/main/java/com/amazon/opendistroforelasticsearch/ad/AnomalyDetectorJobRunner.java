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

package com.amazon.opendistroforelasticsearch.ad;

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyResultAction;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyResultRequest;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobExecutionContext;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.LockModel;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.utils.LockService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.threadpool.ThreadPool;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;

import static com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin.AD_JOB_THREAD_POOL_NAME;

/**
 * JobScheduler will call AD job runner to get anomaly result periodically
 */
public class AnomalyDetectorJobRunner implements ScheduledJobRunner {
    private static final Logger log = LogManager.getLogger(AnomalyDetectorJobRunner.class);

    private static AnomalyDetectorJobRunner INSTANCE;

    public static AnomalyDetectorJobRunner getJobRunnerInstance() {
        if (INSTANCE != null) {
            return INSTANCE;
        }
        synchronized (AnomalyDetectorJobRunner.class) {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            INSTANCE = new AnomalyDetectorJobRunner();
            return INSTANCE;
        }
    }

    private Client client;
    private ThreadPool threadPool;

    private AnomalyDetectorJobRunner() {
        // Singleton class, use getJobRunnerInstance method instead of constructor
    }

    public void setClient(Client client) {
        this.client = client;
    }

    public void setThreadPool(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    @Override
    public void runJob(ScheduledJobParameter jobParameter, JobExecutionContext context) {
        log.info("Start to run AD job {}", jobParameter.getName());
        if (!(jobParameter instanceof AnomalyDetectorJob)) {
            throw new IllegalArgumentException(
                "Job parameter is not instance of AnomalyDetectorJob, type: " + jobParameter.getClass().getCanonicalName()
            );
        }

        final LockService lockService = context.getLockService();

        Runnable runnable = () -> {
            if (jobParameter.getLockDurationSeconds() != null) {
                lockService
                    .acquireLock(
                        jobParameter,
                        context,
                        ActionListener
                            .wrap(
                                lock -> runAdJob(jobParameter, lockService, lock),
                                exception -> {
                                    throw new IllegalStateException("Failed to acquire lock for AD job: " + jobParameter.getName());
                                }
                            )
                    );
            } else {
                log.warn("Can't get lock for AD job: " + jobParameter.getName());
            }
        };

        ExecutorService executor = threadPool.executor(AD_JOB_THREAD_POOL_NAME);
        executor.submit(runnable);
    }

    protected void runAdJob(ScheduledJobParameter jobParameter, LockService lockService, LockModel lock) {
        if (lock == null) {
            return;
        }

        try {
            IntervalSchedule schedule = (IntervalSchedule) jobParameter.getSchedule();
            // TODO: handle jitter of job scheduler
            Instant endTime = Instant.now();
            Duration duration = Duration.of(schedule.getInterval(), schedule.getUnit());
            Instant startTime = endTime.minusMillis(duration.toMillis());

            AnomalyResultRequest request = new AnomalyResultRequest(
                jobParameter.getName(),
                startTime.toEpochMilli(),
                endTime.toEpochMilli()
            );
            client.execute(AnomalyResultAction.INSTANCE, request, ActionListener.wrap(response -> {
                log.info("Anomaly result action ran successfully for " + jobParameter.getName());
                releaseLock(jobParameter, lockService, lock);
            }, exception -> {
                log.error("Failed to execute anomaly result action", exception);
                releaseLock(jobParameter, lockService, lock);
            }));
        } catch (Exception e) {
            log.error("Failed to execute AD job", e);
            releaseLock(jobParameter, lockService, lock);
        }
    }

    private void releaseLock(ScheduledJobParameter jobParameter, LockService lockService, LockModel lock) {
        lockService
            .release(
                lock,
                ActionListener
                    .wrap(
                        released -> { log.info("Released lock for AD job {}", jobParameter.getName()); },
                        exception -> { throw new IllegalStateException("Failed to release lock for AD job: " + jobParameter.getName()); }
                    )
            );
    }
}
