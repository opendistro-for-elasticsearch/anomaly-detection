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

import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.InternalFailure;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.FeatureData;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyResultAction;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyResultRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyResultResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.handler.AnomalyResultHandler;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobExecutionContext;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.LockModel;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.Schedule;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.utils.LockService;
import com.google.common.base.Throwables;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin.AD_THREAD_POOL_NAME;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.XCONTENT_WITH_TYPE;
import static org.elasticsearch.action.DocWriteResponse.Result.CREATED;
import static org.elasticsearch.action.DocWriteResponse.Result.UPDATED;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * JobScheduler will call AD job runner to get anomaly result periodically
 */
public class AnomalyDetectorJobRunner implements ScheduledJobRunner {
    private static final Logger log = LogManager.getLogger(AnomalyDetectorJobRunner.class);
    private static final long JOB_RUN_BUFFER_IN_MILLISECONDS = 10 * 1000;
    private static AnomalyDetectorJobRunner INSTANCE;
    private Settings settings;
    private BackoffPolicy backoffPolicy;
    private int maxRetryForEndRunException;
    private Client client;
    private ThreadPool threadPool;
    private AnomalyResultHandler anomalyResultHandler;
    private ConcurrentHashMap<String, Integer> detectorEndRunExceptionCount;

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

    private AnomalyDetectorJobRunner() {
        // Singleton class, use getJobRunnerInstance method instead of constructor
    }

    public void setClient(Client client) {
        this.client = client;
    }

    public void setThreadPool(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    public void setAnomalyResultHandler(AnomalyResultHandler anomalyResultHandler) {
        this.anomalyResultHandler = anomalyResultHandler;
    }

    public void setDetectorEndRunExceptionCount(ConcurrentHashMap<String, Integer> detectorEndRunExceptionCount) {
        this.detectorEndRunExceptionCount = detectorEndRunExceptionCount;
    }

    public void setSettings(Settings settings) {
        this.settings = settings;
        this.backoffPolicy = BackoffPolicy
            .exponentialBackoff(
                AnomalyDetectorSettings.BACKOFF_INITIAL_DELAY.get(settings),
                AnomalyDetectorSettings.MAX_RETRY_FOR_BACKOFF.get(settings)
            );
        this.maxRetryForEndRunException = AnomalyDetectorSettings.MAX_RETRY_FOR_END_RUN_EXCEPTION.get(settings);
    }

    public BackoffPolicy getBackoffPolicy() {
        return this.backoffPolicy;
    }

    @Override
    public void runJob(ScheduledJobParameter jobParameter, JobExecutionContext context) {
        log.info("Start to run AD job {}", jobParameter.getName());
        if (!(jobParameter instanceof AnomalyDetectorJob)) {
            throw new IllegalArgumentException(
                "Job parameter is not instance of AnomalyDetectorJob, type: " + jobParameter.getClass().getCanonicalName()
            );
        }

        Instant executionStartTime = Instant.now();
        IntervalSchedule schedule = (IntervalSchedule) jobParameter.getSchedule();
        Instant startTime = executionStartTime.minus(schedule.getInterval(), schedule.getUnit());

        final LockService lockService = context.getLockService();

        Runnable runnable = () -> {
            if (jobParameter.getLockDurationSeconds() != null) {
                lockService
                    .acquireLock(
                        jobParameter,
                        context,
                        ActionListener
                            .wrap(
                                lock -> runAdJob(
                                    jobParameter,
                                    lockService,
                                    lock,
                                    startTime,
                                    executionStartTime,
                                    backoffPolicy.iterator(),
                                    0
                                ),
                                exception -> {
                                    indexAnomalyResultException(jobParameter, startTime, executionStartTime, exception);
                                    throw new IllegalStateException("Failed to acquire lock for AD job: " + jobParameter.getName());
                                }
                            )
                    );
            } else {
                log.warn("Can't get lock for AD job: " + jobParameter.getName());
            }
        };

        ExecutorService executor = threadPool.executor(AD_THREAD_POOL_NAME);
        executor.submit(runnable);
    }

    protected void runAdJob(
        ScheduledJobParameter jobParameter,
        LockService lockService,
        LockModel lock,
        Instant startTime,
        Instant executionStartTime,
        Iterator<TimeValue> backoff,
        int retryTimes
    ) {

        if (lock == null) {
            indexAnomalyResultException(
                jobParameter,
                startTime,
                executionStartTime,
                new AnomalyDetectionException(jobParameter.getName(), "Can't run AD job due to null lock")
            );
            return;
        }

        try {
            AnomalyResultRequest request = new AnomalyResultRequest(
                jobParameter.getName(),
                startTime.toEpochMilli(),
                executionStartTime.toEpochMilli()
            );
            client.execute(AnomalyResultAction.INSTANCE, request, ActionListener.wrap(response -> {
                indexAnomalyResult(jobParameter, lockService, lock, startTime, executionStartTime, response);
                detectorEndRunExceptionCount.remove(jobParameter.getName());
            },
                exception -> {
                    handleAdException(jobParameter, lockService, lock, startTime, executionStartTime, backoff, retryTimes, exception);
                }
            ));
        } catch (Exception e) {
            indexAnomalyResultException(jobParameter, lockService, lock, startTime, executionStartTime, e, true);
            log.error("Failed to execute AD job " + jobParameter.getName(), e);
        }
    }

    protected void handleAdException(
        ScheduledJobParameter jobParameter,
        LockService lockService,
        LockModel lock,
        Instant startTime,
        Instant executionStartTime,
        Iterator<TimeValue> backoff,
        int retryTimes,
        Exception exception
    ) {
        String detectorId = jobParameter.getName();
        if (exception instanceof EndRunException) {
            log.error("EndRunException happened when executed anomaly result action for " + detectorId, exception);

            if (((EndRunException) exception).isEndNow()) {
                detectorEndRunExceptionCount.remove(detectorId);
                // Stop AD job if EndRunException shows we should end job now.
                stopAdJob(detectorId);
                indexAnomalyResultException(
                    jobParameter,
                    lockService,
                    lock,
                    startTime,
                    executionStartTime,
                    "Stopped detector: " + exception.getMessage(),
                    true
                );
            } else {
                detectorEndRunExceptionCount.compute(detectorId, (k, v) -> {
                    if (v == null) {
                        return 1;
                    } else if (retryTimes == 0) {
                        return v + 1;
                    } else {
                        return v;
                    }
                });
                // if AD job failed consecutively due to EndRunException and failed times exceeds upper limit, will stop AD job
                if (detectorEndRunExceptionCount.get(detectorId) > maxRetryForEndRunException) {
                    stopAdJobForEndRunException(
                        jobParameter,
                        lockService,
                        lock,
                        startTime,
                        executionStartTime,
                        (EndRunException) exception
                    );
                    return;
                }
                // retry AD job, if all retries failed or have no enough time to retry, will record exception in AD result.
                Schedule schedule = jobParameter.getSchedule();
                long nextExecutionTime = jobParameter.getSchedule().getNextExecutionTime(executionStartTime).toEpochMilli();
                long now = Instant.now().toEpochMilli();
                long leftTime = nextExecutionTime - now - JOB_RUN_BUFFER_IN_MILLISECONDS;
                long usedTime = now - executionStartTime.toEpochMilli();

                TimeValue nextRunDelay = backoff.hasNext() ? backoff.next() : null;

                if (nextRunDelay != null && leftTime > (usedTime + nextRunDelay.getMillis())) {
                    threadPool
                        .schedule(
                            () -> runAdJob(jobParameter, lockService, lock, startTime, executionStartTime, backoff, retryTimes + 1),
                            nextRunDelay,
                            ThreadPool.Names.SAME
                        );
                } else {
                    indexAnomalyResultException(
                        jobParameter,
                        lockService,
                        lock,
                        startTime,
                        executionStartTime,
                        exception.getMessage(),
                        true
                    );
                }
            }
        } else {
            if (exception instanceof InternalFailure) {
                // AnomalyResultTransportAction already prints exception stack trace
                log.error("InternalFailure happened when executed anomaly result action for " + jobParameter.getName());
            } else {
                log.error("Failed to execute anomaly result action for " + jobParameter.getName(), exception);
            }
            indexAnomalyResultException(jobParameter, lockService, lock, startTime, executionStartTime, exception, true);
        }
    }

    private void stopAdJobForEndRunException(
        ScheduledJobParameter jobParameter,
        LockService lockService,
        LockModel lock,
        Instant startTime,
        Instant executionStartTime,
        EndRunException exception
    ) {
        detectorEndRunExceptionCount.remove(jobParameter.getName());
        stopAdJob(jobParameter.getName());
        indexAnomalyResultException(
            jobParameter,
            lockService,
            lock,
            startTime,
            executionStartTime,
            "Stopped detector: " + exception.getMessage(),
            true
        );
    }

    private void stopAdJob(String detectorId) {
        try {
            GetRequest getRequest = new GetRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX).id(detectorId);

            client.get(getRequest, ActionListener.wrap(response -> {
                if (response.isExists()) {
                    String s = response.getSourceAsString();
                    try (
                        XContentParser parser = XContentType.JSON
                            .xContent()
                            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.getSourceAsString())
                    ) {
                        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                        AnomalyDetectorJob job = AnomalyDetectorJob.parse(parser);
                        if (job.isEnabled()) {
                            AnomalyDetectorJob newJob = new AnomalyDetectorJob(
                                job.getName(),
                                job.getSchedule(),
                                job.getWindowDelay(),
                                false,
                                job.getEnabledTime(),
                                Instant.now(),
                                Instant.now(),
                                job.getLockDurationSeconds()
                            );
                            IndexRequest indexRequest = new IndexRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX)
                                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                                .source(newJob.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), XCONTENT_WITH_TYPE))
                                .id(detectorId);
                            client.index(indexRequest, ActionListener.wrap(indexResponse -> {
                                if (indexResponse != null
                                    && (indexResponse.getResult() == CREATED || indexResponse.getResult() == UPDATED)) {
                                    log.info("AD Job was disabled by JobRunner for " + detectorId);
                                } else {
                                    log.warn("Failed to disable AD job for " + detectorId);
                                }
                            }, exception -> log.error("JobRunner failed to update AD job as disabled for " + detectorId, exception)));
                        }
                    } catch (IOException e) {
                        log.error("JobRunner failed to stop detector job " + detectorId, e);
                    }
                }
            }, exception -> log.error("JobRunner failed to get detector job " + detectorId, exception)));
        } catch (Exception e) {
            log.error("JobRunner failed to stop AD job " + detectorId, e);
        }
    }

    private void indexAnomalyResult(
        ScheduledJobParameter jobParameter,
        LockService lockService,
        LockModel lock,
        Instant startTime,
        Instant executionStartTime,
        AnomalyResultResponse response
    ) {
        try {
            IntervalTimeConfiguration windowDelay = (IntervalTimeConfiguration) ((AnomalyDetectorJob) jobParameter).getWindowDelay();
            Instant dataStartTime = startTime.minus(windowDelay.getInterval(), windowDelay.getUnit());
            Instant dataEndTime = executionStartTime.minus(windowDelay.getInterval(), windowDelay.getUnit());

            if (response.getError() != null) {
                log.info("Anomaly result action run successfully for {} with error {}", jobParameter.getName(), response.getError());
            }
            AnomalyResult anomalyResult = new AnomalyResult(
                jobParameter.getName(),
                response.getAnomalyScore(),
                response.getAnomalyGrade(),
                response.getConfidence(),
                response.getFeatures(),
                dataStartTime,
                dataEndTime,
                executionStartTime,
                Instant.now(),
                response.getError()
            );
            anomalyResultHandler.indexAnomalyResult(anomalyResult);
        } catch (Exception e) {
            log.error("Failed to index anomaly result for " + jobParameter.getName(), e);
        } finally {
            releaseLock(jobParameter, lockService, lock);
        }
    }

    private void indexAnomalyResultException(
        ScheduledJobParameter jobParameter,
        Instant startTime,
        Instant executionStartTime,
        Exception exception
    ) {
        indexAnomalyResultException(jobParameter, null, null, startTime, executionStartTime, exception, false);
    }

    private void indexAnomalyResultException(
        ScheduledJobParameter jobParameter,
        LockService lockService,
        LockModel lock,
        Instant startTime,
        Instant executionStartTime,
        Exception exception,
        boolean releaseLock
    ) {
        try {
            String errorMessage = exception instanceof AnomalyDetectionException
                ? exception.getMessage()
                : Throwables.getStackTraceAsString(exception);
            indexAnomalyResultException(jobParameter, lockService, lock, startTime, executionStartTime, errorMessage, releaseLock);
        } catch (Exception e) {
            log.error("Failed to index anomaly result for " + jobParameter.getName(), e);
        }
    }

    private void indexAnomalyResultException(
        ScheduledJobParameter jobParameter,
        LockService lockService,
        LockModel lock,
        Instant startTime,
        Instant executionStartTime,
        String errorMessage,
        boolean releaseLock
    ) {
        try {
            IntervalTimeConfiguration windowDelay = (IntervalTimeConfiguration) ((AnomalyDetectorJob) jobParameter).getWindowDelay();
            Instant dataStartTime = startTime.minus(windowDelay.getInterval(), windowDelay.getUnit());
            Instant dataEndTime = executionStartTime.minus(windowDelay.getInterval(), windowDelay.getUnit());

            AnomalyResult anomalyResult = new AnomalyResult(
                jobParameter.getName(),
                Double.NaN,
                Double.NaN,
                Double.NaN,
                new ArrayList<FeatureData>(),
                dataStartTime,
                dataEndTime,
                executionStartTime,
                Instant.now(),
                errorMessage
            );
            anomalyResultHandler.indexAnomalyResult(anomalyResult);
        } catch (Exception e) {
            log.error("Failed to index anomaly result for " + jobParameter.getName(), e);
        } finally {
            if (releaseLock) {
                releaseLock(jobParameter, lockService, lock);
            }
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