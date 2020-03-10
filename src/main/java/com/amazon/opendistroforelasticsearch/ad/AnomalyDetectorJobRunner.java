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

    private static AnomalyDetectorJobRunner INSTANCE;
    private Settings settings;
    private BackoffPolicy backoffPolicy;

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
    private AnomalyResultHandler anomalyResultHandler;
    private static final long JOB_RUN_BUFFER_IN_MILLISECONDS = 10 * 1000;

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

    public void setSettings(Settings settings) {
        this.settings = settings;
        this.backoffPolicy = BackoffPolicy
            .exponentialBackoff(
                AnomalyDetectorSettings.BACKOFF_INITIAL_DELAY.get(settings),
                AnomalyDetectorSettings.MAX_RETRY_FOR_BACKOFF.get(settings)
            );
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
        Instant endTime = Instant.now();
        Instant startTime = endTime.minus(schedule.getInterval(), schedule.getUnit());

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
                                    endTime,
                                    executionStartTime,
                                    backoffPolicy.iterator()
                                ),
                                exception -> {
                                    indexAnomalyResultExceptionSilently(jobParameter, startTime, endTime, executionStartTime, exception);
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
        Instant endTime,
        Instant executionStartTime,
        Iterator<TimeValue> backoff
    ) {

        if (lock == null) {
            indexAnomalyResultExceptionSilently(
                jobParameter,
                startTime,
                endTime,
                executionStartTime,
                new AnomalyDetectionException(jobParameter.getName(), "Can't run AD job due to null lock")
            );
            return;
        }

        try {
            AnomalyResultRequest request = new AnomalyResultRequest(
                jobParameter.getName(),
                startTime.toEpochMilli(),
                endTime.toEpochMilli()
            );
            client
                .execute(
                    AnomalyResultAction.INSTANCE,
                    request,
                    ActionListener
                        .wrap(
                            response -> {
                                indexAnomalyResultSilently(
                                    jobParameter,
                                    lockService,
                                    lock,
                                    startTime,
                                    endTime,
                                    executionStartTime,
                                    response
                                );
                            },
                            exception -> {
                                handleAdException(
                                    jobParameter,
                                    lockService,
                                    lock,
                                    startTime,
                                    endTime,
                                    executionStartTime,
                                    backoff,
                                    exception
                                );
                            }
                        )
                );
        } catch (Exception e) {
            indexAnomalyResultExceptionSilently(jobParameter, startTime, endTime, executionStartTime, e);
            log.error("Failed to execute AD job " + jobParameter.getName(), e);
            releaseLock(jobParameter, lockService, lock);
        }
    }

    protected void handleAdException(
        ScheduledJobParameter jobParameter,
        LockService lockService,
        LockModel lock,
        Instant startTime,
        Instant endTime,
        Instant executionStartTime,
        Iterator<TimeValue> backoff,
        Exception exception
    ) {
        if (exception instanceof EndRunException) {
            log.error("EndRunException happened when executed anomaly result action for " + jobParameter.getName(), exception);

            if (((EndRunException) exception).isEndNow()) {
                // Stop AD job if EndRunException shows we should end job now.
                stopAdJobSilently(jobParameter.getName());
                indexAnomalyResultExceptionSilently(
                    jobParameter,
                    startTime,
                    endTime,
                    executionStartTime,
                    "Stopped detector: " + exception.getMessage()
                );
                releaseLock(jobParameter, lockService, lock);
            } else {
                // retry AD job, if all retries failed or have no enough time to retry, will stop AD job.
                Schedule schedule = jobParameter.getSchedule();
                long nextExecutionTime = jobParameter.getSchedule().getNextExecutionTime(executionStartTime).toEpochMilli();
                long now = Instant.now().toEpochMilli();
                long leftTime = nextExecutionTime - now - JOB_RUN_BUFFER_IN_MILLISECONDS;
                long usedTime = now - executionStartTime.toEpochMilli();

                if (!backoff.hasNext()) {
                    stopAdJobForEndRunException(
                        jobParameter,
                        lockService,
                        lock,
                        startTime,
                        endTime,
                        executionStartTime,
                        (EndRunException) exception
                    );
                } else {
                    if (backoff.hasNext()) {
                        TimeValue nextRunDelay = backoff.next();
                        if (leftTime > (usedTime + nextRunDelay.getMillis())) {
                            threadPool
                                .schedule(
                                    () -> runAdJob(jobParameter, lockService, lock, startTime, endTime, executionStartTime, backoff),
                                    nextRunDelay,
                                    ThreadPool.Names.SAME
                                );
                        } else {
                            stopAdJobForEndRunException(
                                jobParameter,
                                lockService,
                                lock,
                                startTime,
                                endTime,
                                executionStartTime,
                                (EndRunException) exception
                            );
                        }
                    }
                }
            }
        } else {
            if (exception instanceof InternalFailure) {
                // AnomalyResultTransportAction already prints exception stack trace
                log.error("InternalFailure happened when executed anomaly result action for " + jobParameter.getName());
            } else {
                log.error("Failed to execute anomaly result action for " + jobParameter.getName(), exception);
            }
            indexAnomalyResultExceptionSilently(jobParameter, startTime, endTime, executionStartTime, exception);
            releaseLock(jobParameter, lockService, lock);
        }
    }

    private void stopAdJobForEndRunException(
        ScheduledJobParameter jobParameter,
        LockService lockService,
        LockModel lock,
        Instant startTime,
        Instant endTime,
        Instant executionStartTime,
        EndRunException exception
    ) {
        stopAdJobSilently(jobParameter.getName());
        indexAnomalyResultExceptionSilently(
            jobParameter,
            startTime,
            endTime,
            executionStartTime,
            "Stopped detector: " + exception.getMessage()
        );
        releaseLock(jobParameter, lockService, lock);
    }

    private void stopAdJobSilently(String detectorId) {
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

    private void indexAnomalyResultSilently(
        ScheduledJobParameter jobParameter,
        LockService lockService,
        LockModel lock,
        Instant startTime,
        Instant endTime,
        Instant executionStartTime,
        AnomalyResultResponse response
    ) {
        try {
            IntervalTimeConfiguration windowDelay = (IntervalTimeConfiguration) ((AnomalyDetectorJob) jobParameter).getWindowDelay();
            Instant dataStartTime = startTime.minus(windowDelay.getInterval(), windowDelay.getUnit());
            Instant dataEndTime = endTime.minus(windowDelay.getInterval(), windowDelay.getUnit());

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

    private void indexAnomalyResultExceptionSilently(
        ScheduledJobParameter jobParameter,
        Instant startTime,
        Instant endTime,
        Instant executionStartTime,
        Exception exception
    ) {
        try {
            String errorMessage = exception instanceof AnomalyDetectionException
                ? exception.getMessage()
                : Throwables.getStackTraceAsString(exception);
            indexAnomalyResultExceptionSilently(jobParameter, startTime, endTime, executionStartTime, errorMessage);
        } catch (Exception e) {
            log.error("Failed to index anomaly result for " + jobParameter.getName(), e);
        }
    }

    private void indexAnomalyResultExceptionSilently(
        ScheduledJobParameter jobParameter,
        Instant startTime,
        Instant endTime,
        Instant executionStartTime,
        String errorMessage
    ) {
        try {
            IntervalTimeConfiguration windowDelay = (IntervalTimeConfiguration) ((AnomalyDetectorJob) jobParameter).getWindowDelay();
            Instant dataStartTime = startTime.minus(windowDelay.getInterval(), windowDelay.getUnit());
            Instant dataEndTime = endTime.minus(windowDelay.getInterval(), windowDelay.getUnit());

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
