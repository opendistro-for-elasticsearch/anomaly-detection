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

import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.transport.handler.AnomalyResultHandler;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobExecutionContext;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.LockModel;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.Schedule;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.utils.LockService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AnomalyDetectorJobRunnerTests extends AbstractADTest {

    @Mock
    private Client client;
    @Mock
    private ClusterService clusterService;

    private LockService lockService;

    @Mock
    private AnomalyDetectorJob jobParameter;

    @Mock
    private JobExecutionContext context;

    private AnomalyDetectorJobRunner runner = AnomalyDetectorJobRunner.getJobRunnerInstance();

    @Mock
    private ThreadPool mockedThreadPool;

    private ExecutorService executorService;

    @Mock
    private Iterator<TimeValue> backoff;

    @Mock
    private AnomalyResultHandler anomalyResultHandler;

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(AnomalyDetectorJobRunnerTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
    }

    @Before
    public void setup() throws Exception {
        super.setUp();
        super.setUpLog4jForJUnit(AnomalyDetectorJobRunner.class);
        MockitoAnnotations.initMocks(this);
        ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(EsExecutors.threadName("node1", "test-ad"));
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        executorService = EsExecutors.newFixed("test-ad", 4, 100, threadFactory, threadContext);
        doReturn(executorService).when(mockedThreadPool).executor(anyString());
        runner.setThreadPool(mockedThreadPool);
        runner.setClient(client);
        runner.setAnomalyResultHandler(anomalyResultHandler);

        setUpJobParameter();

        runner
            .setSettings(
                Settings
                    .builder()
                    .put("opendistro.anomaly_detection.max_retry_for_backoff", 2)
                    .put("opendistro.anomaly_detection.backoff_initial_delay", TimeValue.timeValueMillis(1))
                    .put("opendistro.anomaly_detection.max_retry_for_end_run_exception", 3)
                    .build()
            );

        lockService = new LockService(client, clusterService);
        doReturn(lockService).when(context).getLockService();
    }

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        super.tearDownLog4jForJUnit();
        executorService.shutdown();
    }

    @Test
    public void testRunJobWithWrongParameterType() {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Job parameter is not instance of AnomalyDetectorJob, type: ");

        ScheduledJobParameter parameter = mock(ScheduledJobParameter.class);
        when(jobParameter.getLockDurationSeconds()).thenReturn(null);
        runner.runJob(parameter, context);
    }

    @Test
    public void testRunJobWithNullLockDuration() throws InterruptedException {
        when(jobParameter.getLockDurationSeconds()).thenReturn(null);
        when(jobParameter.getSchedule()).thenReturn(new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES));
        runner.runJob(jobParameter, context);
        Thread.sleep(1000);
        assertTrue(testAppender.containsMessage("Can't get lock for AD job"));
    }

    @Test
    public void testRunJobWithLockDuration() throws InterruptedException {
        when(jobParameter.getLockDurationSeconds()).thenReturn(100L);
        when(jobParameter.getSchedule()).thenReturn(new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES));
        runner.runJob(jobParameter, context);
        Thread.sleep(1000);
        assertFalse(testAppender.containsMessage("Can't get lock for AD job"));
        verify(context, times(1)).getLockService();
    }

    @Test
    public void testRunAdJobWithNullLock() {
        LockModel lock = null;
        runner.runAdJob(jobParameter, lockService, lock, Instant.now().minusMillis(1000 * 60), Instant.now());
        verify(client, never()).execute(any(), any(), any());
    }

    @Test
    public void testRunAdJobWithLock() {
        LockModel lock = new LockModel("indexName", "jobId", Instant.now(), 10, false);

        runner.runAdJob(jobParameter, lockService, lock, Instant.now().minusMillis(1000 * 60), Instant.now());
        verify(client, times(1)).execute(any(), any(), any());
    }

    @Test
    public void testRunAdJobWithExecuteException() {
        LockModel lock = new LockModel("indexName", "jobId", Instant.now(), 10, false);

        doThrow(RuntimeException.class).when(client).execute(any(), any(), any());

        runner.runAdJob(jobParameter, lockService, lock, Instant.now().minusMillis(1000 * 60), Instant.now());
        verify(client, times(1)).execute(any(), any(), any());
        assertTrue(testAppender.containsMessage("Failed to execute AD job"));
    }

    @Test
    public void testRunAdJobWithEndRunExceptionNow() {
        LockModel lock = new LockModel("indexName", "jobId", Instant.now(), 10, false);
        Exception exception = new EndRunException(jobParameter.getName(), randomAlphaOfLength(5), true);
        runner.handleAdException(jobParameter, lockService, lock, Instant.now().minusMillis(1000 * 60), Instant.now(), exception);
        verify(anomalyResultHandler).indexAnomalyResult(any());
    }

    @Test
    public void testRunAdJobWithEndRunExceptionNowAndExistingAdJob() {
        testRunAdJobWithEndRunExceptionNowAndStopAdJob(true, true, true);
        verify(anomalyResultHandler).indexAnomalyResult(any());
        verify(client).index(any(), any());
        assertTrue(testAppender.containsMessage("AD Job was disabled by JobRunner for"));
    }

    @Test
    public void testRunAdJobWithEndRunExceptionNowAndExistingAdJobAndIndexException() {
        testRunAdJobWithEndRunExceptionNowAndStopAdJob(true, true, false);
        verify(anomalyResultHandler).indexAnomalyResult(any());
        verify(client).index(any(), any());
        assertTrue(testAppender.containsMessage("Failed to disable AD job for"));
    }

    @Test
    public void testRunAdJobWithEndRunExceptionNowAndNotExistingEnabledAdJob() {
        testRunAdJobWithEndRunExceptionNowAndStopAdJob(false, true, true);
        verify(anomalyResultHandler).indexAnomalyResult(any());
        verify(client, never()).index(any(), any());
        assertFalse(testAppender.containsMessage("AD Job was disabled by JobRunner for"));
        assertFalse(testAppender.containsMessage("Failed to disable AD job for"));
    }

    @Test
    public void testRunAdJobWithEndRunExceptionNowAndExistingDisabledAdJob() {
        testRunAdJobWithEndRunExceptionNowAndStopAdJob(true, false, true);
        verify(anomalyResultHandler).indexAnomalyResult(any());
        verify(client, never()).index(any(), any());
        assertFalse(testAppender.containsMessage("AD Job was disabled by JobRunner for"));
    }

    @Test
    public void testRunAdJobWithEndRunExceptionNowAndNotExistingDisabledAdJob() {
        testRunAdJobWithEndRunExceptionNowAndStopAdJob(false, false, true);
        verify(anomalyResultHandler).indexAnomalyResult(any());
        verify(client, never()).index(any(), any());
        assertFalse(testAppender.containsMessage("AD Job was disabled by JobRunner for"));
    }

    private void testRunAdJobWithEndRunExceptionNowAndStopAdJob(boolean jobExists, boolean jobEnabled, boolean disableSuccessfully) {
        LockModel lock = new LockModel(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX, jobParameter.getName(), Instant.now(), 10, false);
        Exception exception = new EndRunException(jobParameter.getName(), randomAlphaOfLength(5), true);

        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            GetResponse response = new GetResponse(
                new GetResult(
                    AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX,
                    MapperService.SINGLE_MAPPING_NAME,
                    jobParameter.getName(),
                    UNASSIGNED_SEQ_NO,
                    0,
                    -1,
                    jobExists,
                    BytesReference
                        .bytes(
                            new AnomalyDetectorJob(
                                jobParameter.getName(),
                                jobParameter.getSchedule(),
                                jobParameter.getWindowDelay(),
                                jobEnabled,
                                Instant.now().minusSeconds(60),
                                Instant.now(),
                                Instant.now(),
                                60L
                            ).toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS)
                        ),
                    Collections.emptyMap(),
                    Collections.emptyMap()
                )
            );

            listener.onResponse(response);
            return null;
        }).when(client).get(any(), any());

        doAnswer(invocation -> {
            IndexRequest request = invocation.getArgument(0);
            ActionListener<IndexResponse> listener = invocation.getArgument(1);
            ShardId shardId = new ShardId(new Index(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX, randomAlphaOfLength(10)), 0);
            if (disableSuccessfully) {
                listener.onResponse(new IndexResponse(shardId, randomAlphaOfLength(10), request.id(), 1, 1, 1, true));
            } else {
                listener.onResponse(null);
            }
            return null;
        }).when(client).index(any(), any());

        runner.handleAdException(jobParameter, lockService, lock, Instant.now().minusMillis(1000 * 60), Instant.now(), exception);
    }

    @Test
    public void testRunAdJobWithEndRunExceptionNowAndGetJobException() {
        LockModel lock = new LockModel("indexName", "jobId", Instant.now(), 10, false);
        Exception exception = new EndRunException(jobParameter.getName(), randomAlphaOfLength(5), true);

        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException("test"));
            return null;
        }).when(client).get(any(), any());

        runner.handleAdException(jobParameter, lockService, lock, Instant.now().minusMillis(1000 * 60), Instant.now(), exception);
        verify(anomalyResultHandler).indexAnomalyResult(any());
        assertEquals(1, testAppender.countMessage("JobRunner failed to get detector job"));
    }

    @Test
    public void testRunAdJobWithEndRunExceptionNowAndFailToGetJob() {
        LockModel lock = new LockModel("indexName", "jobId", Instant.now(), 10, false);
        Exception exception = new EndRunException(jobParameter.getName(), randomAlphaOfLength(5), true);

        doThrow(new RuntimeException("fail to get AD job")).when(client).get(any(), any());

        runner.handleAdException(jobParameter, lockService, lock, Instant.now().minusMillis(1000 * 60), Instant.now(), exception);
        verify(anomalyResultHandler).indexAnomalyResult(any());
        assertEquals(1, testAppender.countMessage("JobRunner failed to stop AD job"));
    }

    @Test
    public void testRunAdJobWithEndRunExceptionNotNowAndRetryUntilStop() throws InterruptedException {
        LockModel lock = new LockModel(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX, jobParameter.getName(), Instant.now(), 10, false);
        Instant executionStartTime = Instant.now();
        Schedule schedule = mock(IntervalSchedule.class);
        when(jobParameter.getSchedule()).thenReturn(schedule);
        when(schedule.getNextExecutionTime(executionStartTime)).thenReturn(executionStartTime.plusSeconds(5));

        doAnswer(invocation -> {
            Exception exception = new EndRunException(jobParameter.getName(), randomAlphaOfLength(5), false);
            ActionListener<?> listener = invocation.getArgument(2);
            listener.onFailure(exception);
            return null;
        }).when(client).execute(any(), any(), any());

        for (int i = 0; i < 3; i++) {
            runner.runAdJob(jobParameter, lockService, lock, Instant.now().minusSeconds(60), executionStartTime);
            assertEquals(i + 1, testAppender.countMessage("EndRunException happened for"));
        }
        runner.runAdJob(jobParameter, lockService, lock, Instant.now().minusSeconds(60), executionStartTime);
        assertEquals(1, testAppender.countMessage("JobRunner will stop AD job due to EndRunException retry exceeds upper limit"));
    }

    private void setUpJobParameter() {
        when(jobParameter.getName()).thenReturn(randomAlphaOfLength(10));
        IntervalSchedule schedule = new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES);
        when(jobParameter.getSchedule()).thenReturn(schedule);
        when(jobParameter.getWindowDelay()).thenReturn(new IntervalTimeConfiguration(10, ChronoUnit.SECONDS));
    }

}
