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
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobExecutionContext;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.LockModel;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.utils.LockService;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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
    private ThreadPool threadPool;

    private ExecutorService executorService;

    @Before
    public void setup() throws Exception {
        super.setUp();
        super.setUpLog4jForJUnit(AnomalyDetectorJobRunner.class);
        MockitoAnnotations.initMocks(this);
        ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(EsExecutors.threadName("node1", "test-ad"));
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        executorService = EsExecutors.newFixed("test-ad", 4, 100, threadFactory, threadContext);
        doReturn(executorService).when(threadPool).executor(anyString());
        runner.setThreadPool(threadPool);
        runner.setClient(client);

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
        runner.runJob(jobParameter, context);
        Thread.sleep(1000);
        assertTrue(testAppender.containsMessage("Can't get lock for AD job"));
    }

    @Test
    public void testRunJobWithLocakDuration() throws InterruptedException {
        when(jobParameter.getLockDurationSeconds()).thenReturn(100L);
        runner.runJob(jobParameter, context);
        Thread.sleep(1000);
        assertFalse(testAppender.containsMessage("Can't get lock for AD job"));
        verify(context, times(1)).getLockService();
    }

    @Test
    public void testRunAdJobWithNullLock() {
        LockModel lock = null;
        runner.runAdJob(jobParameter, lockService, lock);
        verify(client, never()).execute(any(), any(), any());
    }

    @Test
    public void testRunAdJobWithLock() {
        LockModel lock = new LockModel("indexName", "jobId", Instant.now(), 10, false);
        IntervalSchedule schedule = mock(IntervalSchedule.class);
        when(schedule.getInterval()).thenReturn(1);
        when(schedule.getUnit()).thenReturn(ChronoUnit.MINUTES);
        when(jobParameter.getSchedule()).thenReturn(schedule);

        runner.runAdJob(jobParameter, lockService, lock);
        verify(client, times(1)).execute(any(), any(), any());
    }

    @Test
    public void testRunAdJobWithExecuteException() {
        LockModel lock = new LockModel("indexName", "jobId", Instant.now(), 10, false);
        IntervalSchedule schedule = mock(IntervalSchedule.class);
        when(schedule.getInterval()).thenReturn(1);
        when(schedule.getUnit()).thenReturn(ChronoUnit.MINUTES);
        when(jobParameter.getSchedule()).thenReturn(schedule);
        doThrow(RuntimeException.class).when(client).execute(any(), any(), any());

        runner.runAdJob(jobParameter, lockService, lock);
        verify(client, times(1)).execute(any(), any(), any());
        assertTrue(testAppender.containsMessage("Failed to execute AD job"));
    }

}
