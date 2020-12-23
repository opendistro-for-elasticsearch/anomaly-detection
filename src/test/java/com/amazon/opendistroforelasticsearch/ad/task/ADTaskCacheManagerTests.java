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

package com.amazon.opendistroforelasticsearch.ad.task;

import static com.amazon.opendistroforelasticsearch.ad.MemoryTracker.Origin.HISTORICAL_SINGLE_ENTITY_DETECTOR;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import com.amazon.opendistroforelasticsearch.ad.MemoryTracker;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskState;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;

public class ADTaskCacheManagerTests extends ESTestCase {
    private MemoryTracker memoryTracker;
    private ADTaskCacheManager adTaskCacheManager;
    private ClusterService clusterService;
    private Settings settings;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        settings = Settings.builder().put(AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE.getKey(), 2).build();

        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        memoryTracker = mock(MemoryTracker.class);
        adTaskCacheManager = new ADTaskCacheManager(settings, clusterService, memoryTracker);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        adTaskCacheManager.clear();
    }

    public void testPutTask() throws IOException {
        when(memoryTracker.canAllocateReserved(anyString(), anyLong())).thenReturn(true);
        ADTask adTask = TestHelpers.randomAdTask();
        adTaskCacheManager.put(adTask);
        assertEquals(1, adTaskCacheManager.size());
        assertTrue(adTaskCacheManager.contains(adTask.getTaskId()));
        assertTrue(adTaskCacheManager.containsTaskOfDetector(adTask.getDetectorId()));
        assertNotNull(adTaskCacheManager.getRcfModel(adTask.getTaskId()));
        assertNotNull(adTaskCacheManager.getShingle(adTask.getTaskId()));
        assertNotNull(adTaskCacheManager.getThresholdModel(adTask.getTaskId()));
        assertNotNull(adTaskCacheManager.getThresholdModelTrainingData(adTask.getTaskId()));
        assertFalse(adTaskCacheManager.isThresholdModelTrained(adTask.getTaskId()));
        adTaskCacheManager.remove(adTask.getTaskId());
        assertEquals(0, adTaskCacheManager.size());
    }

    public void testPutDuplicateTask() throws IOException {
        when(memoryTracker.canAllocateReserved(anyString(), anyLong())).thenReturn(true);
        ADTask adTask1 = TestHelpers.randomAdTask();
        adTaskCacheManager.put(adTask1);
        assertEquals(1, adTaskCacheManager.size());
        IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, () -> adTaskCacheManager.put(adTask1));
        assertEquals("AD task is already running", e1.getMessage());

        ADTask adTask2 = TestHelpers
            .randomAdTask(
                randomAlphaOfLength(5),
                ADTaskState.INIT,
                adTask1.getExecutionEndTime(),
                adTask1.getStoppedBy(),
                adTask1.getDetectorId(),
                adTask1.getDetector()
            );
        IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> adTaskCacheManager.put(adTask2));
        assertEquals("There is one task executing for detector", e2.getMessage());
    }

    public void testPutTaskWithMemoryExceedLimit() {
        when(memoryTracker.canAllocateReserved(anyString(), anyLong())).thenReturn(false);
        LimitExceededException exception = expectThrows(
            LimitExceededException.class,
            () -> adTaskCacheManager.put(TestHelpers.randomAdTask())
        );
        assertEquals("No enough memory to run detector", exception.getMessage());
    }

    public void testThresholdModelTrained() throws IOException {
        when(memoryTracker.canAllocateReserved(anyString(), anyLong())).thenReturn(true);
        ADTask adTask = TestHelpers.randomAdTask();
        adTaskCacheManager.put(adTask);
        assertEquals(1, adTaskCacheManager.size());
        int size = adTaskCacheManager.addThresholdModelTrainingData(adTask.getTaskId(), randomDouble(), randomDouble());
        long cacheSize = adTaskCacheManager.trainingDataMemorySize(size);
        adTaskCacheManager.setThresholdModelTrained(adTask.getTaskId(), false);
        verify(memoryTracker, never()).releaseMemory(anyLong(), anyBoolean(), eq(HISTORICAL_SINGLE_ENTITY_DETECTOR));
        adTaskCacheManager.setThresholdModelTrained(adTask.getTaskId(), true);
        verify(memoryTracker, times(1)).releaseMemory(eq(cacheSize), eq(true), eq(HISTORICAL_SINGLE_ENTITY_DETECTOR));
    }

    public void testCancel() throws IOException {
        when(memoryTracker.canAllocateReserved(anyString(), anyLong())).thenReturn(true);
        ADTask adTask = TestHelpers.randomAdTask();
        adTaskCacheManager.put(adTask);
        assertEquals(1, adTaskCacheManager.size());
        assertEquals(false, adTaskCacheManager.isCancelled(adTask.getTaskId()));
        String cancelReason = randomAlphaOfLength(10);
        String userName = randomAlphaOfLength(5);
        adTaskCacheManager.cancel(adTask.getTaskId(), cancelReason, userName);
        assertEquals(true, adTaskCacheManager.isCancelled(adTask.getTaskId()));
        assertEquals(cancelReason, adTaskCacheManager.getCancelReason(adTask.getTaskId()));
        assertEquals(userName, adTaskCacheManager.getCancelledBy(adTask.getTaskId()));
    }

    public void testTaskNotExist() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> adTaskCacheManager.getRcfModel(randomAlphaOfLength(5))
        );
        assertEquals("AD task not in cache", e.getMessage());
    }

    public void testRemoveTaskWhichNotExist() {
        adTaskCacheManager.remove(randomAlphaOfLength(5));
        verify(memoryTracker, never()).releaseMemory(anyLong(), anyBoolean(), eq(HISTORICAL_SINGLE_ENTITY_DETECTOR));
    }

    public void testExceedRunningTaskLimit() throws IOException {
        when(memoryTracker.canAllocateReserved(anyString(), anyLong())).thenReturn(true);
        adTaskCacheManager.put(TestHelpers.randomAdTask());
        adTaskCacheManager.put(TestHelpers.randomAdTask());
        assertEquals(2, adTaskCacheManager.size());
        LimitExceededException e = expectThrows(LimitExceededException.class, () -> adTaskCacheManager.put(TestHelpers.randomAdTask()));
        assertEquals("Can't run more than 2 historical detectors per data node", e.getMessage());
    }
}
