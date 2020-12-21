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
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_TREES;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.THRESHOLD_MODEL_TRAINING_SIZE;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;

import com.amazon.opendistroforelasticsearch.ad.MemoryTracker;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.randomcutforest.RandomCutForest;

public class ADTaskCacheManager {

    private final Map<String, ADBatchTaskCache> taskCaches;
    private volatile Integer maxAdBatchTaskPerNode;
    private final MemoryTracker memoryTracker;

    /**
     * Constructor to create AD task cache manager.
     *
     * @param settings ES settings
     * @param clusterService ES cluster service
     * @param memoryTracker AD memory tracker
     */
    public ADTaskCacheManager(Settings settings, ClusterService clusterService, MemoryTracker memoryTracker) {
        this.maxAdBatchTaskPerNode = MAX_BATCH_TASK_PER_NODE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_BATCH_TASK_PER_NODE, it -> maxAdBatchTaskPerNode = it);
        taskCaches = new ConcurrentHashMap<>();
        this.memoryTracker = memoryTracker;
    }

    /**
     * Put AD task into cache.
     * If AD task is already in cache, will throw {@link IllegalArgumentException}
     * If there is one AD task in cache for detector, will throw {@link IllegalArgumentException}
     * If there is no enough memory for this AD task, will throw {@link LimitExceededException}
     *
     * @param adTask AD task
     */
    public synchronized void put(ADTask adTask) {
        String taskId = adTask.getTaskId();
        if (contains(taskId)) {
            throw new IllegalArgumentException("AD task is already running");
        }
        if (containsTaskOfDetector(adTask.getDetectorId())) {
            throw new IllegalArgumentException("There is one task executing for detector");
        }
        checkRunningTaskLimit();
        long neededCacheSize = calculateADTaskCacheSize(adTask);
        if (!memoryTracker.canAllocate(neededCacheSize)) {
            throw new LimitExceededException("No enough memory to run detector");
        }
        memoryTracker.consumeMemory(neededCacheSize, false, HISTORICAL_SINGLE_ENTITY_DETECTOR);
        ADBatchTaskCache taskCache = new ADBatchTaskCache(adTask);
        taskCache.getCacheMemorySize().set(neededCacheSize);
        taskCaches.put(taskId, taskCache);
    }

    /**
     * check if current running batch task on current node exceeds
     * max running task limitation.
     * If executing task count exceeds limitation, will throw
     * {@link LimitExceededException}
     */
    public void checkRunningTaskLimit() {
        if (size() >= maxAdBatchTaskPerNode) {
            String error = "Can't run more than " + maxAdBatchTaskPerNode + " historical detectors per data node";
            throw new LimitExceededException(error);
        }
    }

    /**
     * Get task RCF model.
     * If task doesn't exist in cache, will throw {@link java.lang.IllegalArgumentException}.
     *
     * @param taskId AD task id
     * @return RCF model
     */
    public RandomCutForest getRcfModel(String taskId) {
        return getBatchTaskCache(taskId).getRcfModel();
    }

    /**
     * Get task threshold model.
     * If task doesn't exist in cache, will throw {@link java.lang.IllegalArgumentException}.
     *
     * @param taskId AD task id
     * @return threshold model
     */
    public ThresholdingModel getThresholdModel(String taskId) {
        return getBatchTaskCache(taskId).getThresholdModel();
    }

    /**
     * Get threshold model training data.
     * If task doesn't exist in cache, will throw {@link java.lang.IllegalArgumentException}.
     *
     * @param taskId AD task id
     * @return threshold model training data
     */
    public List<Double> getThresholdModelTrainingData(String taskId) {
        return getBatchTaskCache(taskId).getThresholdModelTrainingData();
    }

    /**
     * Threshold model trained or not.
     * If task doesn't exist in cache, will throw {@link java.lang.IllegalArgumentException}.
     *
     * @param taskId AD task id
     * @return true if threshold model trained; otherwise, return false
     */
    public boolean isThresholdModelTrained(String taskId) {
        return getBatchTaskCache(taskId).isThresholdModelTrained();
    }

    /**
     * Set threshold model trained or not.
     *
     * @param taskId task id
     * @param trained threshold model trained or not
     */
    public void setThresholdModelTrained(String taskId, boolean trained) {
        ADBatchTaskCache taskCache = getBatchTaskCache(taskId);
        taskCache.setThresholdModelTrained(trained);
        if (trained) {
            int size = taskCache.getThresholdModelTrainingData().size();
            long cacheSize = trainingDataMemorySize(size);
            taskCache.getThresholdModelTrainingData().clear();
            taskCache.getCacheMemorySize().getAndAdd(-cacheSize);
            memoryTracker.releaseMemory(cacheSize, false, HISTORICAL_SINGLE_ENTITY_DETECTOR);
        }
    }

    /**
     * Get shingle data.
     *
     * @param taskId AD task id
     * @return shingle data
     */
    public Deque<Map.Entry<Long, Optional<double[]>>> getShingle(String taskId) {
        return getBatchTaskCache(taskId).getShingle();
    }

    /**
     * Check if task exists in cache.
     *
     * @param taskId task id
     * @return true if task exists in cache; otherwise, return false.
     */
    public boolean contains(String taskId) {
        return taskCaches.containsKey(taskId);
    }

    /**
     * Check if there is task in cache for detector.
     *
     * @param detectorId detector id
     * @return true if there is task in cache; otherwise return false
     */
    public boolean containsTaskOfDetector(String detectorId) {
        return taskCaches.values().stream().filter(v -> Objects.equals(detectorId, v.getDetectorId())).findAny().isPresent();
    }

    /**
     * Get batch task cache. If task doesn't exist in cache, will throw
     * {@link java.lang.IllegalArgumentException}
     *
     * @param taskId task id
     * @return AD batch task cache
     */
    private ADBatchTaskCache getBatchTaskCache(String taskId) {
        if (!contains(taskId)) {
            throw new IllegalArgumentException("AD task not in cache");
        }
        return taskCaches.get(taskId);
    }

    /**
     * Calculate AD task cache memory usage.
     *
     * @param adTask AD task
     * @return how many bytes will consume
     */
    private long calculateADTaskCacheSize(ADTask adTask) {
        return memoryTracker.estimateModelSize(adTask.getDetector(), NUM_TREES) + trainingDataMemorySize(THRESHOLD_MODEL_TRAINING_SIZE)
            + shingleMemorySize(adTask.getDetector().getShingleSize());
    }

    /**
     * Remove task from cache.
     *
     * @param taskId AD task id
     */
    public void remove(String taskId) {
        if (contains(taskId)) {
            memoryTracker.releaseMemory(getBatchTaskCache(taskId).getCacheMemorySize().get(), false, HISTORICAL_SINGLE_ENTITY_DETECTOR);
            taskCaches.remove(taskId);
        }
    }

    /**
     * Cancel AD task.
     *
     * @param taskId AD task id
     * @param reason why need to cancel task
     * @param userName user name
     */
    public void cancel(String taskId, String reason, String userName) {
        getBatchTaskCache(taskId).cancel(reason, userName);
    }

    /**
     * Task is cancelled or not.
     *
     * @param taskId AD task id
     * @return true if task is cancelled; otherwise return false
     */
    public boolean isCancelled(String taskId) {
        ADBatchTaskCache taskCache = getBatchTaskCache(taskId);
        return taskCache.isCancelled();
    }

    /**
     * Get why task cancelled.
     *
     * @param taskId AD task id
     * @return task cancellation reason
     */
    public String getCancelReason(String taskId) {
        return getBatchTaskCache(taskId).getCancelReason();
    }

    /**
     * Get task cancelled by which user.
     *
     * @param taskId AD task id
     * @return user name
     */
    public String getCancelledBy(String taskId) {
        return getBatchTaskCache(taskId).getCancelledBy();
    }

    /**
     * Get current task count in cache.
     *
     * @return task count
     */
    public int size() {
        return taskCaches.size();
    }

    /**
     * Clear all tasks.
     */
    public void clear() {
        taskCaches.clear();
    }

    /**
     * Estimate max memory usage of model training data.
     * The training data is double and will cache in {@link java.util.ArrayList}.
     * Check {@link ADBatchTaskCache#getThresholdModelTrainingData()}
     *
     * @param size training data point count
     * @return how many bytes will consume
     */
    public long trainingDataMemorySize(int size) {
        return 24 * size;
    }

    /**
     * Estimate max memory usage of shingle data.
     * Based on the test, one shingle data point consumes about 96 bytes.
     * The shingle data is stored in {@link java.util.Deque}
     * Check {@link ADBatchTaskCache#getShingle()}
     *
     * @param shingleSize shingle data point count
     * @return how many bytes will consume
     */
    public long shingleMemorySize(int shingleSize) {
        return 96 * shingleSize;
    }

}
