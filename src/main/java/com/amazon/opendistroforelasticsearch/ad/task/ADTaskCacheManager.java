/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import static com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages.DETECTOR_IS_RUNNING;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_TREES;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.THRESHOLD_MODEL_TRAINING_SIZE;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.model.Entity;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;

import com.amazon.opendistroforelasticsearch.ad.MemoryTracker;
import com.amazon.opendistroforelasticsearch.ad.common.exception.DuplicateTaskException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.randomcutforest.RandomCutForest;

public class ADTaskCacheManager {
    private final Logger logger = LogManager.getLogger(ADTaskCacheManager.class);
    private final Map<String, ADBatchTaskCache> taskCaches;
    private volatile Integer maxAdBatchTaskPerNode;
    private final MemoryTracker memoryTracker;
    private final int numberSize = 8;

    // We use this field to record all detectors which running on the
    // coordinating node to resolve race condition. We will check if
    // detector id exists in cache or not first. If user starts
    // multiple tasks for the same detector, we will put the first
    // task in cache. For other tasks, we find the detector id exists,
    // that means there is already one task running for this detector,
    // so we will reject the task.
    private Set<String> detectors;

    //TODO: cualculate memory usage, support multiple category field
    private Map<String, Queue<String>> pendingEntities;
    private Map<String, Queue<String>> runningEntities;

    private Map<String, Integer> entityCount;
    private Map<String, AtomicBoolean> detectorTaskUpdating;
    private Map<String, Boolean> topEntitiesInited;

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
        this.detectors = Sets.newConcurrentHashSet();
        this.pendingEntities = new ConcurrentHashMap<>();
        this.runningEntities = new ConcurrentHashMap<>();
        this.entityCount = new ConcurrentHashMap<>();
        this.topEntitiesInited = new ConcurrentHashMap<>();
        this.detectorTaskUpdating = new ConcurrentHashMap<>();
    }

    /**
     * Put AD task into cache.
     * If AD task is already in cache, will throw {@link IllegalArgumentException}
     * If there is one AD task in cache for detector, will throw {@link IllegalArgumentException}
     * If there is no enough memory for this AD task, will throw {@link LimitExceededException}
     *
     * @param adTask AD task
     */
    public synchronized void add(ADTask adTask) {
        String taskId = adTask.getTaskId();
        if (contains(taskId)) {
            throw new DuplicateTaskException(DETECTOR_IS_RUNNING);
        }
        //TODO: handle HC detector
        if (!adTask.isEntityTask() && containsTaskOfDetector(adTask.getDetectorId())) {
            throw new DuplicateTaskException(DETECTOR_IS_RUNNING);
        }
        checkRunningTaskLimit();
        long neededCacheSize = calculateADTaskCacheSize(adTask);
        if (!memoryTracker.canAllocateReserved(adTask.getDetectorId(), neededCacheSize)) {
            throw new LimitExceededException("No enough memory to run detector");
        }
        memoryTracker.consumeMemory(neededCacheSize, true, HISTORICAL_SINGLE_ENTITY_DETECTOR);
        ADBatchTaskCache taskCache = new ADBatchTaskCache(adTask);
        taskCache.getCacheMemorySize().set(neededCacheSize);
        taskCaches.put(taskId, taskCache);

//        if (adTask.getEntity() != null && adTask.getEntity().size() > 0) {
//            this.moveToRunningEntities(adTask.getDetectorId(), adTask.getEntity().get(0).getValue());
//        }
    }

    /**
     * Put detector id in running detector cache.
     *
     * @param detectorId detector id
     * @throws LimitExceededException throw limit exceed exception when the detector id already in cache
     */
    public synchronized void add(String detectorId) {
        if (detectors.contains(detectorId)) {
            logger.debug("detector is already in running detector cache, detectorId: " + detectorId);
            throw new DuplicateTaskException(DETECTOR_IS_RUNNING);
        }
        logger.debug("add detector in running detector cache, detectorId: " + detectorId);
        this.detectors.add(detectorId);
    }

    //TODO: support multiple category field
//    public void addEntities(String detectorId, Set<String> newEntities) {
//        if (newEntities == null || newEntities.size() == 0) {
//            return;
//        }
//        if (this.entities.containsKey(detectorId)) {
//            Queue<String> queue = this.entities.get(detectorId);
//            for (String entity : newEntities) {
//                if (!queue.contains(entity)) {
//                    queue.add(entity);
//                }
//            }
//        } else {
//            ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
//            queue.addAll(newEntities);
//            this.entities.put(detectorId, queue);
//        }
//    }

    public void addEntity(String detectorId, String newEntities) {
        addEntities(detectorId, ImmutableList.of(newEntities));
    }

    public void addEntities(String detectorId, List<String> newEntities) {
        if (newEntities == null || newEntities.size() == 0) {
            return;
        }
        if (this.pendingEntities.containsKey(detectorId)) {
            Queue<String> queue = this.pendingEntities.get(detectorId);
            for (String entity : newEntities) {
                if (entity != null && !queue.contains(entity)) {
                    queue.add(entity);
                }
            }
        } else {
            ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
            queue.addAll(newEntities);
            this.pendingEntities.put(detectorId, queue);
        }
    }

    private void moveToRunningEntities(String detectorId, String entity) {
        if (this.runningEntities.containsKey(detectorId)) {
//            logger.info(" -------- add entity to runnning entity cache : " + entity);
            Queue<String> queue = this.runningEntities.get(detectorId);
            if (!queue.contains(entity)) {
                queue.add(entity);//TODO:null pointer exception
            }
        } else {
//            logger.info(" 22 -------- add entity to runnning entity cache : " + entity);
            ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
            queue.add(entity);
            this.runningEntities.put(detectorId, queue);
        }
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
    public double[] getThresholdModelTrainingData(String taskId) {
        return getBatchTaskCache(taskId).getThresholdModelTrainingData();
    }

    /**
     * Get threshhold model training data size in bytes.
     *
     * @param taskId task id
     * @return training data size in bytes
     */
    public int getThresholdModelTrainingDataSize(String taskId) {
        return getBatchTaskCache(taskId).getThresholdModelTrainingDataSize().get();
    }

    /**
     * Add threshold model training data.
     *
     * @param taskId task id
     * @param data training data
     * @return latest threshold model training data size after adding new data
     */
    public int addThresholdModelTrainingData(String taskId, double... data) {
        ADBatchTaskCache taskCache = getBatchTaskCache(taskId);
        double[] thresholdModelTrainingData = taskCache.getThresholdModelTrainingData();
        AtomicInteger size = taskCache.getThresholdModelTrainingDataSize();
        int dataPointsAdded = Math.min(data.length, THRESHOLD_MODEL_TRAINING_SIZE - size.get());
        System.arraycopy(data, 0, thresholdModelTrainingData, size.get(), dataPointsAdded);
        return size.addAndGet(dataPointsAdded);
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
    protected void setThresholdModelTrained(String taskId, boolean trained) {
        ADBatchTaskCache taskCache = getBatchTaskCache(taskId);
        taskCache.setThresholdModelTrained(trained);
        if (trained) {
            int size = taskCache.getThresholdModelTrainingDataSize().get();
            long cacheSize = trainingDataMemorySize(size);
            taskCache.clearTrainingData();
            taskCache.getCacheMemorySize().getAndAdd(-cacheSize);
            memoryTracker.releaseMemory(cacheSize, true, HISTORICAL_SINGLE_ENTITY_DETECTOR);
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
     * Get task id list of detector.
     *
     * @param detectorId detector id
     * @return list of task id
     */
    public List<String> getTasksOfDetector(String detectorId) {
        return taskCaches
            .values()
            .stream()
            .filter(v -> Objects.equals(detectorId, v.getDetectorId()))
            .map(c -> c.getTaskId())
            .collect(Collectors.toList());
    }

    /**
     * Get batch task cache. If task doesn't exist in cache, will throw
     * {@link java.lang.IllegalArgumentException}
     * We throw exception rather than return {@code Optional.empty} or null
     * here, so don't need to check task existence by writing duplicate null
     * checking code. All AD task exceptions will be handled in AD task manager.
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

    private List<ADBatchTaskCache> getBatchTaskCacheByDetectorId(String detectorId) {
        return taskCaches.values().stream().filter(v -> Objects.equals(detectorId, v.getDetectorId())).collect(Collectors.toList());
    }

    /**
     * Calculate AD task cache memory usage.
     *
     * @param adTask AD task
     * @return how many bytes will consume
     */
    private long calculateADTaskCacheSize(ADTask adTask) {
        AnomalyDetector detector = adTask.getDetector();
        return memoryTracker.estimateModelSize(detector, NUM_TREES) + trainingDataMemorySize(THRESHOLD_MODEL_TRAINING_SIZE)
            + shingleMemorySize(detector.getShingleSize(), detector.getEnabledFeatureIds().size());
    }

    /**
     * Get RCF model size in bytes.
     *
     * @param taskId task id
     * @return model size in bytes
     */
    public long getModelSize(String taskId) {
        ADBatchTaskCache batchTaskCache = getBatchTaskCache(taskId);
        int dimensions = batchTaskCache.getRcfModel().getDimensions();
        int numberOfTrees = batchTaskCache.getRcfModel().getNumberOfTrees();
        return memoryTracker.estimateModelSize(dimensions, numberOfTrees, NUM_SAMPLES_PER_TREE);
    }

    /**
     * Remove task from cache.
     *
     * @param taskId AD task id
     */
//    public void remove(String taskId, List<Entity> entity) {
    public void remove(String taskId) {
        if (contains(taskId)) {
            ADBatchTaskCache taskCache = getBatchTaskCache(taskId);
            memoryTracker.releaseMemory(taskCache.getCacheMemorySize().get(), true, HISTORICAL_SINGLE_ENTITY_DETECTOR);
            taskCaches.remove(taskId);
            // can't remove detector id from cache here as it's possible that some task running on
            // other worker nodes
//            if(entity != null && entity.size() > 0){
//                String detectorId = taskCache.getDetectorId();
//                runningEntities.get(detectorId).remove(entity.get(0));
//            }
            //TODO: check if node is coordinating node, if yes, we can remove running entity
//            removeRunningEntity(taskCache.getDetectorId(), entity);
        }
    }

    public void removeRunningEntity(String detectorId, List<Entity> entity) {

        if(entity != null && entity.size() > 0 && runningEntities.containsKey(detectorId)) {
            String runningEntity = entity.get(0).getValue();
            logger.debug("Remove entity from running entities cache: " + runningEntity);
            logger.debug("Pending entity count: " + pendingEntities.get(detectorId).size()
                    + ", Running entity count: " + runningEntities.get(detectorId).size());
            runningEntities.get(detectorId).remove(runningEntity);
        }
    }

    /**
     * Remove detector id from running detector cache
     *
     * @param detectorId detector id
     */
    public void removeDetector(String detectorId) {
        if (hasEntity(detectorId)) {
            throw new AnomalyDetectionException("Can't remove detector from cache as there is running entity tasks");
        }
        if (detectors.contains(detectorId)) {
            detectors.remove(detectorId);
            logger.debug("Removed detector from AD task coordinating node cache, detectorId: " + detectorId);
        } else {
            logger.debug("Detector is not in AD task coordinating node cache");
        }
        if (pendingEntities.containsKey(detectorId)) {
            pendingEntities.remove(detectorId);
            logger.debug("Removed detector from AD task coordinating node entities cache, detectorId: " + detectorId);
        }
        if (runningEntities.containsKey(detectorId)) {
            runningEntities.remove(detectorId);
            logger.debug("Removed detector from AD task coordinating node entities cache, detectorId: " + detectorId);
        }
        if (entityCount.containsKey(detectorId)) {
            entityCount.remove(detectorId);
            logger.debug("Removed detector from AD task coordinating node entities count cache, detectorId: " + detectorId);
        }
        if (topEntitiesInited.containsKey(detectorId)) {
            topEntitiesInited.remove(detectorId);
            logger.debug("Removed detector from AD task coordinating node entities initialization cache, detectorId: " + detectorId);
        }
        if (detectorTaskUpdating.containsKey(detectorId)) {
            detectorTaskUpdating.remove(detectorId);
            logger.debug("Removed detector from AD task coordinating node detector task updating cache, detectorId: " + detectorId);
        }
    }

    /**
     * Cancel AD task.
     *
     * @param taskId AD task id
     * @param reason why need to cancel task
     * @param userName user name
     * @return AD task cancellation state
     */
    public ADTaskCancellationState cancel(String taskId, String reason, String userName) {
        if (!contains(taskId)) {
            return ADTaskCancellationState.NOT_FOUND;
        }
        if (isCancelled(taskId)) {
            return ADTaskCancellationState.ALREADY_CANCELLED;
        }
        getBatchTaskCache(taskId).cancel(reason, userName);
        return ADTaskCancellationState.CANCELLED;
    }

    /**
     * Cancel AD task by detector id.
     *
     * @param detectorId detector id
     * @param reason why need to cancel task
     * @param userName user name
     * @return AD task cancellation state
     */
    public ADTaskCancellationState cancelByDetectorId(String detectorId, String reason, String userName) {
        List<ADBatchTaskCache> taskCaches = getBatchTaskCacheByDetectorId(detectorId);

        if (taskCaches.isEmpty()) {
            return ADTaskCancellationState.NOT_FOUND;
        }

        ADTaskCancellationState cancellationState = ADTaskCancellationState.ALREADY_CANCELLED;
        for (ADBatchTaskCache cache : taskCaches) {
            if (!cache.isCancelled()) {
                cancellationState = ADTaskCancellationState.CANCELLED;
                cache.cancel(reason, userName);
            }
        }
        return cancellationState;
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
        detectors.clear();
    }

    /**
     * Estimate max memory usage of model training data.
     * The training data is double and will cache in double array.
     * One double consumes 8 bytes.
     *
     * @param size training data point count
     * @return how many bytes will consume
     */
    public long trainingDataMemorySize(int size) {
        return numberSize * size;
    }

    /**
     * Estimate max memory usage of shingle data.
     * One feature aggregated data point(double) consumes 8 bytes.
     * The shingle data is stored in {@link java.util.Deque}. From testing,
     * other parts except feature data consume 80 bytes.
     *
     * Check {@link ADBatchTaskCache#getShingle()}
     *
     * @param shingleSize shingle data point count
     * @param enabledFeatureSize enabled feature count
     * @return how many bytes will consume
     */
    public long shingleMemorySize(int shingleSize, int enabledFeatureSize) {
        return (80 + numberSize * enabledFeatureSize) * shingleSize;
    }

    /**
     * Poll one entity from HC detector entities cache.
     * Will return null if no entities in cache.
     *
     * @param detectorId detector id
     * @return one entity
     */
    public synchronized String pollEntity(String detectorId) { //TODO add syncronize?
        String entity = pendingEntities.containsKey(detectorId)? pendingEntities.get(detectorId).poll() : null;
        if (entity != null) {
            this.moveToRunningEntities(detectorId, entity);
        }
        return entity;
    }

    public boolean hasEntity(String detectorId) {
        logger.debug("ylwudebug: pending entities contains detector: {}, not empty pending entity: {} ",
                pendingEntities.containsKey(detectorId), pendingEntities.containsKey(detectorId) && !pendingEntities.get(detectorId).isEmpty());
        logger.debug("ylwudebug: running entities contains detector: {}, not empty running entity: {} ",
                runningEntities.containsKey(detectorId), runningEntities.containsKey(detectorId) && !runningEntities.get(detectorId).isEmpty());
        return (pendingEntities.containsKey(detectorId) && !pendingEntities.get(detectorId).isEmpty()) ||
                (runningEntities.containsKey(detectorId) && !runningEntities.get(detectorId).isEmpty());
    }

    public boolean hcDetectorInCache(String detectorId) {
        logger.info("11111------- pending entities contains detector: {} ",
                pendingEntities.containsKey(detectorId));
        logger.info("11111------- running entities contains detector: {}",
                runningEntities.containsKey(detectorId));
        return pendingEntities.containsKey(detectorId)  || runningEntities.containsKey(detectorId);
    }

    public int pendingEntityCount(String detectorId) {
        return pendingEntities.containsKey(detectorId) ? pendingEntities.get(detectorId).size() : 0;
    }

    public int runningEntitiesCount(String detectorId) {
        return runningEntities.containsKey(detectorId) ? runningEntities.get(detectorId).size() : 0;
    }

    public void setEntityCount(String detectorId, Integer count) {
        entityCount.put(detectorId, count);
    }

    public Integer getEntityCount(String detectorId) {
        return entityCount.containsKey(detectorId) ? entityCount.get(detectorId) : 0;
    }

    public boolean topEntityInited(String detectorId) {
        if (!topEntitiesInited.containsKey(detectorId)) {
            return false;
        }
        return topEntitiesInited.get(detectorId);
    }

    public void putTopEntityInited(String detectorId, boolean inited) {
        topEntitiesInited.put(detectorId, inited);
    }

    public boolean hasRunningEntity(String detectorId) {
        return runningEntities.containsKey(detectorId) && runningEntities.get(detectorId).size() > 0;
    }

    public AtomicBoolean detectorTaskUpdating(String detectorId) {
        return detectorTaskUpdating.computeIfAbsent(detectorId, id -> new AtomicBoolean(false));
    }
}
