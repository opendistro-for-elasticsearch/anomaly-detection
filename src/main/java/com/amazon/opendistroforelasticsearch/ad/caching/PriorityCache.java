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

package com.amazon.opendistroforelasticsearch.ad.caching;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.DEDICATED_CACHE_SIZE;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.MemoryTracker;
import com.amazon.opendistroforelasticsearch.ad.MemoryTracker.Origin;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.ml.CheckpointDao;
import com.amazon.opendistroforelasticsearch.ad.ml.EntityModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager.ModelType;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelState;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.ratelimit.CheckpointWriteQueue;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class PriorityCache implements EntityCache {
    private final Logger LOG = LogManager.getLogger(PriorityCache.class);

    // detector id -> CacheBuffer, weight based
    private final Map<String, CacheBuffer> activeEnities;
    private final CheckpointDao checkpointDao;
    private int dedicatedCacheSize;
    // LRU Cache
    private Cache<String, ModelState<EntityModel>> inActiveEntities;
    private final MemoryTracker memoryTracker;
    private final ReentrantLock maintenanceLock;
    private final int numberOfTrees;
    private final Clock clock;
    private final Duration modelTtl;
    // A bloom filter placed in front of inactive entity cache to
    // filter out unpopular items that are not likely to appear more
    // than once.
    private Map<String, DoorKeeper> doorKeepers;
    private ThreadPool threadPool;
    private Random random;
    private CheckpointWriteQueue checkpointWriteQueue;
    // iterating through all of inactive entities is heavy. We don't want to do
    // it again and again for no obvious benefits.
    private Instant lastInActiveEntityMaintenance;

    public PriorityCache(
        CheckpointDao checkpointDao,
        int dedicatedCacheSize,
        Duration inactiveEntityTtl,
        int maxInactiveStates,
        MemoryTracker memoryTracker,
        int numberOfTrees,
        Clock clock,
        ClusterService clusterService,
        Duration modelTtl,
        ThreadPool threadPool,
        CheckpointWriteQueue checkpointWriteQueue
    ) {
        this.checkpointDao = checkpointDao;

        this.dedicatedCacheSize = dedicatedCacheSize;
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DEDICATED_CACHE_SIZE, it -> this.dedicatedCacheSize = it);

        this.activeEnities = new ConcurrentHashMap<>();
        this.memoryTracker = memoryTracker;
        this.maintenanceLock = new ReentrantLock();
        this.numberOfTrees = numberOfTrees;
        this.clock = clock;
        this.modelTtl = modelTtl;
        this.doorKeepers = new ConcurrentHashMap<>();

        this.inActiveEntities = CacheBuilder
            .newBuilder()
            .expireAfterAccess(inactiveEntityTtl.toHours(), TimeUnit.HOURS)
            .maximumSize(maxInactiveStates)
            .concurrencyLevel(1)
            .build();

        this.threadPool = threadPool;
        this.random = new Random(42);
        this.checkpointWriteQueue = checkpointWriteQueue;
        this.lastInActiveEntityMaintenance = Instant.MIN;
    }

    @Override
    public ModelState<EntityModel> get(String modelId, AnomalyDetector detector) {
        String detectorId = detector.getDetectorId();
        CacheBuffer buffer = computeBufferIfAbsent(detector, detectorId);
        ModelState<EntityModel> modelState = buffer.get(modelId);

        // during maintenance period, stop putting new entries
        if (!maintenanceLock.isLocked() && modelState == null) {
            DoorKeeper doorKeeper = doorKeepers
                .computeIfAbsent(
                    detectorId,
                    id -> {
                        // reset every 60 intervals
                        return new DoorKeeper(
                            AnomalyDetectorSettings.DOOR_KEEPER_FOR_CACHE_MAX_INSERTION,
                            AnomalyDetectorSettings.DOOR_KEEPER_FAULSE_POSITIVE_RATE,
                            detector.getDetectionIntervalDuration().multipliedBy(60),
                            clock
                        );
                    }
                );

            // first hit, ignore
            if (doorKeeper.mightContain(modelId) == false) {
                doorKeeper.put(modelId);
                return null;
            }

            try {
                ModelState<EntityModel> state = inActiveEntities.get(modelId, new Callable<ModelState<EntityModel>>() {
                    @Override
                    public ModelState<EntityModel> call() {
                        return new ModelState<>(null, modelId, detectorId, ModelType.ENTITY.getName(), clock, 0);
                    }
                });

                // make sure no model has been stored due to previous race conditions
                state.setModel(null);

                // compute updated priority
                // We don’t want to admit the latest entity for correctness by throwing out a
                // hot entity. We have a priority (time-decayed count) sensitive to
                // the number of hits, length of time, and sampling interval. Examples:
                // 1) an entity from a 5-minute interval detector that is hit 5 times in the
                // past 25 minutes should have an equal chance of using the cache along with
                // an entity from a 1-minute interval detector that is hit 5 times in the past
                // 5 minutes.
                // 2) It might be the case that the frequency of entities changes dynamically
                // during run-time. For example, entity A showed up for the first 500 times,
                // but entity B showed up for the next 500 times. Our priority should give
                // entity B higher priority than entity A as time goes by.
                // 3) Entity A will have a higher priority than entity B if A runs
                // for a longer time given other things are equal.
                //
                // We ensure fairness by using periods instead of absolute duration. Entity A
                // accessed once three intervals ago should have the same priority with entity B
                // accessed once three periods ago, though they belong to detectors of different
                // intervals.

                // update state using new priority or create a new one
                state.setPriority(buffer.getPriorityTracker().getUpdatedPriority(state.getPriority()));
            } catch (Exception e) {
                LOG.error(new ParameterizedMessage("Fail to update priority of [{}]", modelId), e);
            }

        }

        return modelState;
    }

    private Optional<ModelState<EntityModel>> getStateFromInactiveEntiiyCache(String modelId) {
        if (modelId == null) {
            return Optional.empty();
        }

        // null if not even recorded in inActiveEntities yet because of doorKeeper
        return Optional.ofNullable(inActiveEntities.getIfPresent(modelId));
    }

    @Override
    public boolean hostIfPossible(AnomalyDetector detector, ModelState<EntityModel> toUpdate) {
        if (toUpdate == null) {
            return false;
        }
        String modelId = toUpdate.getModelId();
        String detectorId = toUpdate.getDetectorId();

        if (Strings.isEmpty(modelId) || Strings.isEmpty(detectorId)) {
            return false;
        }

        CacheBuffer buffer = computeBufferIfAbsent(detector, detectorId);

        Optional<ModelState<EntityModel>> state = getStateFromInactiveEntiiyCache(modelId);
        if (false == state.isPresent()) {
            return false;
        }

        ModelState<EntityModel> modelState = state.get();

        float priority = modelState.getPriority();

        toUpdate.setLastUsedTime(clock.instant());
        toUpdate.setPriority(priority);

        // current buffer's dedicated cache has free slots or can allocate in shared cache
        if (buffer.dedicatedCacheAvailable() || memoryTracker.canAllocate(buffer.getMemoryConsumptionPerEntity())) {
            buffer.put(modelId, toUpdate);
            return true;
        }

        // can replace an entity in the same CacheBuffer living in reserved or shared cache
        if (buffer.canReplaceWithinDetector(priority)) {
            ModelState<EntityModel> removed = buffer.replace(modelId, toUpdate);
            // null in the case of some other threads have emptied the queue at
            // the same time so there is nothing to replace
            if (removed != null) {
                // set last used time for profile API so that we know when an entities is evicted
                removed.setLastUsedTime(clock.instant());
                inActiveEntities.put(removed.getModelId(), removed);
                return true;
            }
        }

        // If two threads try to remove the same entity and add their own state, the 2nd remove
        // returns null and only the first one succeeds.
        float scaledPriority = buffer.getPriorityTracker().getScaledPriority(priority);
        Triple<CacheBuffer, String, Float> bufferToRemoveEntity = canReplaceInSharedCache(buffer, scaledPriority);
        CacheBuffer bufferToRemove = bufferToRemoveEntity.getLeft();
        String entityModelId = bufferToRemoveEntity.getMiddle();
        ModelState<EntityModel> removed = null;
        if (bufferToRemove != null && ((removed = bufferToRemove.remove(entityModelId)) != null)) {
            buffer.put(modelId, toUpdate);
            // set last used time for profile API so that we know when an entities is evicted
            removed.setLastUsedTime(clock.instant());
            inActiveEntities.put(removed.getModelId(), removed);
            return true;
        }

        return false;
    }

    @Override
    public Pair<List<String>, List<String>> selectUpdateCandidate(
        Collection<String> cacheMissEntities,
        String detectorId,
        AnomalyDetector detector
    ) {
        List<String> hotEntities = new ArrayList<>();
        CacheBuffer buffer = computeBufferIfAbsent(detector, detectorId);
        Iterator<String> cacheMissEntitiesIter = cacheMissEntities.iterator();
        // current buffer's dedicated cache has free slots
        while (cacheMissEntitiesIter.hasNext() && buffer.dedicatedCacheAvailable()) {
            hotEntities.add(cacheMissEntitiesIter.next());
        }

        while (cacheMissEntitiesIter.hasNext() && memoryTracker.canAllocate(buffer.getMemoryConsumptionPerEntity())) {
            // can allocate in shared cache
            // race conditions can happen when multiple threads evaluating this condition.
            // This is a problem as our AD memory usage is close to full and we put
            // more things than we planned. One model in HCAD is small,
            // it is fine we exceed a little. We have regular maintenance to remove
            // extra memory usage.
            hotEntities.add(cacheMissEntitiesIter.next());
        }

        // check if we can replace anything in dedicated or shared cache
        // have a copy since we need to do the iteration twice: one for
        // dedicated cache and one for shared cache
        List<String> otherBufferReplaceCandidates = new ArrayList<>();

        while (cacheMissEntitiesIter.hasNext()) {
            // can replace an entity in the same CacheBuffer living in reserved
            // or shared cache
            // thread safe as each detector has one thread at one time and only the
            // thread can access its buffer.
            String modelId = cacheMissEntitiesIter.next();

            Optional<ModelState<EntityModel>> state = getStateFromInactiveEntiiyCache(modelId);
            if (false == state.isPresent()) {
                // not even recorded in inActiveEntities yet because of doorKeeper
                continue;
            }

            ModelState<EntityModel> modelState = state.get();
            float priority = modelState.getPriority();

            if (buffer.canReplaceWithinDetector(priority)) {
                hotEntities.add(modelId);
            } else {
                // re-evaluate replacement condition in other buffers
                otherBufferReplaceCandidates.add(modelId);
            }
        }

        // record current minimum priority among all detectors to save redundant
        // scanning of all CacheBuffers
        CacheBuffer bufferToRemove = null;
        float minPriority = Float.MIN_VALUE;

        // check if we can replace in other CacheBuffer
        cacheMissEntitiesIter = otherBufferReplaceCandidates.iterator();

        List<String> coldEntities = new ArrayList<>();

        while (cacheMissEntitiesIter.hasNext()) {
            // If two threads try to remove the same entity and add their own state, the 2nd remove
            // returns null and only the first one succeeds.
            String modelId = cacheMissEntitiesIter.next();
            Optional<ModelState<EntityModel>> maybeState = getStateFromInactiveEntiiyCache(modelId);
            if (false == maybeState.isPresent()) {
                // empty state should not stand a chance to replace others
                continue;
            }

            ModelState<EntityModel> state = maybeState.get();

            float priority = state.getPriority();
            float scaledPriority = buffer.getPriorityTracker().getScaledPriority(priority);

            if (scaledPriority <= minPriority) {
                // not even larger than the minPriority, we can put this to coldEntities
                coldEntities.add(modelId);
                continue;
            }

            // Float.MIN_VALUE means we need to re-iterate through all CacheBuffers
            if (minPriority == Float.MIN_VALUE) {
                Triple<CacheBuffer, String, Float> bufferToRemoveEntity = canReplaceInSharedCache(buffer, scaledPriority);
                bufferToRemove = bufferToRemoveEntity.getLeft();
                minPriority = bufferToRemoveEntity.getRight();
            }

            if (bufferToRemove != null) {
                hotEntities.add(modelId);
                // reset minPriority after the replacement so that we need to iterate all CacheBuffer
                // again
                minPriority = Float.MIN_VALUE;
            } else {
                // after trying everything, we can now safely put this to cold entities list
                coldEntities.add(modelId);
            }
        }

        return Pair.of(hotEntities, coldEntities);
    }

    private CacheBuffer computeBufferIfAbsent(AnomalyDetector detector, String detectorId) {
        return activeEnities.computeIfAbsent(detectorId, k -> {
            long requiredBytes = getReservedDetectorMemory(detector);
            tryClearUpMemory();
            if (memoryTracker.canAllocateReserved(detectorId, requiredBytes)) {
                memoryTracker.consumeMemory(requiredBytes, true, Origin.MULTI_ENTITY_DETECTOR);
                long intervalSecs = detector.getDetectorIntervalInSeconds();
                return new CacheBuffer(
                    dedicatedCacheSize,
                    intervalSecs,
                    memoryTracker.estimateModelSize(detector, numberOfTrees),
                    memoryTracker,
                    clock,
                    modelTtl,
                    detectorId,
                    checkpointWriteQueue,
                    random
                );
            }
            // if hosting not allowed, exception will be thrown by isHostingAllowed
            throw new LimitExceededException(detectorId, CommonErrorMessages.MEMORY_LIMIT_EXCEEDED_ERR_MSG);
        });
    }

    private long getReservedDetectorMemory(AnomalyDetector detector) {
        return dedicatedCacheSize * memoryTracker.estimateModelSize(detector, numberOfTrees);
    }

    /**
     * Whether the candidate entity can replace any entity in the shared cache.
     * We can have race conditions when multiple threads try to evaluate this
     * function. The result is that we can have multiple threads thinks they
     * can replace entities in the cache.
     *
     *
     * @param originBuffer the CacheBuffer that the entity belongs to (with the same detector Id)
     * @param candidatePriority the candidate entity's priority
     * @return the CacheBuffer if we can find a CacheBuffer to make room for the candidate entity
     */
    private Triple<CacheBuffer, String, Float> canReplaceInSharedCache(CacheBuffer originBuffer, float candidatePriority) {
        CacheBuffer minPriorityBuffer = null;
        float minPriority = candidatePriority;
        String minPriorityEntityModelId = null;
        for (Map.Entry<String, CacheBuffer> entry : activeEnities.entrySet()) {
            CacheBuffer buffer = entry.getValue();
            if (buffer != originBuffer && buffer.canRemove()) {
                Entry<String, Float> priorityEntry = buffer.getPriorityTracker().getMinimumScaledPriority();
                float priority = priorityEntry.getValue();
                if (candidatePriority > priority && priority < minPriority) {
                    minPriority = priority;
                    minPriorityBuffer = buffer;
                    minPriorityEntityModelId = priorityEntry.getKey();
                }
            }
        }
        return Triple.of(minPriorityBuffer, minPriorityEntityModelId, minPriority);
    }

    private void tryClearUpMemory() {
        try {
            if (maintenanceLock.tryLock()) {
                clearMemory();
            } else {
                threadPool.schedule(() -> {
                    try {
                        tryClearUpMemory();
                    } catch (Exception e) {
                        LOG.error("Fail to clear up memory taken by CacheBuffer.  Will retry during maintenance.");
                    }
                }, new TimeValue(random.nextInt(90), TimeUnit.SECONDS), AnomalyDetectorPlugin.AD_THREAD_POOL_NAME);
            }
        } finally {
            if (maintenanceLock.isHeldByCurrentThread()) {
                maintenanceLock.unlock();
            }
        }
    }

    private void clearMemory() {
        recalculateUsedMemory();
        long memoryToShed = memoryTracker.memoryToShed();
        float minPriority = Float.MAX_VALUE;
        CacheBuffer minPriorityBuffer = null;
        String minPriorityEntityModelId = null;
        while (memoryToShed > 0) {
            for (Map.Entry<String, CacheBuffer> entry : activeEnities.entrySet()) {
                CacheBuffer buffer = entry.getValue();
                Entry<String, Float> priorityEntry = buffer.getPriorityTracker().getMinimumScaledPriority();
                float priority = priorityEntry.getValue();
                if (buffer.canRemove() && priority < minPriority) {
                    minPriority = priority;
                    minPriorityBuffer = buffer;
                    minPriorityEntityModelId = priorityEntry.getKey();
                }
            }
            if (minPriorityBuffer != null) {
                minPriorityBuffer.remove(minPriorityEntityModelId);
                long memoryReleased = minPriorityBuffer.getMemoryConsumptionPerEntity();
                memoryTracker.releaseMemory(memoryReleased, false, Origin.MULTI_ENTITY_DETECTOR);
                memoryToShed -= memoryReleased;
            } else {
                break;
            }
        }
    }

    /**
     * Recalculate memory consumption in case of bugs/race conditions when allocating/releasing memory
     */
    private void recalculateUsedMemory() {
        long reserved = 0;
        long shared = 0;
        for (Map.Entry<String, CacheBuffer> entry : activeEnities.entrySet()) {
            CacheBuffer buffer = entry.getValue();
            reserved += buffer.getReservedBytes();
            shared += buffer.getBytesInSharedCache();
        }
        memoryTracker.syncMemoryState(Origin.MULTI_ENTITY_DETECTOR, reserved + shared, reserved);
    }

    /**
     * Maintain active entity's cache and door keepers.
     *
     * inActiveEntities is a Guava's LRU cache. The data structure itself is
     * gonna evict items if they are inactive for 3 days or its maximum size
     * reached (1 million entries)
     */
    @Override
    public void maintenance() {
        try {
            // clean up memory if we allocate more memory than we should
            tryClearUpMemory();
            activeEnities.entrySet().stream().forEach(cacheBufferEntry -> {
                String detectorId = cacheBufferEntry.getKey();
                CacheBuffer cacheBuffer = cacheBufferEntry.getValue();
                // remove expired cache buffer
                if (cacheBuffer.expired(modelTtl)) {
                    activeEnities.remove(detectorId);
                    cacheBuffer.clear();
                } else {
                    cacheBuffer.maintenance();
                }
            });

            maintainInactiveCache();

            doorKeepers.entrySet().stream().forEach(doorKeeperEntry -> {
                String detectorId = doorKeeperEntry.getKey();
                DoorKeeper doorKeeper = doorKeeperEntry.getValue();
                if (doorKeeper.expired(modelTtl)) {
                    doorKeepers.remove(detectorId);
                } else {
                    doorKeeper.maintenance();
                }
            });
        } catch (Exception e) {
            // will be thrown to ES's transport broadcast handler
            throw new ElasticsearchException("Fail to maintain cache", e);
        }

    }

    /**
     * Permanently deletes models hosted in memory and persisted in index.
     *
     * @param detectorId id the of the detector for which models are to be permanently deleted
     */
    @Override
    public void clear(String detectorId) {
        if (detectorId == null) {
            return;
        }
        CacheBuffer buffer = activeEnities.remove(detectorId);
        if (buffer != null) {
            buffer.clear();
        }
        checkpointDao.deleteModelCheckpointByDetectorId(detectorId);
        doorKeepers.remove(detectorId);
    }

    /**
     * Get the number of active entities of a detector
     * @param detectorId Detector Id
     * @return The number of active entities
     */
    @Override
    public int getActiveEntities(String detectorId) {
        CacheBuffer cacheBuffer = activeEnities.get(detectorId);
        if (cacheBuffer != null) {
            return cacheBuffer.getActiveEntities();
        }
        return 0;
    }

    /**
     * Whether an entity is active or not
     * @param detectorId The Id of the detector that an entity belongs to
     * @param entityModelId Entity's Model Id
     * @return Whether an entity is active or not
     */
    @Override
    public boolean isActive(String detectorId, String entityModelId) {
        CacheBuffer cacheBuffer = activeEnities.get(detectorId);
        if (cacheBuffer != null) {
            return cacheBuffer.isActive(entityModelId);
        }
        return false;
    }

    @Override
    public long getTotalUpdates(String detectorId) {
        return Optional
            .of(activeEnities)
            .map(entities -> entities.get(detectorId))
            .map(buffer -> buffer.getPriorityTracker().getHighestPriorityEntityId())
            .map(entityModelIdOptional -> entityModelIdOptional.get())
            .map(entityModelId -> getTotalUpdates(detectorId, entityModelId))
            .orElse(0L);
    }

    @Override
    public long getTotalUpdates(String detectorId, String entityModelId) {
        CacheBuffer cacheBuffer = activeEnities.get(detectorId);
        if (cacheBuffer != null) {
            Optional<EntityModel> modelOptional = cacheBuffer.getModel(entityModelId);
            // TODO: make it work for shingles. samples.size() is not the real shingle
            long accumulatedShingles = modelOptional
                .map(model -> model.getRcf())
                .map(rcf -> rcf.getTotalUpdates())
                .orElseGet(
                    () -> modelOptional.map(model -> model.getSamples()).map(samples -> samples.size()).map(Long::valueOf).orElse(0L)
                );
            return accumulatedShingles;
        }
        return 0L;
    }

    /**
     *
     * @return total active entities in the cache
     */
    @Override
    public int getTotalActiveEntities() {
        AtomicInteger total = new AtomicInteger();
        activeEnities.values().stream().forEach(cacheBuffer -> { total.addAndGet(cacheBuffer.getActiveEntities()); });
        return total.get();
    }

    /**
     * Gets modelStates of all model hosted on a node
     *
     * @return list of modelStates
     */
    @Override
    public List<ModelState<?>> getAllModels() {
        List<ModelState<?>> states = new ArrayList<>();
        activeEnities.values().stream().forEach(cacheBuffer -> states.addAll(cacheBuffer.getAllModels()));
        return states;
    }

    /**
     * Gets all of a detector's model sizes hosted on a node
     *
     * @return a map of model id to its memory size
     */
    @Override
    public Map<String, Long> getModelSize(String detectorId) {
        CacheBuffer cacheBuffer = activeEnities.get(detectorId);
        Map<String, Long> res = new HashMap<>();
        if (cacheBuffer != null) {
            long size = cacheBuffer.getMemoryConsumptionPerEntity();
            cacheBuffer.getAllModels().forEach(entry -> res.put(entry.getModelId(), size));
        }
        return res;
    }

    @Override
    /**
     * Gets an entity's model state
     *
     * @param detectorId detector id
     * @param entityModelId  entity model id
     * @return the model state
     */
    public long getModelSize(String detectorId, String entityModelId) {
        CacheBuffer cacheBuffer = activeEnities.get(detectorId);
        if (cacheBuffer != null && cacheBuffer.getModel(entityModelId).isPresent()) {
            return cacheBuffer.getMemoryConsumptionPerEntity();
        }
        return -1L;
    }

    /**
     * Return the last active time of an entity's state.
     *
     * If the entity's state is active in the cache, the value indicates when the cache
     * is lastly accessed (get/put).  If the entity's state is inactive in the cache,
     * the value indicates when the cache state is created or when the entity is evicted
     * from active entity cache.
     *
     * @param detectorId The Id of the detector that an entity belongs to
     * @param entityModelId Entity's Model Id
     * @return if the entity is in the cache, return the timestamp in epoch
     * milliseconds when the entity's state is lastly used.  Otherwise, return -1.
     */
    @Override
    public long getLastActiveMs(String detectorId, String entityModelId) {
        CacheBuffer cacheBuffer = activeEnities.get(detectorId);
        if (cacheBuffer != null) {
            return cacheBuffer.getLastUsedTime(entityModelId);
        }
        ModelState<EntityModel> stateInActive = inActiveEntities.getIfPresent(entityModelId);
        if (stateInActive != null) {
            return stateInActive.getLastUsedTime().getEpochSecond();
        }
        return -1L;
    }

    @Override
    public void releaseMemoryForOpenCircuitBreaker() {
        maintainInactiveCache();

        tryClearUpMemory();
        activeEnities.values().stream().forEach(cacheBuffer -> {
            if (cacheBuffer.canRemove()) {
                cacheBuffer.remove();
            }
        });
    }

    private void maintainInactiveCache() {
        if (lastInActiveEntityMaintenance.plus(this.modelTtl).isAfter(clock.instant())) {
            // don't scan inactive cache too frequently as it is costly
            return;
        }

        // force maintenance of the cache. ref: https://tinyurl.com/pyy3p9v6
        inActiveEntities.cleanUp();

        // // make sure no model has been stored due to bugs
        for (ModelState<EntityModel> state : inActiveEntities.asMap().values()) {
            EntityModel model = state.getModel();
            if (model != null && (model.getRcf() != null || model.getThreshold() != null)) {
                LOG
                    .warn(
                        new ParameterizedMessage(
                            "Inactive entity's model is null: [{}], [{}]. Maybe there are bugs.",
                            model.getRcf(),
                            model.getThreshold()
                        )
                    );
                state.setModel(null);
            }
        }

        lastInActiveEntityMaintenance = clock.instant();
    }
}
