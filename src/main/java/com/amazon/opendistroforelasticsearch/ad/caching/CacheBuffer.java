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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazon.opendistroforelasticsearch.ad.ExpiringState;
import com.amazon.opendistroforelasticsearch.ad.MaintenanceState;
import com.amazon.opendistroforelasticsearch.ad.MemoryTracker;
import com.amazon.opendistroforelasticsearch.ad.MemoryTracker.Origin;
import com.amazon.opendistroforelasticsearch.ad.annotation.Generated;
import com.amazon.opendistroforelasticsearch.ad.ml.CheckpointDao;
import com.amazon.opendistroforelasticsearch.ad.ml.EntityModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelState;
import com.amazon.opendistroforelasticsearch.ad.model.InitProgressProfile;

/**
 * We use a layered cache to manage active entities’ states.  We have a two-level
 * cache that stores active entity states in each node.  Each detector has its
 * dedicated cache that stores ten (dynamically adjustable) entities’ states per
 * node.  A detector’s hottest entities load their states in the dedicated cache.
 * If less than 10 entities use the dedicated cache, the secondary cache can use
 * the rest of the free memory available to AD.  The secondary cache is a shared
 * memory among all detectors for the long tail.  The shared cache size is 10%
 * heap minus all of the dedicated cache consumed by single-entity and multi-entity
 * detectors.  The shared cache’s size shrinks as the dedicated cache is filled
 * up or more detectors are started.
 */
public class CacheBuffer implements ExpiringState, MaintenanceState {
    private static final Logger LOG = LogManager.getLogger(CacheBuffer.class);

    static class PriorityNode {
        private String key;
        private float priority;

        PriorityNode(String key, float priority) {
            this.priority = priority;
            this.key = key;
        }

        @Generated
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            if (obj instanceof PriorityNode) {
                PriorityNode other = (PriorityNode) obj;

                EqualsBuilder equalsBuilder = new EqualsBuilder();
                equalsBuilder.append(key, other.key);
                return equalsBuilder.isEquals();
            }
            return false;
        }

        @Generated
        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(key).toHashCode();
        }

        @Generated
        @Override
        public String toString() {
            ToStringBuilder builder = new ToStringBuilder(this);
            builder.append("key", key);
            builder.append("priority", priority);
            return builder.toString();
        }
    }

    static class PriorityNodeComparator implements Comparator<PriorityNode> {

        @Override
        public int compare(PriorityNode priority, PriorityNode priority2) {
            int cmp = Float.compare(priority.priority, priority2.priority);
            if (cmp == 0) {
                cmp = priority.key.compareTo(priority2.key);
            }
            return cmp;
        }
    }

    private final int minimumCapacity;
    // key -> Priority node
    private final ConcurrentHashMap<String, PriorityNode> key2Priority;
    private final ConcurrentSkipListSet<PriorityNode> priorityList;
    // key -> value
    private final ConcurrentHashMap<String, ModelState<EntityModel>> items;
    // when detector is created.  Can be reset.  Unit: seconds
    private long landmarkSecs;
    // length of seconds in one interval.  Used to compute elapsed periods
    // since the detector has been enabled.
    private long intervalSecs;
    // memory consumption per entity
    private final long memoryConsumptionPerEntity;
    private final MemoryTracker memoryTracker;
    private final Clock clock;
    private final CheckpointDao checkpointDao;
    private final Duration modelTtl;
    private final String detectorId;
    private Instant lastUsedTime;
    private final int decayConstant;
    private final long reservedBytes;

    public CacheBuffer(
        int minimumCapacity,
        long intervalSecs,
        CheckpointDao checkpointDao,
        long memoryConsumptionPerEntity,
        MemoryTracker memoryTracker,
        Clock clock,
        Duration modelTtl,
        String detectorId
    ) {
        if (minimumCapacity <= 0) {
            throw new IllegalArgumentException("minimum capacity should be larger than 0");
        }
        this.minimumCapacity = minimumCapacity;
        this.key2Priority = new ConcurrentHashMap<>();
        this.priorityList = new ConcurrentSkipListSet<>(new PriorityNodeComparator());
        this.items = new ConcurrentHashMap<>();
        this.landmarkSecs = clock.instant().getEpochSecond();
        this.intervalSecs = intervalSecs;
        this.memoryConsumptionPerEntity = memoryConsumptionPerEntity;
        this.memoryTracker = memoryTracker;
        this.clock = clock;
        this.checkpointDao = checkpointDao;
        this.modelTtl = modelTtl;
        this.detectorId = detectorId;
        this.lastUsedTime = clock.instant();
        this.decayConstant = 3;
        this.reservedBytes = memoryConsumptionPerEntity * minimumCapacity;
    }

    /**
     * Update step at period t_k:
     * new priority = old priority + log(1+e^{\log(g(t_k-L))-old priority}) where g(n) = e^{0.125n},
     * and n is the period.
     * @param entityId model Id
     */
    private void update(String entityId) {
        PriorityNode node = key2Priority.computeIfAbsent(entityId, k -> new PriorityNode(entityId, 0f));
        // reposition this node
        this.priorityList.remove(node);
        node.priority = getUpdatedPriority(node.priority);
        this.priorityList.add(node);

        Instant now = clock.instant();
        items.get(entityId).setLastUsedTime(now);
        lastUsedTime = now;
    }

    public float getUpdatedPriority(float oldPriority) {
        long increment = computeWeightedCountIncrement();
        // if overflowed, we take the short cut from now on
        oldPriority += Math.log(1 + Math.exp(increment - oldPriority));
        // if overflow happens, using \log(g(t_k-L)) instead.
        if (oldPriority == Float.POSITIVE_INFINITY) {
            oldPriority = increment;
        }
        return oldPriority;
    }

    /**
     * Compute periods relative to landmark and the weighted count increment using 0.125n.
     * Multiply by 0.125 is implemented using right shift for efficiency.
     * @return the weighted count increment used in the priority update step.
     */
    private long computeWeightedCountIncrement() {
        long periods = (clock.instant().getEpochSecond() - landmarkSecs) / intervalSecs;
        return periods >> decayConstant;
    }

    /**
     * Compute the weighted total count by considering landmark
     * \log(C)=\log(\sum_{i=1}^{n} (g(t_i-L)/g(t-L)))=\log(\sum_{i=1}^{n} (g(t_i-L))-\log(g(t-L))
     * @return the minimum priority entity's ID and priority
     */
    public Entry<String, Float> getMinimumPriority() {
        PriorityNode smallest = priorityList.first();
        long periods = (clock.instant().getEpochSecond() - landmarkSecs) / intervalSecs;
        float detectorWeight = periods >> decayConstant;
        return new SimpleImmutableEntry<>(smallest.key, smallest.priority - detectorWeight);
    }

    /**
     * Insert the model state associated with a model Id to the cache
     * @param entityId the model Id
     * @param value the ModelState
     */
    public void put(String entityId, ModelState<EntityModel> value) {
        // race conditions can happen between the put and one of the following operations:
        // remove: not a problem as it is unlikely we are removing and putting the same thing
        // maintenance: not a problem as we are unlikely to maintain an entry that's not
        // already in the cache
        // clear: not a problem as we are releasing memory in MemoryTracker.
        // The newly added one loses references and soon GC will collect it.
        // We have memory tracking correction to fix incorrect memory usage record.
        // put from other threads: not a problem as the entry is associated with
        // entityId and our put is idempotent
        put(entityId, value, value.getPriority());
    }

    /**
    * Insert the model state associated with a model Id to the cache.  Update priority.
    * @param entityId the model Id
    * @param value the ModelState
    * @param priority the priority
    */
    private void put(String entityId, ModelState<EntityModel> value, float priority) {
        ModelState<EntityModel> contentNode = items.get(entityId);
        if (contentNode == null) {
            PriorityNode node = new PriorityNode(entityId, priority);
            key2Priority.put(entityId, node);
            priorityList.add(node);
            items.put(entityId, value);
            Instant now = clock.instant();
            value.setLastUsedTime(now);
            lastUsedTime = now;
            // shared cache empty means we are consuming reserved cache.
            // Since we have already considered them while allocating CacheBuffer,
            // skip bookkeeping.
            if (!sharedCacheEmpty()) {
                memoryTracker.consumeMemory(memoryConsumptionPerEntity, false, Origin.MULTI_ENTITY_DETECTOR);
            }
        } else {
            update(entityId);
            items.put(entityId, value);
        }
    }

    /**
     * Retrieve the ModelState associated with the model Id or null if the CacheBuffer
     * contains no mapping for the model Id
     * @param key the model Id
     * @return the Model state to which the specified model Id is mapped, or null
     * if this CacheBuffer contains no mapping for the model Id
     */
    public ModelState<EntityModel> get(String key) {
        // We can get an item that is to be removed soon due to race condition.
        // This is acceptable as it won't cause any corruption and exception.
        // And this item is used for scoring one last time.
        ModelState<EntityModel> node = items.get(key);
        if (node == null) {
            return null;
        }
        update(key);
        return node;
    }

    /**
     *
     * @return whether there is one item that can be removed from shared cache
     */
    public boolean canRemove() {
        return !items.isEmpty() && items.size() > minimumCapacity;
    }

    /**
     * remove the smallest priority item.
     */
    public void remove() {
        // race conditions can happen between the put and one of the following operations:
        // remove from other threads: not a problem. If they remove the same item,
        // our method is idempotent. If they remove two different items,
        // they don't impact each other.
        // maintenance: not a problem as all of the data structures are concurrent.
        // Two threads removing the same entry is not a problem.
        // clear: not a problem as we are releasing memory in MemoryTracker.
        // The removed one loses references and soon GC will collect it.
        // We have memory tracking correction to fix incorrect memory usage record.
        // put: not a problem as it is unlikely we are removing and putting the same thing
        PriorityNode smallest = priorityList.pollFirst();
        if (smallest != null) {
            remove(smallest.key);
        }
    }

    /**
     * Remove everything associated with the key and make a checkpoint.
     *
     * @param keyToRemove The key to remove
     * @return the associated ModelState associated with the key, or null if there
     * is no associated ModelState for the key
     */
    public ModelState<EntityModel> remove(String keyToRemove) {
        // if shared cache is empty, we are using reserved memory
        boolean reserved = sharedCacheEmpty();

        key2Priority.remove(keyToRemove);
        ModelState<EntityModel> valueRemoved = items.remove(keyToRemove);

        if (valueRemoved != null) {
            // if we releasing a shared cache item, release memory as well.
            if (!reserved) {
                memoryTracker.releaseMemory(memoryConsumptionPerEntity, false, Origin.MULTI_ENTITY_DETECTOR);
            }
            checkpointDao.write(valueRemoved, keyToRemove);
        }

        return valueRemoved;
    }

    /**
     * @return whether dedicated cache is available or not
     */
    public boolean dedicatedCacheAvailable() {
        return items.size() < minimumCapacity;
    }

    /**
     * @return whether shared cache is empty or not
     */
    public boolean sharedCacheEmpty() {
        return items.size() <= minimumCapacity;
    }

    /**
     *
     * @return the estimated number of bytes per entity state
     */
    public long getMemoryConsumptionPerEntity() {
        return memoryConsumptionPerEntity;
    }

    /**
     *
     * If the cache is not full, check if some other items can replace internal entities.
     * @param priority another entity's priority
     * @return whether one entity can be replaced by another entity with a certain priority
     */
    public boolean canReplace(float priority) {
        if (items.isEmpty()) {
            return false;
        }
        Entry<String, Float> minPriorityItem = getMinimumPriority();
        return minPriorityItem != null && priority > minPriorityItem.getValue();
    }

    /**
     * Replace the smallest priority entity with the input entity
     * @param entityId the Model Id
     * @param value the model State
     */
    public void replace(String entityId, ModelState<EntityModel> value) {
        remove();
        put(entityId, value);
    }

    @Override
    public void maintenance() {
        items.entrySet().stream().forEach(entry -> {
            String entityId = entry.getKey();
            try {
                ModelState<EntityModel> modelState = entry.getValue();
                Instant now = clock.instant();

                // we can have ConcurrentModificationException when serializing
                // and updating rcf model at the same time. To prevent this,
                // we need to have a deep copy of models or have a lock. Both
                // options are costly.
                // As we are gonna retry serializing either when the entity is
                // evicted out of cache or during the next maintenance period,
                // don't do anything when the exception happens.
                checkpointDao.write(modelState, entityId);

                if (modelState.getLastUsedTime().plus(modelTtl).isBefore(now)) {
                    // race conditions can happen between the put and one of the following operations:
                    // remove: not a problem as all of the data structures are concurrent.
                    // Two threads removing the same entry is not a problem.
                    // clear: not a problem as we are releasing memory in MemoryTracker.
                    // The removed one loses references and soon GC will collect it.
                    // We have memory tracking correction to fix incorrect memory usage record.
                    // put: not a problem as we are unlikely to maintain an entry that's not
                    // already in the cache
                    priorityList.remove(new PriorityNode(entityId, modelState.getPriority()));
                    remove(entityId);
                }
            } catch (Exception e) {
                LOG.warn("Failed to finish maintenance for model id " + entityId, e);
            }
        });
    }

    /**
     *
     * @return the number of active entities
     */
    public int getActiveEntities() {
        return items.size();
    }

    /**
     *
     * @param entityId Model Id
     * @return Whether the model is active or not
     */
    public boolean isActive(String entityId) {
        return items.containsKey(entityId);
    }

    /**
     *
     * @return Get the model of highest priority entity
     */
    public Optional<String> getHighestPriorityEntityId() {
        return Optional.of(priorityList).map(list -> list.last()).map(node -> node.key);
    }

    /**
     *
     * @param entityId entity Id
     * @return Get the model of an entity
     */
    public Optional<EntityModel> getModel(String entityId) {
        return Optional.of(items).map(map -> map.get(entityId)).map(state -> state.getModel());
    }

    /**
     * Clear associated memory.  Used when we are removing an detector.
     */
    public void clear() {
        // race conditions can happen between the put and remove/maintenance/put:
        // not a problem as we are releasing memory in MemoryTracker.
        // The newly added one loses references and soon GC will collect it.
        // We have memory tracking correction to fix incorrect memory usage record.

        memoryTracker.releaseMemory(getReservedBytes(), true, Origin.MULTI_ENTITY_DETECTOR);
        if (!sharedCacheEmpty()) {
            memoryTracker.releaseMemory(getBytesInSharedCache(), false, Origin.MULTI_ENTITY_DETECTOR);
        }
    }

    /**
     *
     * @return reserved bytes by the CacheBuffer
     */
    public long getReservedBytes() {
        return reservedBytes;
    }

    /**
     *
     * @return bytes consumed in the shared cache by the CacheBuffer
     */
    public long getBytesInSharedCache() {
        int sharedCacheEntries = items.size() - minimumCapacity;
        if (sharedCacheEntries > 0) {
            return memoryConsumptionPerEntity * sharedCacheEntries;
        }
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        if (obj instanceof InitProgressProfile) {
            CacheBuffer other = (CacheBuffer) obj;

            EqualsBuilder equalsBuilder = new EqualsBuilder();
            equalsBuilder.append(detectorId, other.detectorId);

            return equalsBuilder.isEquals();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(detectorId).toHashCode();
    }

    @Override
    public boolean expired(Duration stateTtl) {
        return expired(lastUsedTime, stateTtl, clock.instant());
    }

    public String getDetectorId() {
        return detectorId;
    }
}
