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

package com.amazon.opendistroforelasticsearch.ad.settings;

import java.time.Duration;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;

/**
 * AD plugin settings.
 */
public final class AnomalyDetectorSettings {

    private AnomalyDetectorSettings() {}

    public static final Setting<Integer> MAX_SINGLE_ENTITY_ANOMALY_DETECTORS = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_anomaly_detectors",
            1000,
            0,
            10_000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Integer> MAX_MULTI_ENTITY_ANOMALY_DETECTORS = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_multi_entity_anomaly_detectors",
            10,
            0,
            10_000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Integer> MAX_ANOMALY_FEATURES = Setting
        .intSetting("opendistro.anomaly_detection.max_anomaly_features", 5, 0, 100, Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<TimeValue> REQUEST_TIMEOUT = Setting
        .positiveTimeSetting(
            "opendistro.anomaly_detection.request_timeout",
            TimeValue.timeValueSeconds(10),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> DETECTION_INTERVAL = Setting
        .positiveTimeSetting(
            "opendistro.anomaly_detection.detection_interval",
            TimeValue.timeValueMinutes(10),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> DETECTION_WINDOW_DELAY = Setting
        .timeSetting(
            "opendistro.anomaly_detection.detection_window_delay",
            TimeValue.timeValueMinutes(0),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> AD_RESULT_HISTORY_ROLLOVER_PERIOD = Setting
        .positiveTimeSetting(
            "opendistro.anomaly_detection.ad_result_history_rollover_period",
            TimeValue.timeValueHours(12),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Long> AD_RESULT_HISTORY_MAX_DOCS = Setting
        .longSetting(
            "opendistro.anomaly_detection.ad_result_history_max_docs",
            // Total documents in primary replica.
            // A single result doc is roughly 46.8 bytes (measured by experiments).
            // 1.35 billion docs is about 65 GB. We choose 65 GB
            // because we have 1 shard at least. One shard can have at most 65 GB.
            1_350_000_000L,
            0L,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> AD_RESULT_HISTORY_RETENTION_PERIOD = Setting
        .positiveTimeSetting(
            "opendistro.anomaly_detection.ad_result_history_retention_period",
            TimeValue.timeValueDays(30),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Integer> MAX_RETRY_FOR_UNRESPONSIVE_NODE = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_retry_for_unresponsive_node",
            5,
            0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> COOLDOWN_MINUTES = Setting
        .positiveTimeSetting(
            "opendistro.anomaly_detection.cooldown_minutes",
            TimeValue.timeValueMinutes(5),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> BACKOFF_MINUTES = Setting
        .positiveTimeSetting(
            "opendistro.anomaly_detection.backoff_minutes",
            TimeValue.timeValueMinutes(15),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> BACKOFF_INITIAL_DELAY = Setting
        .positiveTimeSetting(
            "opendistro.anomaly_detection.backoff_initial_delay",
            TimeValue.timeValueMillis(1000),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Integer> MAX_RETRY_FOR_BACKOFF = Setting
        .intSetting("opendistro.anomaly_detection.max_retry_for_backoff", 3, 0, Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<Integer> MAX_RETRY_FOR_END_RUN_EXCEPTION = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_retry_for_end_run_exception",
            6,
            0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Boolean> FILTER_BY_BACKEND_ROLES = Setting
        .boolSetting("opendistro.anomaly_detection.filter_by_backend_roles", false, Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final String ANOMALY_DETECTORS_INDEX_MAPPING_FILE = "mappings/anomaly-detectors.json";
    public static final String ANOMALY_DETECTOR_JOBS_INDEX_MAPPING_FILE = "mappings/anomaly-detector-jobs.json";
    public static final String ANOMALY_RESULTS_INDEX_MAPPING_FILE = "mappings/anomaly-results.json";
    public static final String ANOMALY_DETECTION_STATE_INDEX_MAPPING_FILE = "mappings/anomaly-detection-state.json";
    public static final String CHECKPOINT_INDEX_MAPPING_FILE = "mappings/checkpoint.json";

    public static final Duration HOURLY_MAINTENANCE = Duration.ofHours(1);

    public static final Duration CHECKPOINT_TTL = Duration.ofDays(3);

    // ======================================
    // ML parameters
    // ======================================
    // RCF
    public static final int NUM_SAMPLES_PER_TREE = 256;

    public static final int NUM_TREES = 100;

    public static final int TRAINING_SAMPLE_INTERVAL = 64;

    public static final double TIME_DECAY = 0.0001;

    public static final int NUM_MIN_SAMPLES = 128;

    public static final double DESIRED_MODEL_SIZE_PERCENTAGE = 0.0002;

    public static final Setting<Double> MODEL_MAX_SIZE_PERCENTAGE = Setting
        .doubleSetting(
            "opendistro.anomaly_detection.model_max_size_percent",
            0.1,
            0,
            0.7,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // Thresholding
    public static final double THRESHOLD_MIN_PVALUE = 0.995;

    public static final double THRESHOLD_MAX_RANK_ERROR = 0.0001;

    public static final double THRESHOLD_MAX_SCORE = 8;

    public static final int THRESHOLD_NUM_LOGNORMAL_QUANTILES = 400;

    public static final int THRESHOLD_DOWNSAMPLES = 5_000;

    public static final long THRESHOLD_MAX_SAMPLES = 50_000;

    public static final int MIN_PREVIEW_SIZE = 400; // ok to lower

    // Feature processing
    public static final int MAX_TRAIN_SAMPLE = 24;

    public static final int MAX_SAMPLE_STRIDE = 64;

    public static final int TRAIN_SAMPLE_TIME_RANGE_IN_HOURS = 24;

    public static final int MIN_TRAIN_SAMPLES = 512;

    public static final int DEFAULT_SHINGLE_SIZE = 8;

    public static final int MAX_IMPUTATION_NEIGHBOR_DISTANCE = 2;

    public static final double MAX_SHINGLE_PROPORTION_MISSING = 0.25;

    public static final double PREVIEW_SAMPLE_RATE = 0.25; // ok to adjust, higher for more data, lower for lower latency

    public static final int MAX_PREVIEW_SAMPLES = 300; // ok to adjust, higher for more data, lower for lower latency

    public static final int MAX_PREVIEW_RESULTS = 1_000; // ok to adjust, higher for more data, lower for lower latency

    // AD JOB
    public static final long DEFAULT_AD_JOB_LOC_DURATION_SECONDS = 60;

    // Thread pool
    public static final int AD_THEAD_POOL_QUEUE_SIZE = 1000;

    // ======================================
    // HCAD caching parameters
    // ======================================
    // multi-entity caching
    public static final int MAX_ACTIVE_STATES = 1000;

    // the size of the cache for small states like last cold start time for an entity.
    // At most, we have 10 multi-entity detector and each one can be hit by 1000 different entities each
    // minute. Since these states' life time is hour, we keep its size 10 * 1000 = 10000.
    public static final int MAX_SMALL_STATES = 10000;

    // Multi-entity detector model setting:
    // TODO (kaituo): change to 4
    public static final int DEFAULT_MULTI_ENTITY_SHINGLE = 1;

    // how many categorical fields we support
    public static final int CATEGORY_FIELD_LIMIT = 1;

    public static final int MULTI_ENTITY_NUM_TREES = 10;

    // cache related
    public static final Setting<Integer> DEDICATED_CACHE_SIZE = Setting
        .intSetting(
            "opendistro.anomaly_detection.dedicated_cache_size",
            10,
            1,
            100_000_000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // We only keep priority (4 bytes float) in inactive cache. 100k priorities
    // take up 400KB.
    public static final int MAX_INACTIVE_ENTITIES = 100_000;

    // 1 million insertion costs roughly 1 MB.
    public static final int DOOR_KEEPER_FOR_CACHE_MAX_INSERTION = 1_000_000;

    // 100,000 insertions costs roughly 1KB.
    public static final int DOOR_KEEPER_FOR_COLD_STARTER_MAX_INSERTION = 100_000;

    // public static final int DOOR_KEEPER_MAX_INSERTION = 1_000_000;

    public static final double DOOR_KEEPER_FAULSE_POSITIVE_RATE = 0.01;

    // Increase the value will adding pressure to indexing anomaly results and our feature query
    public static final Setting<Integer> MAX_ENTITIES_PER_QUERY = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_entities_per_query",
            1000,
            1,
            100_000_000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // Default number of entities retrieved for Preview API
    public static final int DEFAULT_ENTITIES_FOR_PREVIEW = 30;

    // Maximum number of entities retrieved for Preview API
    public static final Setting<Integer> MAX_ENTITIES_FOR_PREVIEW = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_entities_for_preview",
            DEFAULT_ENTITIES_FOR_PREVIEW,
            1,
            1000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // save partial zero-anomaly grade results after indexing pressure reaching the limit
    public static final Setting<Float> INDEX_PRESSURE_SOFT_LIMIT = Setting
        .floatSetting(
            "opendistro.anomaly_detection.index_pressure_soft_limit",
            0.6f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Float> INDEX_PRESSURE_HARD_LIMIT = Setting
        .floatSetting(
            "opendistro.anomaly_detection.index_pressure_hard_limit",
            0.9f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // max number of primary shards of an AD index
    public static final Setting<Integer> MAX_PRIMARY_SHARDS = Setting
        .intSetting("opendistro.anomaly_detection.max_primary_shards", 10, 0, 200, Setting.Property.NodeScope, Setting.Property.Dynamic);

    // max entity value's length
    public static int MAX_ENTITY_LENGTH = 256;

    // ======================================
    // historical detector parameters
    // ======================================
    // Maximum number of batch tasks running on one node.
    // TODO: performance test and tune the setting.
    public static final Setting<Integer> MAX_BATCH_TASK_PER_NODE = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_batch_task_per_node",
            2,
            1,
            100,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static int THRESHOLD_MODEL_TRAINING_SIZE = 128; // 128 data points rcf with shingle size 8

    public static int MAX_OLD_AD_TASK_DOCS = 1000;
    public static final Setting<Integer> MAX_OLD_AD_TASK_DOCS_PER_DETECTOR = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_old_ad_task_docs_per_detector",
            // One AD task is roughly 1.5KB for normal case. Suppose task's size
            // is 2KB conservatively. If we store 1000 AD tasks for one detector,
            // that will be 2GB.
            1,
            0, // keep at least 1 old AD task per detector
            MAX_OLD_AD_TASK_DOCS,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final int MAX_BATCH_TASK_PIECE_SIZE = 10_000;
    public static final Setting<Integer> BATCH_TASK_PIECE_SIZE = Setting
        .intSetting(
            "opendistro.anomaly_detection.batch_task_piece_size",
            1000,
            1,
            MAX_BATCH_TASK_PIECE_SIZE,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Integer> BATCH_TASK_PIECE_INTERVAL_SECONDS = Setting
        .intSetting(
            "opendistro.anomaly_detection.batch_task_piece_interval_seconds",
            1,
            1,
            600,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Integer> MAX_TOP_ENTITIES_PER_HC_DETECTOR = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_top_entities_per_hc_detector",
            50,
            1,
            10000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Integer> MAX_RUNNING_ENTITIES_PER_DETECTOR = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_running_entities_per_detector",
            2,
            1,
            1000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // ======================================
    // rate-limiting queue parameters
    // ======================================
    // the percentage of heap usage allowed for queues holding small requests
    public static final Setting<Float> SMALL_REQUEST_QUEUE_MAX_HEAP_PERCENT = Setting
        .floatSetting(
            "opendistro.anomaly_detection.small_request_queue_max_heap_percent",
            0.001f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // the percentage of heap usage allowed for queues holding large requests
    public static final Setting<Float> BIG_REQUEST_QUEUE_MAX_HEAP_PERCENT = Setting
        .floatSetting(
            "opendistro.anomaly_detection.big_request_queue_max_heap_percent",
            0.01f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // expected execution time per cold entity request. This setting controls
    // the speed of cold entity requests execution. The larger, the faster, and
    // the more performance impact to customers' workload.
    public static final Setting<Integer> EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_SECS = Setting
        .intSetting(
            "opendistro.anomaly_detection.expected_cold_entity_execution_time_in_secs",
            3,
            0,
            3600,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    /**
     * EntityRequest has entityName (# category fields * 256, the recommended limit
     * of a keyword field length), model Id (roughly 256 bytes), and QueuedRequest
     * fields including detector Id(roughly 128 bytes), expirationEpochMs (long,
     *  8 bytes), and priority (12 bytes).
     * Plus Java object size (12 bytes), we have roughly 928 bytes per request
     * assuming we have 2 categorical fields (plan to support 2 categorical fields now).
     * We don't want the total size exceeds 0.1% of the heap.
     * We can have at most 0.1% heap / 928 = heap / 928,000.
     * For t3.small, 0.1% heap is of 1MB. The queue's size is up to
     * 10^ 6 / 928 = 1078
     */
    public static int ENTITY_REQUEST_SIZE_IN_BYTES = 928;

    /**
     * EntityFeatureRequest consists of EntityRequest (928 bytes, read comments
     * of ENTITY_COLD_START_QUEUE_SIZE_CONSTANT), pointer to current feature
     * (8 bytes), and dataStartTimeMillis (8 bytes).  We have roughly
     * 928 + 16 = 944 bytes per request.
     *
     * We don't want the total size exceeds 0.1% of the heap.
     * We should have at most 0.1% heap / 944 = heap / 944,000
     * For t3.small, 0.1% heap is of 1MB. The queue's size is up to
     * 10^ 6 / 944 = 1059
     */
    public static int ENTITY_FEATURE_REQUEST_SIZE_IN_BYTES = 944;

    /**
     * ResultWriteRequest consists of index request (roughly 1KB), and QueuedRequest
     * fields (148 bytes, read comments of ENTITY_REQUEST_SIZE_CONSTANT).
     * Plus Java object size (12 bytes), we have roughly 1160 bytes per request
     *
     * We don't want the total size exceeds 1% of the heap.
     * We should have at most 1% heap / 1148 = heap / 116,000
     * For t3.small, 1% heap is of 10MB. The queue's size is up to
     * 10^ 7 / 1160 = 8621
     */
    public static int RESULT_WRITE_QUEUE_SIZE_IN_BYTES = 1160;

    /**
     * CheckpointWriteRequest consists of IndexRequest (200 KB), and QueuedRequest
     * fields (148 bytes, read comments of ENTITY_REQUEST_SIZE_CONSTANT).
     * The total is roughly 200 KB per request.
     *
     * We don't want the total size exceeds 1% of the heap.
     * We should have at most 1% heap / 200KB = heap / 20,000,000
     * For t3.small, 1% heap is of 10MB. The queue's size is up to
     * 10^ 7 / 2.0 * 10^5 = 50
     */
    public static int CHECKPOINT_WRITE_QUEUE_SIZE_IN_BYTES = 200_000;

    /**
     * Max concurrent entity cold starts per node
     */
    public static final Setting<Integer> ENTITY_COLDSTART_QUEUE_CONCURRENCY = Setting
        .intSetting(
            "opendistro.anomaly_detection.entity_coldstart_queue_concurrency",
            1,
            1,
            10,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    /**
     * Max concurrent checkpoint reads per node
     */
    public static final Setting<Integer> CHECKPOINT_READ_QUEUE_CONCURRENCY = Setting
        .intSetting(
            "opendistro.anomaly_detection.checkpoint_read_queue_concurrency",
            1,
            1,
            10,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    /**
     * Max concurrent checkpoint writes per node
     */
    public static final Setting<Integer> CHECKPOINT_WRITE_QUEUE_CONCURRENCY = Setting
        .intSetting(
            "opendistro.anomaly_detection.checkpoint_write_queue_concurrency",
            2,
            1,
            10,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    /**
     * Max concurrent result writes per node.  Since checkpoint is relatively large
     * (250KB), we have 2 concurrent threads processing the queue.
     */
    public static final Setting<Integer> RESULT_WRITE_QUEUE_CONCURRENCY = Setting
        .intSetting(
            "opendistro.anomaly_detection.result_write_queue_concurrency",
            2,
            1,
            10,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    /**
     * Max concurrent cold entity processing per node
     */
    public static final Setting<Integer> COLD_ENTITY_QUEUE_CONCURRENCY = Setting
        .intSetting(
            "opendistro.anomaly_detection.cold_entity_queue_concurrency",
            1,
            1,
            10,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    /**
     * Assume each checkpoint takes roughly 200KB.  25 requests are of 5 MB.
     */
    public static final Setting<Integer> CHECKPOINT_READ_QUEUE_BATCH_SIZE = Setting
        .intSetting(
            "opendistro.anomaly_detection.checkpoint_read_queue_batch_size",
            25,
            1,
            60,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    /**
     * ES recommends bulk size to be 5~15 MB.
     * ref: https://tinyurl.com/3zdbmbwy
     * Assume each checkpoint takes roughly 200KB.  25 requests are of 5 MB.
     */
    public static final Setting<Integer> CHECKPOINT_WRITE_QUEUE_BATCH_SIZE = Setting
        .intSetting(
            "opendistro.anomaly_detection.checkpoint_write_queue_batch_size",
            25,
            1,
            60,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    /**
     * ES recommends bulk size to be 5~15 MB.
     * ref: https://tinyurl.com/3zdbmbwy
     * Assume each result takes roughly 1KB.  5000 requests are of 5 MB.
     */
    public static final Setting<Integer> RESULT_WRITE_QUEUE_BATCH_SIZE = Setting
        .intSetting(
            "opendistro.anomaly_detection.result_write_queue_batch_size",
            5000,
            1,
            15000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Duration QUEUE_MAINTENANCE = Duration.ofMinutes(5);

    // we won't accept a checkpoint larger than 10MB. Or we risk OOM.
    public static final int MAX_CHECKPOINT_BYTES = 10_000_000;

    public static final float MAX_QUEUED_TASKS_RATIO = 0.5f;

    public static final float MEDIUM_SEGMENT_PRUNE_RATIO = 0.1f;

    public static final float LOW_SEGMENT_PRUNE_RATIO = 0.3f;

    // maintain queues with 1/100 probability
    public static final int QUEUE_MAINTENANCE_FREQ_CONSTANT = 100;
}
