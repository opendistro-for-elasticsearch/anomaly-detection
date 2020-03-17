/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
 * TODO: add more settings once detail design done.
 */
public final class AnomalyDetectorSettings {

    private AnomalyDetectorSettings() {}

    public static final Setting<Integer> MAX_ANOMALY_DETECTORS = Setting
        .intSetting("opendistro.anomaly_detection.max_anomaly_detectors", 1000, Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<Integer> MAX_ANOMALY_FEATURES = Setting
        .intSetting("opendistro.anomaly_detection.max_anomaly_features", 5, Setting.Property.NodeScope, Setting.Property.Dynamic);

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

    public static final Setting<TimeValue> AD_RESULT_ROLLOVER_PERIOD = Setting
        .positiveTimeSetting(
            "opendistro.anomaly_detection.ad_result_rollover_period",
            TimeValue.timeValueHours(12),
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

    public static final Setting<TimeValue> AD_RESULT_HISTORY_INDEX_MAX_AGE = Setting
        .positiveTimeSetting(
            "opendistro.anomaly_detection.ad_result_history_max_age",
            TimeValue.timeValueHours(24),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Long> AD_RESULT_HISTORY_MAX_DOCS = Setting
        .longSetting(
            "opendistro.anomaly_detection.ad_result_history_max_docs",
            10000L,
            0L,
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
            3,
            0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final String ANOMALY_DETECTORS_INDEX_MAPPING_FILE = "mappings/anomaly-detectors.json";
    public static final String ANOMALY_DETECTOR_JOBS_INDEX_MAPPING_FILE = "mappings/anomaly-detector-jobs.json";
    public static final String ANOMALY_RESULTS_INDEX_MAPPING_FILE = "mappings/anomaly-results.json";

    public static final Duration HOURLY_MAINTENANCE = Duration.ofHours(1);

    public static final Duration CHECKPOINT_TTL = Duration.ofDays(14);

    // ======================================
    // ML parameters
    // ======================================
    // RCF
    public static final int NUM_SAMPLES_PER_TREE = 256;

    public static final int NUM_TREES = 100;

    public static final int TRAINING_SAMPLE_INTERVAL = 64;

    public static final double TIME_DECAY = 0.0001;

    public static final double DESIRED_MODEL_SIZE_PERCENTAGE = 0.0002;

    public static final double MODEL_MAX_SIZE_PERCENTAGE = 0.1;

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

    public static final int SHINGLE_SIZE = 8;

    public static final int MAX_MISSING_POINTS = Math.min(2, SHINGLE_SIZE);

    public static final int MAX_NEIGHBOR_DISTANCE = Math.min(2, SHINGLE_SIZE);

    public static final double PREVIEW_SAMPLE_RATE = 0.25; // ok to adjust, higher for more data, lower for lower latency

    public static final int MAX_PREVIEW_SAMPLES = 300; // ok to adjust, higher for more data, lower for lower latency

    public static final int MAX_PREVIEW_RESULTS = 1_000; // ok to adjust, higher for more data, lower for lower latency

    // AD JOB
    public static final long DEFAULT_AD_JOB_LOC_DURATION_SECONDS = 60;

    // Thread pool
    public static final int AD_THEAD_POOL_QUEUE_SIZE = 1000;
}
