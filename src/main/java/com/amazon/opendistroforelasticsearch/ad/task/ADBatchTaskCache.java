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

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_MIN_SAMPLES;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_TREES;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.THRESHOLD_MODEL_TRAINING_SIZE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.TIME_DECAY;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.amazon.opendistroforelasticsearch.ad.ml.HybridThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.randomcutforest.RandomCutForest;

/**
 * AD batch task cache which will hold RCF, threshold model, shingle and training data.
 */
public class ADBatchTaskCache {
    private final String detectorId;
    private final String taskId;
    private RandomCutForest rcfModel;
    private ThresholdingModel thresholdModel;
    private boolean thresholdModelTrained;
    private Deque<Map.Entry<Long, Optional<double[]>>> shingle;
    private AtomicInteger thresholdModelTrainingDataSize = new AtomicInteger(0);
    private double[] thresholdModelTrainingData;
    private AtomicBoolean cancelled = new AtomicBoolean(false);
    private AtomicLong cacheMemorySize = new AtomicLong(0);
    private String cancelReason;
    private String cancelledBy;

    protected ADBatchTaskCache(ADTask adTask) {
        this.detectorId = adTask.getDetectorId();
        this.taskId = adTask.getTaskId();

        AnomalyDetector detector = adTask.getDetector();
        rcfModel = RandomCutForest
            .builder()
            .dimensions(detector.getShingleSize() * detector.getEnabledFeatureIds().size())
            .numberOfTrees(NUM_TREES)
            .lambda(TIME_DECAY)
            .sampleSize(NUM_SAMPLES_PER_TREE)
            .outputAfter(NUM_MIN_SAMPLES)
            .parallelExecutionEnabled(false)
            .build();

        this.thresholdModel = new HybridThresholdingModel(
            AnomalyDetectorSettings.THRESHOLD_MIN_PVALUE,
            AnomalyDetectorSettings.THRESHOLD_MAX_RANK_ERROR,
            AnomalyDetectorSettings.THRESHOLD_MAX_SCORE,
            AnomalyDetectorSettings.THRESHOLD_NUM_LOGNORMAL_QUANTILES,
            AnomalyDetectorSettings.THRESHOLD_DOWNSAMPLES,
            AnomalyDetectorSettings.THRESHOLD_MAX_SAMPLES
        );
        this.thresholdModelTrainingData = new double[THRESHOLD_MODEL_TRAINING_SIZE];
        this.thresholdModelTrained = false;
        this.shingle = new ArrayDeque<>(detector.getShingleSize());
    }

    protected String getDetectorId() {
        return detectorId;
    }

    protected String getTaskId() {
        return taskId;
    }

    protected RandomCutForest getRcfModel() {
        return rcfModel;
    }

    protected Deque<Map.Entry<Long, Optional<double[]>>> getShingle() {
        return shingle;
    }

    protected ThresholdingModel getThresholdModel() {
        return thresholdModel;
    }

    protected void setThresholdModelTrained(boolean thresholdModelTrained) {
        this.thresholdModelTrained = thresholdModelTrained;
    }

    protected boolean isThresholdModelTrained() {
        return thresholdModelTrained;
    }

    protected double[] getThresholdModelTrainingData() {
        return thresholdModelTrainingData;
    }

    protected void clearTrainingData() {
        this.thresholdModelTrainingData = null;
        this.thresholdModelTrainingDataSize.set(0);
    }

    public AtomicInteger getThresholdModelTrainingDataSize() {
        return thresholdModelTrainingDataSize;
    }

    protected AtomicLong getCacheMemorySize() {
        return cacheMemorySize;
    }

    protected boolean isCancelled() {
        return cancelled.get();
    }

    protected String getCancelReason() {
        return cancelReason;
    }

    protected String getCancelledBy() {
        return cancelledBy;
    }

    protected void cancel(String reason, String userName) {
        this.cancelled.compareAndSet(false, true);
        this.cancelReason = reason;
        this.cancelledBy = userName;
    }
}
