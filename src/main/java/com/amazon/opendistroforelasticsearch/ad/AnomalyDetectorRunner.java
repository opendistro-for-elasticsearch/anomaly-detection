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

package com.amazon.opendistroforelasticsearch.ad;

import com.amazon.opendistroforelasticsearch.ad.feature.FeatureManager;
import com.amazon.opendistroforelasticsearch.ad.feature.Features;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingResult;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.Feature;
import com.amazon.opendistroforelasticsearch.ad.model.FeatureData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Runner to trigger an anomaly detector.
 */
public final class AnomalyDetectorRunner {

    private final Logger logger = LogManager.getLogger(AnomalyDetectorRunner.class);
    private final ModelManager modelManager;
    private final FeatureManager featureManager;
    private final int maxPreviewResults;

    public AnomalyDetectorRunner(ModelManager modelManager, FeatureManager featureManager, int maxPreviewResults) {
        this.modelManager = modelManager;
        this.featureManager = featureManager;
        this.maxPreviewResults = maxPreviewResults;
    }

    /**
     * run anomaly detector and return anomaly result.
     *
     * @param detector  anomaly detector instance
     * @param startTime detection period start time
     * @param endTime   detection period end time
     * @param listener handle anomaly result
     * @throws IOException - if a user gives wrong query input when defining a detector
     */
    public void executeDetector(AnomalyDetector detector, Instant startTime, Instant endTime, ActionListener<List<AnomalyResult>> listener)
        throws IOException {
        featureManager.getPreviewFeatures(detector, startTime.toEpochMilli(), endTime.toEpochMilli(), ActionListener.wrap(features -> {
            try {
                List<ThresholdingResult> results = modelManager.getPreviewResults(features.getProcessedFeatures());
                listener.onResponse(sample(parsePreviewResult(detector, features, results), maxPreviewResults));
            } catch (Exception e) {
                onFailure(e, listener, detector.getDetectorId());
            }
        }, e -> onFailure(e, listener, detector.getDetectorId())));

    }

    private void onFailure(Exception e, ActionListener<List<AnomalyResult>> listener, String detectorId) {
        logger.info("Fail to preview anomaly detector " + detectorId, e);
        listener.onResponse(Collections.emptyList());
    }

    private List<AnomalyResult> parsePreviewResult(AnomalyDetector detector, Features features, List<ThresholdingResult> results) {
        // unprocessedFeatures[][], each row is for one date range.
        // For example, unprocessedFeatures[0][2] is for the first time range, the third feature
        double[][] unprocessedFeatures = features.getUnprocessedFeatures();
        List<Map.Entry<Long, Long>> timeRanges = features.getTimeRanges();
        List<Feature> featureAttributes = detector.getFeatureAttributes().stream().filter(Feature::getEnabled).collect(Collectors.toList());

        List<AnomalyResult> anomalyResults = new ArrayList<>();
        if (timeRanges != null && timeRanges.size() > 0) {
            for (int i = 0; i < timeRanges.size(); i++) {
                Map.Entry<Long, Long> timeRange = timeRanges.get(i);

                List<FeatureData> featureDatas = new ArrayList<>();
                for (int j = 0; j < featureAttributes.size(); j++) {
                    double value = unprocessedFeatures[i][j];
                    Feature feature = featureAttributes.get(j);
                    FeatureData data = new FeatureData(feature.getId(), feature.getName(), value);
                    featureDatas.add(data);
                }

                AnomalyResult result;
                if (results != null && results.size() > i) {
                    ThresholdingResult thresholdingResult = results.get(i);
                    result = new AnomalyResult(
                        detector.getDetectorId(),
                        null,
                        thresholdingResult.getGrade(),
                        thresholdingResult.getConfidence(),
                        featureDatas,
                        Instant.ofEpochMilli(timeRange.getKey()),
                        Instant.ofEpochMilli(timeRange.getValue()),
                        null,
                        null,
                        null
                    );
                } else {
                    result = new AnomalyResult(
                        detector.getDetectorId(),
                        null,
                        null,
                        null,
                        featureDatas,
                        Instant.ofEpochMilli(timeRange.getKey()),
                        Instant.ofEpochMilli(timeRange.getValue()),
                        null,
                        null,
                        null
                    );
                }

                anomalyResults.add(result);
            }
        }
        return anomalyResults;
    }

    private List<AnomalyResult> sample(List<AnomalyResult> results, int sampleSize) {
        if (results.size() <= sampleSize) {
            return results;
        } else {
            double stepSize = (results.size() - 1.0) / (sampleSize - 1.0);
            List<AnomalyResult> samples = new ArrayList<>(sampleSize);
            for (int i = 0; i < sampleSize; i++) {
                int index = Math.min((int) (stepSize * i), results.size() - 1);
                samples.add(results.get(index));
            }
            return samples;
        }
    }

}
