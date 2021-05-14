/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package com.amazon.opendistroforelasticsearch.ad.rest;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.DETECTION_INTERVAL;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.DETECTION_WINDOW_DELAY;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_ANOMALY_FEATURES;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_MULTI_ENTITY_ANOMALY_DETECTORS;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_SINGLE_ENTITY_ANOMALY_DETECTORS;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.REQUEST_TIMEOUT;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;

public abstract class AbstractAnomalyDetectorAction extends BaseRestHandler {

    protected volatile TimeValue requestTimeout;
    protected volatile TimeValue detectionInterval;
    protected volatile TimeValue detectionWindowDelay;
    protected volatile Integer maxSingleEntityDetectors;
    protected volatile Integer maxMultiEntityDetectors;
    protected volatile Integer maxAnomalyFeatures;

    public AbstractAnomalyDetectorAction(Settings settings, ClusterService clusterService) {
        this.requestTimeout = REQUEST_TIMEOUT.get(settings);
        this.detectionInterval = DETECTION_INTERVAL.get(settings);
        this.detectionWindowDelay = DETECTION_WINDOW_DELAY.get(settings);
        this.maxSingleEntityDetectors = MAX_SINGLE_ENTITY_ANOMALY_DETECTORS.get(settings);
        this.maxMultiEntityDetectors = MAX_MULTI_ENTITY_ANOMALY_DETECTORS.get(settings);
        this.maxAnomalyFeatures = MAX_ANOMALY_FEATURES.get(settings);
        // TODO: will add more cluster setting consumer later
        // TODO: inject ClusterSettings only if clusterService is only used to get ClusterSettings
        clusterService.getClusterSettings().addSettingsUpdateConsumer(REQUEST_TIMEOUT, it -> requestTimeout = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DETECTION_INTERVAL, it -> detectionInterval = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DETECTION_WINDOW_DELAY, it -> detectionWindowDelay = it);
        clusterService
                .getClusterSettings()
                .addSettingsUpdateConsumer(MAX_SINGLE_ENTITY_ANOMALY_DETECTORS, it -> maxSingleEntityDetectors = it);
        clusterService
                .getClusterSettings()
                .addSettingsUpdateConsumer(MAX_MULTI_ENTITY_ANOMALY_DETECTORS, it -> maxMultiEntityDetectors = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_ANOMALY_FEATURES, it -> maxAnomalyFeatures = it);
    }
}
