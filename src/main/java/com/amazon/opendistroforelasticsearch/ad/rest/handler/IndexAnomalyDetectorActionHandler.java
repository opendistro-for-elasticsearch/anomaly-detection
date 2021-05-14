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

package com.amazon.opendistroforelasticsearch.ad.rest.handler;

import com.amazon.opendistroforelasticsearch.ad.feature.SearchFeatureDao;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.task.ADTaskManager;
import com.amazon.opendistroforelasticsearch.ad.transport.IndexAnomalyDetectorResponse;
import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * Anomaly detector REST action handler to process POST/PUT request.
 * POST request is for creating anomaly detector.
 * PUT request is for updating anomaly detector.
 */
public class IndexAnomalyDetectorActionHandler extends AbstractAnomalyDetectorActionHandler<IndexAnomalyDetectorResponse> {

    /**
     * Constructor function.
     *
     * @param clusterService          ClusterService
     * @param client                  ES node client that executes actions on the local node
     * @param transportService        ES transport service
     * @param listener                 ES channel used to construct bytes / builder based outputs, and send responses
     * @param anomalyDetectionIndices anomaly detector index manager
     * @param detectorId              detector identifier
     * @param seqNo                   sequence number of last modification
     * @param primaryTerm             primary term of last modification
     * @param refreshPolicy           refresh policy
     * @param anomalyDetector         anomaly detector instance
     * @param requestTimeout          request time out configuration
     * @param maxSingleEntityAnomalyDetectors     max single-entity anomaly detectors allowed
     * @param maxMultiEntityAnomalyDetectors      max multi-entity detectors allowed
     * @param maxAnomalyFeatures      max features allowed per detector
     * @param method                  Rest Method type
     * @param xContentRegistry        Registry which is used for XContentParser
     * @param user                    User context
     * @param adTaskManager           AD Task manager
     * @param searchFeatureDao        Search feature dao
     */
    public IndexAnomalyDetectorActionHandler(
        ClusterService clusterService,
        Client client,
        TransportService transportService,
        ActionListener<IndexAnomalyDetectorResponse> listener,
        AnomalyDetectionIndices anomalyDetectionIndices,
        String detectorId,
        Long seqNo,
        Long primaryTerm,
        WriteRequest.RefreshPolicy refreshPolicy,
        AnomalyDetector anomalyDetector,
        TimeValue requestTimeout,
        Integer maxSingleEntityAnomalyDetectors,
        Integer maxMultiEntityAnomalyDetectors,
        Integer maxAnomalyFeatures,
        RestRequest.Method method,
        NamedXContentRegistry xContentRegistry,
        User user,
        ADTaskManager adTaskManager,
        SearchFeatureDao searchFeatureDao
    ) {
        super(
                clusterService,
                client,
                transportService,
                listener,
                anomalyDetectionIndices,
                detectorId,
                seqNo,
                primaryTerm,
                refreshPolicy,
                anomalyDetector,
                requestTimeout,
                maxSingleEntityAnomalyDetectors,
                maxMultiEntityAnomalyDetectors,
                maxAnomalyFeatures,
                method,
                xContentRegistry,
                user,
                adTaskManager,
                searchFeatureDao,
                false
        );
    }

    /**
     * Start function to process create/update anomaly detector request.
     * Check if anomaly detector index exist first, if not, will create first.
     *
     * @throws IOException IOException from {@link AnomalyDetectionIndices#initAnomalyDetectorIndexIfAbsent(ActionListener)}
     */
    public void start() throws IOException {
        super.start();
    }
}
