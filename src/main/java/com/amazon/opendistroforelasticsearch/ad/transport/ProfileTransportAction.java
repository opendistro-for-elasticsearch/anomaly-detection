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

package com.amazon.opendistroforelasticsearch.ad.transport;

import com.amazon.opendistroforelasticsearch.ad.feature.FeatureManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.model.ProfileName;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *  This class contains the logic to extract the stats from the nodes
 */
public class ProfileTransportAction extends TransportNodesAction<ProfileRequest, ProfileResponse, ProfileNodeRequest, ProfileNodeResponse> {

    private ModelManager modelManager;
    private FeatureManager featureManager;

    /**
     * Constructor
     *
     * @param threadPool ThreadPool to use
     * @param clusterService ClusterService
     * @param transportService TransportService
     * @param actionFilters Action Filters
     * @param modelManager model manager object
     * @param featureManager feature manager object
     */
    @Inject
    public ProfileTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ModelManager modelManager,
        FeatureManager featureManager
    ) {
        super(
            ProfileAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ProfileRequest::new,
            ProfileNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            ProfileNodeResponse.class
        );
        this.modelManager = modelManager;
        this.featureManager = featureManager;
    }

    @Override
    protected ProfileResponse newResponse(ProfileRequest request, List<ProfileNodeResponse> responses, List<FailedNodeException> failures) {
        return new ProfileResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected ProfileNodeRequest newNodeRequest(ProfileRequest request) {
        return new ProfileNodeRequest(request);
    }

    @Override
    protected ProfileNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new ProfileNodeResponse(in);
    }

    @Override
    protected ProfileNodeResponse nodeOperation(ProfileNodeRequest request) {
        String detectorId = request.getDetectorId();
        Set<ProfileName> profiles = request.getProfilesToBeRetrieved();
        int shingleSize = -1;
        if (profiles.contains(ProfileName.COORDINATING_NODE) || profiles.contains(ProfileName.SHINGLE_SIZE)) {
            shingleSize = featureManager.getShingleSize(detectorId);
        }
        Map<String, Long> modelSize = null;
        if (profiles.contains(ProfileName.TOTAL_SIZE_IN_BYTES) || profiles.contains(ProfileName.MODELS)) {
            modelSize = modelManager.getModelSize(detectorId);
        } else {
            modelSize = new HashMap<>();
        }
        return new ProfileNodeResponse(clusterService.localNode(), modelSize, shingleSize);
    }
}
