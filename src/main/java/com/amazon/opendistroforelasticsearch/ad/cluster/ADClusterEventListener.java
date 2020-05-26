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

package com.amazon.opendistroforelasticsearch.ad.cluster;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes.Delta;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.gateway.GatewayService;

import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;

public class ADClusterEventListener implements ClusterStateListener {
    private static final Logger LOG = LogManager.getLogger(ADClusterEventListener.class);
    static final String NODE_NOT_APPLIED_MSG = "AD does not use master or ultrawarm nodes";
    static final String NOT_RECOVERED_MSG = "CLuster is not recovered yet.";
    static final String IN_PROGRESS_MSG = "Cluster state change in progress, return.";
    static final String REMOVE_MODEL_MSG = "Remove model";
    static final String NODE_ADDED_MSG = "Data node added ";
    static final String NODE_REMOVED_MSG = "Data node removed ";

    private final Semaphore inProgress;
    private HashRing hashRing;
    private ModelManager modelManager;
    private final ClusterService clusterService;
    private final DiscoveryNodeFilterer nodeFilter;

    @Inject
    public ADClusterEventListener(
        ClusterService clusterService,
        HashRing hashRing,
        ModelManager modelManager,
        DiscoveryNodeFilterer nodeFilter
    ) {
        this.clusterService = clusterService;
        this.clusterService.addListener(this);
        this.hashRing = hashRing;
        this.modelManager = modelManager;
        this.inProgress = new Semaphore(1);
        this.nodeFilter = nodeFilter;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {

        if (!nodeFilter.isEligibleNode(event.state().nodes().getLocalNode())) {
            LOG.debug(NODE_NOT_APPLIED_MSG);
            return;
        }

        hashRing.recordMembershipChange();

        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            LOG.info(NOT_RECOVERED_MSG);
            return;
        }

        if (!inProgress.tryAcquire()) {
            LOG.info(IN_PROGRESS_MSG);
            return;
        }

        try {
            Delta delta = event.nodesDelta();

            // Check whether it was a data node that was removed
            boolean dataNodeRemoved = false;
            for (DiscoveryNode removedNode : delta.removedNodes()) {
                if (nodeFilter.isEligibleNode(removedNode)) {
                    LOG.info(NODE_REMOVED_MSG + " {}", removedNode.getId());
                    dataNodeRemoved = true;
                    break;
                }
            }

            // Check whether it was a data node that was added
            boolean dataNodeAdded = false;
            for (DiscoveryNode addedNode : delta.addedNodes()) {
                if (nodeFilter.isEligibleNode(addedNode)) {
                    LOG.info(NODE_ADDED_MSG + " {}", addedNode.getId());
                    dataNodeAdded = true;
                    break;
                }
            }

            boolean rebuiltDone = false;

            if (dataNodeAdded || dataNodeRemoved) {
                rebuiltDone = hashRing.build();
            }

            if (dataNodeAdded && rebuiltDone) {
                String localNodeId = event.state().nodes().getLocalNode().getId();
                Set<String> modelIds = modelManager.getAllModelIds();
                for (String modelId : modelIds) {
                    Optional<DiscoveryNode> node = hashRing.getOwningNode(modelId);
                    if (node.isPresent() && !node.get().getId().equals(localNodeId)) {
                        LOG.info(REMOVE_MODEL_MSG + " {}", modelId);
                        modelManager.stopModel(modelManager.getDetectorIdForModelId(modelId), modelId);
                    }
                }
            }
        } catch (Exception ex) {
            // One possible exception is ElasticsearchTimeoutException thrown when we fail
            // to put checkpoint when ModelManager stops model.
            LOG.error("Cluster state change handler has issue(s)", ex);
        } finally {
            inProgress.release();
        }

    }
}
