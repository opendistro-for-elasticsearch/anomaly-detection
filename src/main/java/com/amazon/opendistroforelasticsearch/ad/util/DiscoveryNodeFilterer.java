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

package com.amazon.opendistroforelasticsearch.ad.util;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;

/**
 * Util class to filter unwanted node types
 *
 */
public class DiscoveryNodeFilterer {
    private static final Logger LOG = LogManager.getLogger(DiscoveryNodeFilterer.class);
    private final ClusterService clusterService;
    private final HotDataNodePredicate eligibleNodeFilter;

    public DiscoveryNodeFilterer(ClusterService clusterService) {
        this.clusterService = clusterService;
        eligibleNodeFilter = new HotDataNodePredicate();
    }

    /**
     * Find nodes that are elibile to be used by us.  For example, Ultrawarm
     *  introduces warm nodes into the ES cluster. Currently, we distribute
     *  model partitions to all data nodes in the cluster randomly, which
     *  could cause a model performance downgrade issue once warm nodes
     *  are throttled due to resource limitations. The PR excludes warm nodes
     *  to place model partitions.
     * @return an array of eligible data nodes
     */
    public DiscoveryNode[] getEligibleDataNodes() {
        ClusterState state = this.clusterService.state();
        final List<DiscoveryNode> eligibleNodes = new ArrayList<>();
        for (DiscoveryNode node : state.nodes()) {
            if (eligibleNodeFilter.test(node)) {
                eligibleNodes.add(node);
            }
        }
        return eligibleNodes.toArray(new DiscoveryNode[0]);
    }

    /**
     *
     * @return the number of eligible data nodes
     */
    public int getNumberOfEligibleDataNodes() {
        return getEligibleDataNodes().length;
    }

    /**
     * @param node a discovery node
     * @return whether we should use this node for AD
     */
    public boolean isEligibleNode(DiscoveryNode node) {
        return eligibleNodeFilter.test(node);
    }

    static class HotDataNodePredicate implements Predicate<DiscoveryNode> {
        @Override
        public boolean test(DiscoveryNode discoveryNode) {
            return discoveryNode.isDataNode()
                && discoveryNode
                    .getAttributes()
                    .getOrDefault(CommonName.BOX_TYPE_KEY, CommonName.HOT_BOX_TYPE)
                    .equals(CommonName.HOT_BOX_TYPE);
        }
    }
}
