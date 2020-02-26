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

package test.com.amazon.opendistroforelasticsearch.ad.util;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.MASTER_ROLE;
import static org.mockito.Mockito.mock;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.transport.TransportAddress;

public class ClusterCreation {
    /**
     * Creates a cluster state where local node and master node can be specified
     *
     * @param localNode  node in allNodes that is the local node
     * @param masterNode node in allNodes that is the master node. Can be null if no master exists
     * @param allNodes   all nodes in the cluster
     * @return cluster state
     */
    public static ClusterState state(ClusterName name, DiscoveryNode localNode, DiscoveryNode masterNode, List<DiscoveryNode> allNodes) {
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        for (DiscoveryNode node : allNodes) {
            discoBuilder.add(node);
        }
        if (masterNode != null) {
            discoBuilder.masterNodeId(masterNode.getId());
        }
        discoBuilder.localNodeId(localNode.getId());

        ClusterState.Builder state = ClusterState.builder(name);
        state.nodes(discoBuilder);
        state.metaData(MetaData.builder().generateClusterUuidIfNeeded());
        return state.build();
    }

    /**
     * Create data node map
     * @param numDataNodes the number of data nodes
     * @return data nodes map
     *
     * TODO: ModelManagerTests has the same method.  Refactor.
     */
    public static ImmutableOpenMap<String, DiscoveryNode> createDataNodes(int numDataNodes) {
        ImmutableOpenMap.Builder<String, DiscoveryNode> dataNodes = ImmutableOpenMap.builder();
        for (int i = 0; i < numDataNodes; i++) {
            dataNodes.put("foo" + i, mock(DiscoveryNode.class));
        }
        return dataNodes.build();
    }

    /**
     * Create a cluster state with 1 master node and a few data nodes
     * @param numDataNodes the number of data nodes
     * @return the cluster state
     */
    public static ClusterState state(int numDataNodes) {
        DiscoveryNode masterNode = new DiscoveryNode(
            "foo0",
            "foo0",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
            Collections.emptyMap(),
            Collections.singleton(MASTER_ROLE),
            Version.CURRENT
        );
        List<DiscoveryNode> allNodes = new ArrayList<>();
        allNodes.add(masterNode);
        for (int i = 1; i <= numDataNodes - 1; i++) {
            allNodes
                .add(
                    new DiscoveryNode(
                        "foo" + i,
                        "foo" + i,
                        new TransportAddress(InetAddress.getLoopbackAddress(), 9300 + i),
                        Collections.emptyMap(),
                        Collections.singleton(DATA_ROLE),
                        Version.CURRENT
                    )
                );
        }
        return state(new ClusterName("test"), masterNode, masterNode, allNodes);
    }
}
