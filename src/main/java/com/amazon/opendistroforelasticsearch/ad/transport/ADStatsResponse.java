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

package com.amazon.opendistroforelasticsearch.ad.transport;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * ADStatsResponse consists of the aggregated responses from the nodes
 */
public class ADStatsResponse extends BaseNodesResponse<ADStatsNodeResponse> implements ToXContentObject {

    private static final String NODES_KEY = "nodes";
    private Map<String, Object> clusterStats;

    /**
     * Constructor
     *
     * @param in StreamInput
     * @throws IOException thrown when unable to read from stream
     */
    public ADStatsResponse(StreamInput in) throws IOException {
        super(new ClusterName(in), in.readList(ADStatsNodeResponse::readStats), in.readList(FailedNodeException::new));
        clusterStats = in.readMap();
    }

    /**
     * Constructor
     *
     * @param clusterName name of cluster
     * @param nodes List of ADStatsNodeResponses from nodes
     * @param failures List of failures from nodes
     * @param clusterStats Cluster level stats only obtained from a single node
     */
    public ADStatsResponse(
        ClusterName clusterName,
        List<ADStatsNodeResponse> nodes,
        List<FailedNodeException> failures,
        Map<String, Object> clusterStats
    ) {
        super(clusterName, nodes, failures);
        this.clusterStats = clusterStats;
    }

    Map<String, Object> getClusterStats() {
        return clusterStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(clusterStats);
    }

    @Override
    public void writeNodesTo(StreamOutput out, List<ADStatsNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public List<ADStatsNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(ADStatsNodeResponse::readStats);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (Map.Entry<String, Object> clusterStat : clusterStats.entrySet()) {
            builder.field(clusterStat.getKey(), clusterStat.getValue());
        }

        String nodeId;
        DiscoveryNode node;
        builder.startObject(NODES_KEY);
        for (ADStatsNodeResponse adStats : getNodes()) {
            node = adStats.getNode();
            nodeId = node.getId();
            builder.startObject(nodeId);
            adStats.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }
}
