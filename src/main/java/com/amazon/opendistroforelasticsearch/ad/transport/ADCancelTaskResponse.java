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

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class ADCancelTaskResponse extends BaseNodesResponse<ADCancelTaskNodeResponse> {

    public ADCancelTaskResponse(StreamInput in) throws IOException {
        super(new ClusterName(in), in.readList(ADCancelTaskNodeResponse::readNodeResponse), in.readList(FailedNodeException::new));
    }

    public ADCancelTaskResponse(ClusterName clusterName, List<ADCancelTaskNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    public void writeNodesTo(StreamOutput out, List<ADCancelTaskNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public List<ADCancelTaskNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(ADCancelTaskNodeResponse::readNodeResponse);
    }
}
