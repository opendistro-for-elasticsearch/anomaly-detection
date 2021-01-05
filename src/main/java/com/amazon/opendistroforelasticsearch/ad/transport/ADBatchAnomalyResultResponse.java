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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class ADBatchAnomalyResultResponse extends ActionResponse {
    public String nodeId;
    public boolean runTaskRemotely;

    public ADBatchAnomalyResultResponse(String nodeId, boolean runTaskRemotely) {
        this.nodeId = nodeId;
        this.runTaskRemotely = runTaskRemotely;
    }

    public ADBatchAnomalyResultResponse(StreamInput in) throws IOException {
        super(in);
        nodeId = in.readString();
        runTaskRemotely = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeId);
        out.writeBoolean(runTaskRemotely);
    }

    public String getNodeId() {
        return nodeId;
    }

    public boolean isRunTaskRemotely() {
        return runTaskRemotely;
    }

}
