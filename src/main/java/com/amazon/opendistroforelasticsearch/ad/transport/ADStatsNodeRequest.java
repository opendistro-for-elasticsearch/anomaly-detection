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

import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 *  ADStatsNodeRequest to get a nodes stat
 */
public class ADStatsNodeRequest extends BaseNodeRequest {
    private ADStatsRequest request;

    /**
     * Constructor
     */
    public ADStatsNodeRequest() {
        super();
    }

    /**
     * Constructor
     *
     * @param nodeId nodeId
     * @param request ADStatsRequest
     */
    public ADStatsNodeRequest(String nodeId, ADStatsRequest request) {
        super(nodeId);
        this.request = request;
    }

    /**
     * Get ADStatsRequest
     *
     * @return ADStatsRequest for this node
     */
    public ADStatsRequest getADStatsRequest() {
        return request;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        request = new ADStatsRequest();
        request.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }
}
