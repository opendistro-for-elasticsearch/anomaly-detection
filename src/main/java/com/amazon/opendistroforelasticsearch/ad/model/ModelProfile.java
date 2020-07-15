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

package com.amazon.opendistroforelasticsearch.ad.model;

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

import java.io.IOException;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class ModelProfile implements Writeable, ToXContent {
    // field name in toXContent
    public static final String MODEL_ID = "model_id";
    public static final String MODEL_SIZE_IN_BYTES = "model_size_in_bytes";
    public static final String NODE_ID = "node_id";

    private final String modelId;
    private final long modelSizeInBytes;
    private final String nodeId;

    public ModelProfile(String modelId, long modelSize, String nodeId) {
        super();
        this.modelId = modelId;
        this.modelSizeInBytes = modelSize;
        this.nodeId = nodeId;
    }

    public ModelProfile(StreamInput in) throws IOException {
        modelId = in.readString();
        modelSizeInBytes = in.readVLong();
        nodeId = in.readString();
    }

    public String getModelId() {
        return modelId;
    }

    public long getModelSize() {
        return modelSizeInBytes;
    }

    public String getNodeId() {
        return nodeId;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL_ID, modelId);
        if (modelSizeInBytes > 0) {
            builder.field(MODEL_SIZE_IN_BYTES, modelSizeInBytes);
        }
        builder.field(NODE_ID, nodeId);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeVLong(modelSizeInBytes);
        out.writeString(nodeId);
    }

    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this);
        builder.append(MODEL_ID, modelId);
        if (modelSizeInBytes > 0) {
            builder.append(MODEL_SIZE_IN_BYTES, modelSizeInBytes);
        }
        builder.append(NODE_ID, nodeId);
        return builder.toString();
    }
}
