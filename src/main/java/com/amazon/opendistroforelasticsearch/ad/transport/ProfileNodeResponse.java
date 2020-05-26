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
import java.util.Map;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * Profile response on a node
 */
public class ProfileNodeResponse extends BaseNodeResponse implements ToXContentFragment {
    // filed name in toXContent
    static final String MODEL_SIZE_IN_BYTES = "model_size_in_bytes";
    static final String SHINGLE_SIZE = "shingle_size";

    private Map<String, Long> modelSize;
    private int shingleSize;

    /**
     * Constructor
     *
     * @param in StreamInput
     * @throws IOException throws an IO exception if the StreamInput cannot be read from
     */
    public ProfileNodeResponse(StreamInput in) throws IOException {
        super(in);
        modelSize = in.readMap(StreamInput::readString, StreamInput::readLong);
        shingleSize = in.readInt();
    }

    /**
     * Constructor
     *
     * @param node DiscoveryNode object
     * @param modelSize Mapping of model id to its memory consumption in bytes
     * @param shingleSize shingle size
     */
    public ProfileNodeResponse(DiscoveryNode node, Map<String, Long> modelSize, int shingleSize) {
        super(node);
        this.modelSize = modelSize;
        this.shingleSize = shingleSize;
    }

    /**
     * Creates a new ProfileNodeResponse object and reads in the profile from an input stream
     *
     * @param in StreamInput to read from
     * @return ProfileNodeResponse object corresponding to the input stream
     * @throws IOException throws an IO exception if the StreamInput cannot be read from
     */
    public static ProfileNodeResponse readProfiles(StreamInput in) throws IOException {
        return new ProfileNodeResponse(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(modelSize, StreamOutput::writeString, StreamOutput::writeLong);
        out.writeInt(shingleSize);
    }

    /**
     * Converts profile to xContent
     *
     * @param builder XContentBuilder
     * @param params Params
     * @return XContentBuilder
     * @throws IOException thrown by builder for invalid field
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(MODEL_SIZE_IN_BYTES);
        for (Map.Entry<String, Long> entry : modelSize.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();

        builder.field(SHINGLE_SIZE, shingleSize);

        return builder;
    }

    public Map<String, Long> getModelSize() {
        return modelSize;
    }

    public int getShingleSize() {
        return shingleSize;
    }
}
