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

import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;

/**
 * Profile response on a node
 */
public class ProfileNodeResponse extends BaseNodeResponse implements ToXContentFragment {
    // filed name in toXContent
    static final String MODEL_SIZE_IN_BYTES = "model_size_in_bytes";

    private Map<String, Long> modelSize;
    private int shingleSize;
    private long activeEntities;
    private long totalUpdates;

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
        activeEntities = in.readVLong();
        totalUpdates = in.readVLong();
    }

    /**
     * Constructor
     *
     * @param node DiscoveryNode object
     * @param modelSize Mapping of model id to its memory consumption in bytes
     * @param shingleSize shingle size
     * @param activeEntity active entity count
     * @param totalUpdates RCF model total updates
     */
    public ProfileNodeResponse(DiscoveryNode node, Map<String, Long> modelSize, int shingleSize, long activeEntity, long totalUpdates) {
        super(node);
        this.modelSize = modelSize;
        this.shingleSize = shingleSize;
        this.activeEntities = activeEntity;
        this.totalUpdates = totalUpdates;
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
        out.writeVLong(activeEntities);
        out.writeVLong(totalUpdates);
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

        builder.field(CommonName.SHINGLE_SIZE, shingleSize);
        builder.field(CommonName.ACTIVE_ENTITIES, activeEntities);
        builder.field(CommonName.TOTAL_UPDATES, totalUpdates);

        return builder;
    }

    public Map<String, Long> getModelSize() {
        return modelSize;
    }

    public int getShingleSize() {
        return shingleSize;
    }

    public long getActiveEntities() {
        return activeEntities;
    }

    public long getTotalUpdates() {
        return totalUpdates;
    }
}
