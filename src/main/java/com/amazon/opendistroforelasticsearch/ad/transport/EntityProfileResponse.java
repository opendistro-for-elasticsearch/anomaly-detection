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
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class EntityProfileResponse extends ActionResponse implements ToXContentObject {
    public static final String ACTIVE = "active";
    private final boolean isActive;

    public EntityProfileResponse(boolean isActive) {
        this.isActive = isActive;
    }

    public EntityProfileResponse(StreamInput in) throws IOException {
        super(in);
        isActive = in.readBoolean();
    }

    public boolean isActive() {
        return isActive;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(isActive);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ACTIVE, isActive);
        builder.endObject();
        return builder;
    }
}
