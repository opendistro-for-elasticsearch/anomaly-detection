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
import org.elasticsearch.rest.RestStatus;

import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;

public class AnomalyDetectorJobResponse extends ActionResponse implements ToXContentObject {
    private final String id;
    private final long version;
    private final long seqNo;
    private final long primaryTerm;
    private final RestStatus restStatus;

    public AnomalyDetectorJobResponse(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
        version = in.readLong();
        seqNo = in.readLong();
        primaryTerm = in.readLong();
        restStatus = in.readEnum(RestStatus.class);
    }

    public AnomalyDetectorJobResponse(String id, long version, long seqNo, long primaryTerm, RestStatus restStatus) {
        this.id = id;
        this.version = version;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.restStatus = restStatus;
    }

    public String getId() {
        return id;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeLong(version);
        out.writeLong(seqNo);
        out.writeLong(primaryTerm);
        out.writeEnum(restStatus);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder
            .startObject()
            .field(RestHandlerUtils._ID, id)
            .field(RestHandlerUtils._VERSION, version)
            .field(RestHandlerUtils._SEQ_NO, seqNo)
            .field(RestHandlerUtils._PRIMARY_TERM, primaryTerm)
            .endObject();
    }
}
