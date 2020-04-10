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

import static org.elasticsearch.action.ValidateActions.addValidationError;

import java.io.IOException;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonMessageAttributes;

import java.util.Map;
import java.util.List;

public class EntityResultRequest extends ActionRequest implements ToXContentObject {

    private String detectorId;
    private Map<String, double[]> entities;
    private long start;
    private long end;

    public EntityResultRequest(StreamInput in) throws IOException {
        super(in);
        this.detectorId = in.readString();
        this.entities = in.readMap(StreamInput::readString, StreamInput::readDoubleArray);
        this.start = in.readLong();
        this.end = in.readLong();
    }

    public EntityResultRequest(String detectorId, Map<String,double[]> entities, long start, long end) {
        super();
        this.detectorId = detectorId;
        this.entities = entities;
        this.start = start;
        this.end = end;
    }

    public String getDetectorId() {
        return this.detectorId;
    }

    public Map<String, double[]> getEntities() {
        return this.entities;
    }

    public long getStart() {
        return this.start;
    }

    public long getEnd() {
        return this.end;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(this.detectorId);
        out.writeMap(this.entities, StreamOutput::writeString, StreamOutput::writeDoubleArray);
        out.writeLong(this.start);
        out.writeLong(this.end);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }
}
