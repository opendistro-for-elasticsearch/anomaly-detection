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

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class SearchAnomalyDetectorInfoRequest extends ActionRequest {

    private String name;
    private String rawPath;

    public SearchAnomalyDetectorInfoRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readOptionalString();
        rawPath = in.readString();
    }

    public SearchAnomalyDetectorInfoRequest(String name, String rawPath) throws IOException {
        super();
        this.name = name;
        this.rawPath = rawPath;
    }

    public String getName() {
        return name;
    }

    public String getRawPath() {
        return rawPath;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(name);
        out.writeString(rawPath);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
