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
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestRequest;

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;

public class IndexAnomalyDetectorRequest extends ActionRequest {

    private String detectorID;
    private long seqNo;
    private long primaryTerm;
    private WriteRequest.RefreshPolicy refreshPolicy;
    private AnomalyDetector detector;
    private RestRequest.Method method;

    public IndexAnomalyDetectorRequest(StreamInput in) throws IOException {
        super(in);
        detectorID = in.readString();
        seqNo = in.readLong();
        primaryTerm = in.readLong();
        refreshPolicy = in.readEnum(WriteRequest.RefreshPolicy.class);
        detector = new AnomalyDetector(in);
        method = in.readEnum(RestRequest.Method.class);
    }

    public IndexAnomalyDetectorRequest(
        String detectorID,
        long seqNo,
        long primaryTerm,
        WriteRequest.RefreshPolicy refreshPolicy,
        AnomalyDetector detector,
        RestRequest.Method method
    ) {
        super();
        this.detectorID = detectorID;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.refreshPolicy = refreshPolicy;
        this.detector = detector;
        this.method = method;
    }

    public String getDetectorID() {
        return detectorID;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    public AnomalyDetector getDetector() {
        return detector;
    }

    public RestRequest.Method getMethod() {
        return method;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(detectorID);
        out.writeLong(seqNo);
        out.writeLong(primaryTerm);
        out.writeEnum(refreshPolicy);
        detector.writeTo(out);
        out.writeEnum(method);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
