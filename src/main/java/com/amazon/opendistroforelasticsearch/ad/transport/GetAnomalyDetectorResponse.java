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

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfile;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;

public class GetAnomalyDetectorResponse extends ActionResponse implements ToXContentObject {
    private long version;
    private String id;
    private long primaryTerm;
    private long seqNo;
    private AnomalyDetector detector;
    private AnomalyDetectorJob adJob;
    private RestStatus restStatus;
    private DetectorProfile profile;
    private boolean profileResponse;
    private boolean returnJob;

    public GetAnomalyDetectorResponse(StreamInput in) throws IOException {
        super(in);
        profileResponse = in.readBoolean();
        if (profileResponse) {
            profile = new DetectorProfile(in);
        } else {
            profile = null;
            id = in.readString();
            version = in.readLong();
            primaryTerm = in.readLong();
            seqNo = in.readLong();
            restStatus = in.readEnum(RestStatus.class);
            detector = new AnomalyDetector(in);
            returnJob = in.readBoolean();
            if (returnJob) {
                adJob = new AnomalyDetectorJob(in);
            } else {
                adJob = null;
            }
        }
    }

    public GetAnomalyDetectorResponse(
        long version,
        String id,
        long primaryTerm,
        long seqNo,
        AnomalyDetector detector,
        AnomalyDetectorJob adJob,
        boolean returnJob,
        RestStatus restStatus,
        DetectorProfile profile,
        boolean profileResponse
    ) {
        this.version = version;
        this.id = id;
        this.primaryTerm = primaryTerm;
        this.seqNo = seqNo;
        this.detector = detector;
        this.restStatus = restStatus;
        this.returnJob = returnJob;
        if (this.returnJob) {
            this.adJob = adJob;
        } else {
            this.adJob = null;
        }
        this.profile = profile;
        this.profileResponse = profileResponse;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (profileResponse) {
            out.writeBoolean(true); // profileResponse is true
            profile.writeTo(out);
        } else {
            out.writeBoolean(false); // profileResponse is false
            out.writeString(id);
            out.writeLong(version);
            out.writeLong(primaryTerm);
            out.writeLong(seqNo);
            out.writeEnum(restStatus);
            detector.writeTo(out);
            if (returnJob) {
                out.writeBoolean(true); // returnJob is true
                adJob.writeTo(out);
            } else {
                out.writeBoolean(false); // returnJob is false
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (profileResponse) {
            profile.toXContent(builder, params);
        } else {
            builder.startObject();
            builder.field(RestHandlerUtils._ID, id);
            builder.field(RestHandlerUtils._VERSION, version);
            builder.field(RestHandlerUtils._PRIMARY_TERM, primaryTerm);
            builder.field(RestHandlerUtils._SEQ_NO, seqNo);
            builder.field(RestHandlerUtils.ANOMALY_DETECTOR, detector);
            if (returnJob) {
                builder.field(RestHandlerUtils.ANOMALY_DETECTOR_JOB, adJob);
            }
            builder.endObject();
        }
        return builder;
    }
}
