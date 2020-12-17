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

import com.amazon.opendistroforelasticsearch.ad.stats.ADStatsResponse;

public class StatsAnomalyDetectorResponse extends ActionResponse implements ToXContentObject {
    private ADStatsResponse adStatsResponse;

    public StatsAnomalyDetectorResponse(StreamInput in) throws IOException {
        super(in);
        adStatsResponse = new ADStatsResponse(in);
    }

    public StatsAnomalyDetectorResponse(ADStatsResponse adStatsResponse) {
        this.adStatsResponse = adStatsResponse;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        adStatsResponse.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        adStatsResponse.toXContent(builder, params);
        return builder;
    }

    protected ADStatsResponse getAdStatsResponse() {
        return adStatsResponse;
    }
}
