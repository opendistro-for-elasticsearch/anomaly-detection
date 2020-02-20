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
import com.amazon.opendistroforelasticsearch.ad.constant.CommonMessageAttributes;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * Request should be sent from the handler logic of transport delete detector API
 *
 */
public class DeleteModelRequest extends BaseNodesRequest<DeleteModelRequest> implements ToXContentObject {
    private String adID;

    public String getAdID() {
        return adID;
    }

    public DeleteModelRequest() {
        super((String[]) null);
    }

    public DeleteModelRequest(StreamInput in) throws IOException {
        super(in);
        this.adID = in.readString();
    }

    public DeleteModelRequest(String adID, DiscoveryNode... nodes) {
        super(nodes);
        this.adID = adID;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(adID);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isEmpty(adID)) {
            validationException = addValidationError(CommonErrorMessages.AD_ID_MISSING_MSG, validationException);
        }
        return validationException;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CommonMessageAttributes.ID_JSON_KEY, adID);
        builder.endObject();
        return builder;
    }
}
