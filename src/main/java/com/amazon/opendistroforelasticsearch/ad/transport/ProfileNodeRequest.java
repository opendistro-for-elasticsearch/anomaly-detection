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
import java.util.Set;

import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import com.amazon.opendistroforelasticsearch.ad.model.ProfileName;

/**
 *  Class representing a nodes's profile request
 */
public class ProfileNodeRequest extends BaseNodeRequest {
    private ProfileRequest request;

    public ProfileNodeRequest(StreamInput in) throws IOException {
        super(in);
        this.request = new ProfileRequest(in);
    }

    /**
     * Constructor
     *
     * @param request profile request
     */
    public ProfileNodeRequest(ProfileRequest request) {
        this.request = request;
    }

    public String getDetectorId() {
        return request.getDetectorId();
    }

    /**
     * Get the set that tracks which profiles should be retrieved
     *
     * @return the set that contains the profile names marked for retrieval
     */
    public Set<ProfileName> getProfilesToBeRetrieved() {
        return request.getProfilesToBeRetrieved();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }
}
