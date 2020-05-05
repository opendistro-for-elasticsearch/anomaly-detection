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

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import com.amazon.opendistroforelasticsearch.ad.model.ProfileName;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * implements a request to obtain profiles about an AD detector
 */
public class ProfileRequest extends BaseNodesRequest<ProfileRequest> {

    private Set<ProfileName> profilesToBeRetrieved;
    private String detectorId;

    public ProfileRequest(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        profilesToBeRetrieved = new HashSet<ProfileName>();
        if (size != 0) {
            for (int i = 0; i < size; i++) {
                profilesToBeRetrieved.add(in.readEnum(ProfileName.class));
            }
        }
        detectorId = in.readString();
    }

    /**
     * Constructor
     *
     * @param detectorId detector's id
     * @param profilesToBeRetrieved profiles to be retrieved
     * @param nodes nodes of nodes' profiles to be retrieved
     */
    public ProfileRequest(String detectorId, Set<ProfileName> profilesToBeRetrieved, DiscoveryNode... nodes) {
        super(nodes);
        this.detectorId = detectorId;
        this.profilesToBeRetrieved = profilesToBeRetrieved;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(profilesToBeRetrieved.size());
        for (ProfileName profile : profilesToBeRetrieved) {
            out.writeEnum(profile);
        }
        out.writeString(detectorId);
    }

    public String getDetectorId() {
        return detectorId;
    }

    /**
     * Get the set that tracks which profiles should be retrieved
     *
     * @return the set that contains the profile names marked for retrieval
     */
    public Set<ProfileName> getProfilesToBeRetrieved() {
        return profilesToBeRetrieved;
    }
}
