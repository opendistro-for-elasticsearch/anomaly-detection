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

package com.amazon.opendistroforelasticsearch.ad.model;

import java.io.IOException;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;

public class DetectorProfile implements ToXContentObject, Mergeable {
    private DetectorState state;
    private String error;
    private ModelProfile[] modelProfile;
    private int shingleSize;
    private String coordinatingNode;
    private long totalSizeInBytes;

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        return toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    public DetectorProfile() {
        state = null;
        error = null;
        modelProfile = null;
        shingleSize = -1;
        coordinatingNode = null;
        totalSizeInBytes = -1;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();

        if (state != null) {
            xContentBuilder.field(CommonName.STATE, state);
        }
        if (error != null) {
            xContentBuilder.field(CommonName.ERROR, error);
        }
        if (modelProfile != null && modelProfile.length > 0) {
            xContentBuilder.startArray(CommonName.MODELS);
            for (ModelProfile profile : modelProfile) {
                profile.toXContent(xContentBuilder, params);
            }
            xContentBuilder.endArray();
        }
        if (shingleSize != -1) {
            xContentBuilder.field(CommonName.SHINGLE_SIZE, shingleSize);
        }
        if (coordinatingNode != null) {
            xContentBuilder.field(CommonName.COORDINATING_NODE, coordinatingNode);
        }
        if (totalSizeInBytes != -1) {
            xContentBuilder.field(CommonName.TOTAL_SIZE_IN_BYTES, totalSizeInBytes);
        }

        return xContentBuilder.endObject();
    }

    public DetectorState getState() {
        return state;
    }

    public void setState(DetectorState state) {
        this.state = state;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public ModelProfile[] getModelProfile() {
        return modelProfile;
    }

    public void setModelProfile(ModelProfile[] modelProfile) {
        this.modelProfile = modelProfile;
    }

    public int getShingleSize() {
        return shingleSize;
    }

    public void setShingleSize(int shingleSize) {
        this.shingleSize = shingleSize;
    }

    public String getCoordinatingNode() {
        return coordinatingNode;
    }

    public void setCoordinatingNode(String coordinatingNode) {
        this.coordinatingNode = coordinatingNode;
    }

    public long getTotalSizeInBytes() {
        return totalSizeInBytes;
    }

    public void setTotalSizeInBytes(long totalSizeInBytes) {
        this.totalSizeInBytes = totalSizeInBytes;
    }

    @Override
    public void merge(Mergeable other) {
        if (this == other || other == null || getClass() != other.getClass()) {
            return;
        }
        DetectorProfile otherProfile = (DetectorProfile) other;
        if (otherProfile.getState() != null) {
            this.state = otherProfile.getState();
        }
        if (otherProfile.getError() != null) {
            this.error = otherProfile.getError();
        }
        if (otherProfile.getCoordinatingNode() != null) {
            this.coordinatingNode = otherProfile.getCoordinatingNode();
        }
        if (otherProfile.getShingleSize() != -1) {
            this.shingleSize = otherProfile.getShingleSize();
        }
        if (otherProfile.getModelProfile() != null) {
            this.modelProfile = otherProfile.getModelProfile();
        }
        if (otherProfile.getTotalSizeInBytes() != -1) {
            this.totalSizeInBytes = otherProfile.getTotalSizeInBytes();
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        if (obj instanceof DetectorProfile) {
            DetectorProfile other = (DetectorProfile) obj;

            return new EqualsBuilder().append(state, other.state).append(error, other.error).isEquals();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(state).append(error).toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("state", state).append("error", error).toString();
    }
}
