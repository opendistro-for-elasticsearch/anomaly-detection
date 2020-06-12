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
    private InitProgressProfile initProgress;

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        return toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    private DetectorProfile() {}

    public static class Builder {
        private DetectorState state = null;
        private String error = null;
        private ModelProfile[] modelProfile = null;
        private int shingleSize = -1;
        private String coordinatingNode = null;
        private long totalSizeInBytes = -1;
        private InitProgressProfile initProgress = null;

        public Builder() {}

        public Builder state(DetectorState state) {
            this.state = state;
            return this;
        }

        public Builder error(String error) {
            this.error = error;
            return this;
        }

        public Builder modelProfile(ModelProfile[] modelProfile) {
            this.modelProfile = modelProfile;
            return this;
        }

        public Builder shingleSize(int shingleSize) {
            this.shingleSize = shingleSize;
            return this;
        }

        public Builder coordinatingNode(String coordinatingNode) {
            this.coordinatingNode = coordinatingNode;
            return this;
        }

        public Builder totalSizeInBytes(long totalSizeInBytes) {
            this.totalSizeInBytes = totalSizeInBytes;
            return this;
        }

        public Builder initProgress(InitProgressProfile initProgress) {
            this.initProgress = initProgress;
            return this;
        }

        public DetectorProfile build() {
            DetectorProfile profile = new DetectorProfile();
            profile.state = this.state;
            profile.error = this.error;
            profile.modelProfile = modelProfile;
            profile.shingleSize = shingleSize;
            profile.coordinatingNode = coordinatingNode;
            profile.totalSizeInBytes = totalSizeInBytes;
            profile.initProgress = initProgress;

            return profile;
        }
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
        if (initProgress != null) {
            xContentBuilder.field(CommonName.INIT_PROGRESS, initProgress);
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

    public InitProgressProfile getInitProgress() {
        return initProgress;
    }

    public void setInitProgress(InitProgressProfile initProgress) {
        this.initProgress = initProgress;
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
        if (otherProfile.getInitProgress() != null) {
            this.initProgress = otherProfile.getInitProgress();
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

            EqualsBuilder equalsBuilder = new EqualsBuilder();
            if (state != null) {
                equalsBuilder.append(state, other.state);
            }
            if (error != null) {
                equalsBuilder.append(error, other.error);
            }
            if (modelProfile != null && modelProfile.length > 0) {
                equalsBuilder.append(modelProfile, other.modelProfile);
            }
            if (shingleSize != -1) {
                equalsBuilder.append(shingleSize, other.shingleSize);
            }
            if (coordinatingNode != null) {
                equalsBuilder.append(coordinatingNode, other.coordinatingNode);
            }
            if (totalSizeInBytes != -1) {
                equalsBuilder.append(totalSizeInBytes, other.totalSizeInBytes);
            }
            if (initProgress != null) {
                equalsBuilder.append(initProgress, other.initProgress);
            }
            return equalsBuilder.isEquals();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
            .append(state)
            .append(error)
            .append(modelProfile)
            .append(shingleSize)
            .append(coordinatingNode)
            .append(totalSizeInBytes)
            .append(initProgress)
            .toHashCode();
    }

    @Override
    public String toString() {
        ToStringBuilder toStringBuilder = new ToStringBuilder(this);

        if (state != null) {
            toStringBuilder.append(CommonName.STATE, state);
        }
        if (error != null) {
            toStringBuilder.append(CommonName.ERROR, error);
        }
        if (modelProfile != null && modelProfile.length > 0) {
            toStringBuilder.append(modelProfile);
        }
        if (shingleSize != -1) {
            toStringBuilder.append(CommonName.SHINGLE_SIZE, shingleSize);
        }
        if (coordinatingNode != null) {
            toStringBuilder.append(CommonName.COORDINATING_NODE, coordinatingNode);
        }
        if (totalSizeInBytes != -1) {
            toStringBuilder.append(CommonName.TOTAL_SIZE_IN_BYTES, totalSizeInBytes);
        }
        if (initProgress != null) {
            toStringBuilder.append(CommonName.INIT_PROGRESS, initProgress);
        }
        return toStringBuilder.toString();
    }
}
