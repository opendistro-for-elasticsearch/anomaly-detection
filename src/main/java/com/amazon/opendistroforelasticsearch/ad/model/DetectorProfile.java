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

public class DetectorProfile implements ToXContentObject, Mergeable {
    private DetectorState state;
    private String error;

    private static final String STATE_FIELD = "state";
    private static final String ERROR_FIELD = "error";

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        return toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();

        if (state != null) {
            xContentBuilder.field(STATE_FIELD, state);
        }
        if (error != null) {
            xContentBuilder.field(ERROR_FIELD, error);
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
