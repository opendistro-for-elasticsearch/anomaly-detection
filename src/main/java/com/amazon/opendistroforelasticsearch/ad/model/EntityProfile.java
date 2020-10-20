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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * Profile output for detector entity.
 */
public class EntityProfile implements Writeable, ToXContent {
    // field name in toXContent
    public static final String CATEGORY_FIELD = "category_field";
    public static final String ENTITY_VALUE = "value";
    public static final String IS_ACTIVE = "is_active";

    private final String categoryField;
    private final String value;
    private final Boolean isActive;

    public EntityProfile(String categoryField, String value, Boolean isActive) {
        super();
        this.categoryField = categoryField;
        this.value = value;
        this.isActive = isActive;
    }

    public EntityProfile(StreamInput in) throws IOException {
        categoryField = in.readString();
        value = in.readString();
        isActive = in.readBoolean();
    }

    public String getCategoryField() {
        return categoryField;
    }

    public String getValue() {
        return value;
    }

    public Boolean getActive() {
        return isActive;
    }

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        return toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CATEGORY_FIELD, categoryField);
        builder.field(ENTITY_VALUE, value);
        builder.field(IS_ACTIVE, isActive);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(categoryField);
        out.writeString(value);
        out.writeBoolean(isActive);
    }

    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this);
        builder.append(CATEGORY_FIELD, categoryField);
        builder.append(ENTITY_VALUE, value);
        builder.append(IS_ACTIVE, isActive);
        return builder.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        if (obj instanceof EntityProfile) {
            EntityProfile other = (EntityProfile) obj;
            EqualsBuilder equalsBuilder = new EqualsBuilder();
            equalsBuilder.append(categoryField, other.categoryField);
            equalsBuilder.append(value, other.value);
            equalsBuilder.append(isActive, other.isActive);

            return equalsBuilder.isEquals();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(categoryField).append(value).append(isActive).toHashCode();
    }
}
