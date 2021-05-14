/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package com.amazon.opendistroforelasticsearch.ad.model;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.google.common.base.Objects;

/**
 * DetectorValidationIssue is a single validation issue found for detector.
 *
 * For example, if detector's multiple features are using wrong type field or non existing field
 * the issue would be in `detector` aspect, not `model`;
 * and its type is FEATURE_ATTRIBUTES, because it is related to feature;
 * message would be the message from thrown exception;
 * subIssues are issues for each feature;
 * suggestion is how to fix the issue/subIssues found
 */
public class DetectorValidationIssue implements ToXContentObject, Writeable {
    private static final String MESSAGE_FIELD = "message";
    private static final String SUGGESTED_FIELD_NAME = "suggested_value";
    private static final String SUB_ISSUES_FIELD_NAME = "sub_issues";

    private final ValidationAspect aspect;
    private final DetectorValidationIssueType type;
    private final String message;
    private Map<String, String> subIssues;
    private Object suggestion;

    public ValidationAspect getAspect() {
        return aspect;
    }

    public DetectorValidationIssueType getType() {
        return type;
    }

    public String getMessage() {
        return message;
    }

    public Map<String, String> getSubIssues() {
        return subIssues;
    }

    public Object getSuggestion() {
        return suggestion;
    }

    public DetectorValidationIssue(
            ValidationAspect aspect,
            DetectorValidationIssueType type,
            String message,
            Map<String, String> subIssues,
            Object suggestion
    ) {
        this.aspect = aspect;
        this.type = type;
        this.message = message;
        this.subIssues = subIssues;
        this.suggestion = suggestion;
    }

    public DetectorValidationIssue(StreamInput input) throws IOException {
        aspect = input.readEnum(ValidationAspect.class);
        type = input.readEnum(DetectorValidationIssueType.class);
        message = input.readString();
        if (input.readBoolean()) {
            subIssues = input.readMap(StreamInput::readString, StreamInput::readString);
        }
        if (input.readBoolean()) {
            suggestion = input.readGenericValue();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(aspect);
        out.writeEnum(type);
        out.writeString(message);
        if (subIssues != null && !subIssues.isEmpty()) {
            out.writeBoolean(true);
            out.writeMap(subIssues, StreamOutput::writeString, StreamOutput::writeString);
        } else {
            out.writeBoolean(false);
        }
        if (suggestion != null) {
            out.writeBoolean(true);
            out.writeGenericValue(suggestion);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject().startObject(type.getName());
        xContentBuilder.field(MESSAGE_FIELD, message);
        if (subIssues != null) {
            XContentBuilder subIssuesBuilder = xContentBuilder.startObject(SUB_ISSUES_FIELD_NAME);
            for (Map.Entry<String, String> entry : subIssues.entrySet()) {
                subIssuesBuilder.field(entry.getKey(), entry.getValue());
            }
            subIssuesBuilder.endObject();
        }
        if (suggestion != null) {
            xContentBuilder.field(SUGGESTED_FIELD_NAME, suggestion);
        }

        return xContentBuilder.endObject().endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        DetectorValidationIssue anotherIssue = (DetectorValidationIssue) o;
        return Objects.equal(getAspect(), anotherIssue.getAspect())
                && Objects.equal(getMessage(), anotherIssue.getMessage())
                && Objects.equal(getSubIssues(), anotherIssue.getSubIssues())
                && Objects.equal(getSuggestion(), anotherIssue.getSuggestion())
                && Objects.equal(getType(), anotherIssue.getType());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(aspect, message, subIssues, subIssues, type);
    }
}
