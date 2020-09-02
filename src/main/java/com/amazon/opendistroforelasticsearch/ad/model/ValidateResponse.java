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
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class ValidateResponse implements ToXContentObject {
    private Map<String, List<String>> failures;
    private Map<String, List<String>> suggestedChanges;

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        return toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    public ValidateResponse() {
        failures = null;
        suggestedChanges = null;
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();

        xContentBuilder.startObject("failures");
        for (String key : failures.keySet()) {
            xContentBuilder.field(key, failures.get(key));
        }
        xContentBuilder.endObject();

        xContentBuilder.startObject("suggestedChanges");
        for (String key : suggestedChanges.keySet()) {
            xContentBuilder.field(key, suggestedChanges.get(key));
        }
        xContentBuilder.endObject();
        return xContentBuilder.endObject();
    }

    public Map<String, List<String>> getFailures() {
        return failures;
    }

    public Map<String, List<String>> getSuggestedChanges() {
        return suggestedChanges;
    }

    public void setFailures(Map<String, List<String>> failures) {
        this.failures = failures;
    }

    public void setSuggestedChanges(Map<String, List<String>> suggestedChanges) {
        this.suggestedChanges = suggestedChanges;
    }

}
