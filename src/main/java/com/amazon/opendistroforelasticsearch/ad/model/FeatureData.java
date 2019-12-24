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

package com.amazon.opendistroforelasticsearch.ad.model;

import com.amazon.opendistroforelasticsearch.ad.annotation.Generated;
import com.google.common.base.Objects;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Feature data used by RCF model.
 */
public class FeatureData implements ToXContentObject {

    public static final String FEATURE_ID_FIELD = "feature_id";
    public static final String FEATURE_NAME_FIELD = "feature_name";
    public static final String DATA_FIELD = "data";

    private final String featureId;
    private final String featureName;
    private final Double data;

    public FeatureData(String featureId, String featureName, Double data) {
        this.featureId = featureId;
        this.featureName = featureName;
        this.data = data;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
            .startObject()
            .field(FEATURE_ID_FIELD, featureId)
            .field(FEATURE_NAME_FIELD, featureName)
            .field(DATA_FIELD, data);
        return xContentBuilder.endObject();
    }

    public static FeatureData parse(XContentParser parser) throws IOException {
        String featureId = null;
        Double data = null;
        String parsedFeatureName = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case FEATURE_ID_FIELD:
                    featureId = parser.text();
                    break;
                case FEATURE_NAME_FIELD:
                    parsedFeatureName = parser.text();
                    break;
                case DATA_FIELD:
                    data = parser.doubleValue();
                    break;
                default:
                    break;
            }
        }
        return new FeatureData(featureId, parsedFeatureName, data);
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        FeatureData that = (FeatureData) o;
        return Objects.equal(getFeatureId(), that.getFeatureId())
            && Objects.equal(getFeatureName(), that.getFeatureName())
            && Objects.equal(getData(), that.getData());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(getFeatureId(), getData());
    }

    @Generated
    public String getFeatureId() {
        return featureId;
    }

    @Generated
    public Double getData() {
        return data;
    }

    @Generated
    public String getFeatureName() {
        return featureName;
    }
}
