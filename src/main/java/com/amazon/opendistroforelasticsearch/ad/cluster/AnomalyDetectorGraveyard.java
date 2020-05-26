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

package com.amazon.opendistroforelasticsearch.ad.cluster;

import java.io.IOException;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.amazon.opendistroforelasticsearch.ad.annotation.Generated;
import com.google.common.base.Objects;

/**
 * Data structure defining detector id and when this detector gets deleted
 *
 */
public class AnomalyDetectorGraveyard extends AbstractDiffable<AnomalyDetectorGraveyard> implements Writeable, ToXContentObject {
    static final String DETECTOR_ID_KEY = "adID";
    static final String DELETE_TIME_KEY = "deleteMillis";
    private static final ObjectParser<Builder, Void> PARSER;

    static {
        PARSER = new ObjectParser<>("adGraveyard", true, Builder::new);
        PARSER.declareString(Builder::detectorID, new ParseField(DETECTOR_ID_KEY));
        PARSER.declareLong(Builder::deleteEpochMillis, new ParseField(DELETE_TIME_KEY));
    }

    private String detectorID;
    private long deleteEpochMillis;

    public AnomalyDetectorGraveyard(String detectorID, long deleteEpochMillis) {
        this.detectorID = detectorID;
        this.deleteEpochMillis = deleteEpochMillis;
    }

    public AnomalyDetectorGraveyard(StreamInput in) throws IOException {
        this.detectorID = in.readString();
        this.deleteEpochMillis = in.readLong();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DETECTOR_ID_KEY, detectorID);
        builder.field(DELETE_TIME_KEY, deleteEpochMillis);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(detectorID);
        out.writeLong(deleteEpochMillis);
    }

    public String getDetectorID() {
        return detectorID;
    }

    public long getDeleteEpochMillis() {
        return deleteEpochMillis;
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AnomalyDetectorGraveyard that = (AnomalyDetectorGraveyard) o;
        return Objects.equal(detectorID, that.getDetectorID()) && Objects.equal(deleteEpochMillis, that.getDeleteEpochMillis());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(detectorID, deleteEpochMillis);
    }

    static ContextParser<Void, AnomalyDetectorGraveyard> getParser() {
        return (parser, context) -> PARSER.apply(parser, null).build();
    }

    /**
     * Builder for instantiating a AnomalyDetectorGraveyard object
     *
     */
    public static class Builder {
        private String adID;
        private long deleteTime;

        public Builder detectorID(String adID) {
            this.adID = adID;
            return this;
        }

        public Builder deleteEpochMillis(long deleteTime) {
            this.deleteTime = deleteTime;
            return this;
        }

        public AnomalyDetectorGraveyard build() {
            return new AnomalyDetectorGraveyard(this.adID, this.deleteTime);
        }
    }
}
