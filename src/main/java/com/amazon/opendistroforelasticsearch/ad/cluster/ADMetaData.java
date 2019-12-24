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

package com.amazon.opendistroforelasticsearch.ad.cluster;

import static org.elasticsearch.cluster.metadata.MetaData.ALL_CONTEXTS;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaData.Custom;
import org.elasticsearch.cluster.metadata.MetaData.XContentContext;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.amazon.opendistroforelasticsearch.ad.annotation.Generated;
import com.google.common.base.Objects;

/**
 * Data structure defining meta-data of AD plugin
 *
 */
public class ADMetaData implements Custom {
    public static final String TYPE = "ad";
    // JSON field representing a list of deleted detector id and the timestamp
    // indicating the detector's deletion time.
    static final String DETECTOR_GRAVEYARD_FIELD = "detectorGraveyard";
    public static final ADMetaData EMPTY_METADATA = new ADMetaData(Collections.emptySet());
    public static final NamedXContentRegistry.Entry XCONTENT_REGISTRY;

    static final ObjectParser<Builder, Void> PARSER;

    static {
        PARSER = new ObjectParser<>("ad_meta", true, Builder::new);
        PARSER
            .declareObjectArray(
                Builder::addAllDeletedDetector,
                AnomalyDetectorGraveyard.getParser(),
                new ParseField(DETECTOR_GRAVEYARD_FIELD)
            );
        XCONTENT_REGISTRY = new NamedXContentRegistry.Entry(
            MetaData.Custom.class,
            new ParseField(TYPE),
            it -> PARSER.parse(it, null).build()
        );
    }

    private Set<AnomalyDetectorGraveyard> deadDetectors;

    public ADMetaData() {}

    public ADMetaData(Set<AnomalyDetectorGraveyard> deadDetectors) {
        this.deadDetectors = Collections.unmodifiableSet(deadDetectors);
    }

    public ADMetaData(AnomalyDetectorGraveyard... deadDetectors) {
        Set<AnomalyDetectorGraveyard> deletedDetectorSet = new HashSet<>();
        for (AnomalyDetectorGraveyard deletedDetector : deadDetectors) {
            deletedDetectorSet.add(deletedDetector);
        }
        this.deadDetectors = Collections.unmodifiableSet(deletedDetectorSet);
    }

    public ADMetaData(StreamInput in) throws IOException {
        int size = in.readVInt();
        Set<AnomalyDetectorGraveyard> deadDetectors = new HashSet<>();
        for (int i = 0; i < size; i++) {
            deadDetectors.add(new AnomalyDetectorGraveyard(in));
        }
        this.deadDetectors = Collections.unmodifiableSet(deadDetectors);
    }

    @Override
    public Diff<Custom> diff(Custom previousState) {
        return new ADMetaDataDiff((ADMetaData) previousState, this);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.write(deadDetectors.size());
        for (AnomalyDetectorGraveyard deadDetector : deadDetectors) {
            deadDetector.writeTo(out);
        }
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_1_0;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(DETECTOR_GRAVEYARD_FIELD);
        for (AnomalyDetectorGraveyard deadDetector : deadDetectors) {
            deadDetector.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public EnumSet<XContentContext> context() {
        return ALL_CONTEXTS;
    }

    public Set<AnomalyDetectorGraveyard> getAnomalyDetectorGraveyard() {
        return deadDetectors;
    }

    public static ADMetaData getADMetaData(ClusterState state) {
        if (state != null) {
            ADMetaData res = state.getMetaData().custom(TYPE);
            if (res != null) {
                return res;
            }
        }
        return EMPTY_METADATA;
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ADMetaData that = (ADMetaData) o;
        return Objects.equal(deadDetectors, that.getAnomalyDetectorGraveyard());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(deadDetectors);
    }

    public static class ADMetaDataDiff implements NamedDiff<Custom> {
        private Set<AnomalyDetectorGraveyard> addedDetectorGraveyard;
        private Set<AnomalyDetectorGraveyard> removedDetectorGraveyard;
        Logger LOG = LogManager.getLogger(ADMetaDataDiff.class);

        public ADMetaDataDiff(ADMetaData currentMeta, ADMetaData newMeta) {
            Set<AnomalyDetectorGraveyard> currentDetectorGraveyard = currentMeta.getAnomalyDetectorGraveyard();
            Set<AnomalyDetectorGraveyard> newDetectorGraveyard = newMeta.getAnomalyDetectorGraveyard();
            Set<AnomalyDetectorGraveyard> added;
            Set<AnomalyDetectorGraveyard> removed;
            if (currentDetectorGraveyard.isEmpty()) {
                added = new HashSet<>(newDetectorGraveyard);
                removed = Collections.emptySet();
            } else if (newDetectorGraveyard.isEmpty()) {
                added = Collections.emptySet();
                removed = new HashSet<>(currentDetectorGraveyard);
            } else {
                added = new HashSet<>();
                removed = new HashSet<>();
                for (AnomalyDetectorGraveyard key : currentDetectorGraveyard) {
                    if (!newDetectorGraveyard.contains(key)) {
                        removed.add(key);
                    }
                }

                for (AnomalyDetectorGraveyard element : newDetectorGraveyard) {
                    if (!currentDetectorGraveyard.contains(element)) {
                        added.add(element);
                    }
                }
            }

            this.addedDetectorGraveyard = Collections.unmodifiableSet(added);
            this.removedDetectorGraveyard = Collections.unmodifiableSet(removed);
        }

        public ADMetaDataDiff(StreamInput in) throws IOException {
            this.addedDetectorGraveyard = Collections.unmodifiableSet(in.readSet(AnomalyDetectorGraveyard::new));
            this.removedDetectorGraveyard = Collections.unmodifiableSet(in.readSet(AnomalyDetectorGraveyard::new));
        }

        @Override
        public Custom apply(Custom current) {
            // currentDeadDetectors is unmodifiable
            Set<AnomalyDetectorGraveyard> currentDeadDetectors = ((ADMetaData) current).deadDetectors;
            Set<AnomalyDetectorGraveyard> newDeadDetectors = new HashSet<>(currentDeadDetectors);
            newDeadDetectors.addAll(addedDetectorGraveyard);
            newDeadDetectors.removeAll(removedDetectorGraveyard);
            return new ADMetaData(newDeadDetectors);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(addedDetectorGraveyard);
            out.writeCollection(removedDetectorGraveyard);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }
    }

    /**
     * Builder for instantiating an ADMetaData object
     *
     */
    public static class Builder {
        private Set<AnomalyDetectorGraveyard> deletedDetectors;

        public Builder() {
            deletedDetectors = new HashSet<>();
        }

        public Builder addAllDeletedDetector(Collection<AnomalyDetectorGraveyard> deadDetectors) {
            for (AnomalyDetectorGraveyard deadDetector : deadDetectors) {
                this.deletedDetectors.add(deadDetector);
            }
            return this;
        }

        public ADMetaData build() {
            return new ADMetaData(this.deletedDetectors);
        }
    }
}
