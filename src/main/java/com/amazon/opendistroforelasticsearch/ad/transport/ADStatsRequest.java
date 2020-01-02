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

package com.amazon.opendistroforelasticsearch.ad.transport;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * ADStatsRequest implements a request to obtain stats about the AD plugin
 */
public class ADStatsRequest extends BaseNodesRequest<ADStatsRequest> {

    /**
     * Key indicating all stats should be retrieved
     */
    public static final String ALL_STATS_KEY = "_all";

    private Set<String> validStats;
    private Set<String> statsToBeRetrieved;

    /**
     * Empty constructor needed for ADStatsTransportAction
     */
    public ADStatsRequest() {}

    /**
     * Constructor
     *
     * @param validStats a set of stat names that the user could potentially query
     * @param nodeIds nodeIds of nodes' stats to be retrieved
     */
    public ADStatsRequest(Set<String> validStats, String... nodeIds) {
        super(nodeIds);
        this.validStats = validStats;
        statsToBeRetrieved = new HashSet<>();
    }

    /**
     * Add all stats to be retrieved
     */
    public void all() {
        statsToBeRetrieved.addAll(validStats);
    }

    /**
     * Remove all stats from retrieval set
     */
    public void clear() {
        statsToBeRetrieved.clear();
    }

    /**
     * Adds a stat to the set of stats to be retrieved
     *
     * @param stat name of the stat
     * @return true if the stat is valid and marked for retrieval; false otherwise
     */
    public boolean addStat(String stat) {
        if (validStats.contains(stat)) {
            statsToBeRetrieved.add(stat);
            return true;
        }
        return false;
    }

    /**
     * Get the set that tracks which stats should be retrieved
     *
     * @return the set that contains the stat names marked for retrieval
     */
    public Set<String> getStatsToBeRetrieved() {
        return statsToBeRetrieved;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        validStats = in.readSet(StreamInput::readString);
        statsToBeRetrieved = in.readSet(StreamInput::readString);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringCollection(validStats);
        out.writeStringCollection(statsToBeRetrieved);
    }
}