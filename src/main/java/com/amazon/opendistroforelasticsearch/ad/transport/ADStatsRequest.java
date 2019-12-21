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

import com.amazon.opendistroforelasticsearch.ad.stats.ADStats;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * ADStatsRequest implements a request to obtain stats about the AD plugin
 */
public class ADStatsRequest extends BaseNodesRequest<ADStatsRequest> {

    public static final String ALL_STATS_KEY = "_all";

    private Map<String, Boolean> statsRetrievalMap;

    /**
     * Constructor
     *
     * @param nodeIds nodeIds of nodes' stats to be retrieved
     */
    public ADStatsRequest(String... nodeIds) {
        super(nodeIds);
        statsRetrievalMap = initStatsMap();
    }

    /**
     * Initialize map that stores which stats should be retrieved
     */
    private Map<String, Boolean> initStatsMap() {
        Map<String, Boolean> stats = new HashMap<>();
        for (String statName : ADStats.StatNames.getNames()) {
            stats.put(statName, true);
        }
        return stats;
    }

    /**
     * Sets every stats retrieval status to true
     */
    public void all() {
        for (Map.Entry<String, Boolean> entry : statsRetrievalMap.entrySet()) {
            entry.setValue(true);
        }
    }

    /**
     * Sets every stats retrieval status to false
     */
    public void clear() {
        for (Map.Entry<String, Boolean> entry : statsRetrievalMap.entrySet()) {
            entry.setValue(false);
        }
    }

    /**
     * Sets a stats retrieval status to true if it is a valid stat
     *
     * @param stat stat name
     * @return true if the stats's retrieval status is successfully update; false otherwise
     */
    public boolean addStat(String stat) {
        if (statsRetrievalMap.containsKey(stat)) {
            statsRetrievalMap.put(stat, true);
            return true;
        }
        return false;
    }

    /**
     * Get the map that tracks which stats should be retrieved
     *
     * @return the map that contains the stat names to retrieval status mapping
     */
    public Map<String, Boolean> getStatsRetrievalMap() {
        return statsRetrievalMap;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        for (Map.Entry<String, Boolean> entry : statsRetrievalMap.entrySet()) {
            entry.setValue(in.readBoolean());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        for (Map.Entry<String, Boolean> entry : statsRetrievalMap.entrySet()) {
            out.writeBoolean(entry.getValue());
        }
    }
}