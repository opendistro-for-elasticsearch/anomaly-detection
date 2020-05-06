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

package com.amazon.opendistroforelasticsearch.ad.stats;

import java.io.IOException;
import java.util.Map;

import com.amazon.opendistroforelasticsearch.ad.model.Mergeable;
import com.amazon.opendistroforelasticsearch.ad.transport.ADStatsNodesResponse;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * ADStatsResponse contains logic to merge the node stats and cluster stats together and return them to user
 */
public class ADStatsResponse implements ToXContentObject, Mergeable {
    private ADStatsNodesResponse adStatsNodesResponse;
    private Map<String, Object> clusterStats;

    /**
     * Get cluster stats
     *
     * @return Map of cluster stats
     */
    public Map<String, Object> getClusterStats() {
        return clusterStats;
    }

    /**
     * Set cluster stats
     *
     * @param clusterStats Map of cluster stats
     */
    public void setClusterStats(Map<String, Object> clusterStats) {
        this.clusterStats = clusterStats;
    }

    /**
     * Get cluster stats
     *
     * @return ADStatsNodesResponse
     */
    public ADStatsNodesResponse getADStatsNodesResponse() {
        return adStatsNodesResponse;
    }

    /**
     * Sets adStatsNodesResponse
     *
     * @param adStatsNodesResponse AD Stats Response from Nodes
     */
    public void setADStatsNodesResponse(ADStatsNodesResponse adStatsNodesResponse) {
        this.adStatsNodesResponse = adStatsNodesResponse;
    }

    /**
     * Convert ADStatsResponse to XContent
     *
     * @param builder XContentBuilder
     * @return XContentBuilder
     * @throws IOException thrown on invalid input
     */
    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        return toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        for (Map.Entry<String, Object> clusterStat : clusterStats.entrySet()) {
            builder.field(clusterStat.getKey(), clusterStat.getValue());
        }
        adStatsNodesResponse.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
        return xContentBuilder.endObject();
    }

    @Override
    public void merge(Mergeable other) {
        if (this == other || other == null || getClass() != other.getClass()) {
            return;
        }

        ADStatsResponse otherResponse = (ADStatsResponse) other;

        if (otherResponse.adStatsNodesResponse != null) {
            this.adStatsNodesResponse = otherResponse.adStatsNodesResponse;
        }

        if (otherResponse.clusterStats != null) {
            this.clusterStats = otherResponse.clusterStats;
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

        ADStatsResponse other = (ADStatsResponse) obj;
        return new EqualsBuilder()
            .append(adStatsNodesResponse, other.adStatsNodesResponse)
            .append(clusterStats, other.clusterStats)
            .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(adStatsNodesResponse).append(clusterStats).toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
            .append("adStatsNodesResponse", adStatsNodesResponse)
            .append("clusterStats", clusterStats)
            .toString();
    }
}
