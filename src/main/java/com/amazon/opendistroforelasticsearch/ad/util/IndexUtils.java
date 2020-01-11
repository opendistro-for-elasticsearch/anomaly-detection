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

package com.amazon.opendistroforelasticsearch.ad.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.List;
import java.util.Optional;

public class IndexUtils {
    /**
     * Status string of index that does not exist
     */
    public static final String NONEXISTENT_INDEX_STATUS = "non-existent";

    /**
     * Status string when an alias exists, but does not point to an index
     */
    public static final String ALIAS_EXISTS_NO_INDICES_STATUS = "alias exists, but does not point to any indices";
    public static final String ALIAS_POINTS_TO_MULTIPLE_INDICES_STATUS = "alias exists, but does not point to any " + "indices";

    private static final Logger logger = LogManager.getLogger(IndexUtils.class);

    private Client client;
    private ClientUtil clientUtil;
    private ClusterService clusterService;

    /**
     * Constructor
     *
     * @param client Client to make calls to ElasticSearch
     * @param clientUtil AD Client utility
     * @param clusterService ES ClusterService
     */
    public IndexUtils(Client client, ClientUtil clientUtil, ClusterService clusterService) {
        this.client = client;
        this.clientUtil = clientUtil;
        this.clusterService = clusterService;
    }

    /**
     * Gets the cluster index health for a particular index or the index an alias points to
     *
     * If an alias is passed in, it will only return the health status of an index it points to if it only points to a
     * single index. If it points to multiple indices, it will throw an exception.
     *
     * @param indexOrAliasName String of the index or alias name to get health of.
     * @return String represents the status of the index: "red", "yellow" or "green"
     * @throws IllegalArgumentException Thrown when an alias is passed in that points to more than one index
     */
    public String getIndexHealthStatus(String indexOrAliasName) throws IllegalArgumentException {
        if (!clusterService.state().getRoutingTable().hasIndex(indexOrAliasName)) {
            // Check if the index is actually an alias
            if (clusterService.state().getMetaData().hasAlias(indexOrAliasName)) {
                // List of all indices the alias refers to
                List<IndexMetaData> indexMetaDataList = clusterService
                    .state()
                    .getMetaData()
                    .getAliasAndIndexLookup()
                    .get(indexOrAliasName)
                    .getIndices();
                if (indexMetaDataList.size() == 0) {
                    return ALIAS_EXISTS_NO_INDICES_STATUS;
                } else if (indexMetaDataList.size() > 1) {
                    throw new IllegalArgumentException("Cannot get health for alias that points to multiple indices");
                } else {
                    indexOrAliasName = indexMetaDataList.get(0).getIndex().getName();
                }
            } else {
                return NONEXISTENT_INDEX_STATUS;
            }
        }

        ClusterIndexHealth indexHealth = new ClusterIndexHealth(
            clusterService.state().metaData().index(indexOrAliasName),
            clusterService.state().getRoutingTable().index(indexOrAliasName)
        );

        return indexHealth.getStatus().name().toLowerCase();
    }

    /**
     * Gets the number of documents in an index.
     *
     * @param indexName Name of the index
     * @return The number of documents in an index. 0 is returned if the index does not exist. -1 is returned if the
     * request fails.
     */
    public Long getNumberOfDocumentsInIndex(String indexName) {
        if (!clusterService.state().getRoutingTable().hasIndex(indexName)) {
            return 0L;
        }
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);
        searchRequest.source(sourceBuilder);
        Optional<SearchResponse> response = clientUtil.timedRequest(searchRequest, logger, client::search);
        return response.map(r -> r.getHits().getTotalHits().value).orElse(-1L);
    }
}
