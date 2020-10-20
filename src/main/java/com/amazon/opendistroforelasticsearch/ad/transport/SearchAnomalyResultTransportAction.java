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

package com.amazon.opendistroforelasticsearch.ad.transport;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.commons.authuser.AuthUserRequestBuilder;
import com.amazon.opendistroforelasticsearch.commons.authuser.User;

public class SearchAnomalyResultTransportAction extends HandledTransportAction<SearchAnomalyRequest, SearchResponse> {
    private final Logger logger = LogManager.getLogger(SearchAnomalyResultTransportAction.class);

    private final Client client;
    private final RestClient restClient;
    private volatile Boolean filterEnabled;

    @Inject
    public SearchAnomalyResultTransportAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        Client client,
        RestClient restClient
    ) {
        super(SearchAnomalyResultAction.NAME, transportService, actionFilters, SearchAnomalyRequest::new);
        this.client = client;
        this.restClient = restClient;
        filterEnabled = AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES, it -> filterEnabled = it);
    }

    @Override
    protected void doExecute(Task task, SearchAnomalyRequest request, ActionListener<SearchResponse> listener) {
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            validateRole(request, listener);
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    private void validateRole(SearchAnomalyRequest request, ActionListener<SearchResponse> listener) {
        if (request.getAuthHeader() == null) {
            // Auth Header is empty when 1. Security is disabled. 2. When user is super-admin
            // Proceed with search
            search(request.getSearchRequest(), listener);
        } else if (!filterEnabled) {
            // Security is enabled and filter is disabled
            // Proceed with search as user is already authenticated to hit this API.
            search(request.getSearchRequest(), listener);
        } else {
            // Security is enabled and filter is enabled
            Request authRequest = new AuthUserRequestBuilder(request.getAuthHeader()).build();
            restClient.performRequestAsync(authRequest, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try {
                        User user = new User(response);
                        addFilter(user, request.getSearchRequest().source(), "user.backend_roles");
                        logger.debug("Filtering result by " + user.getBackendRoles());
                        search(request.getSearchRequest(), listener);
                    } catch (IOException e) {
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception exception) {
                    listener.onFailure(exception);
                }
            });
        }
    }

    private void search(SearchRequest request, ActionListener<SearchResponse> listener) {
        client.search(request, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                listener.onResponse(searchResponse);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void addFilter(User user, SearchSourceBuilder searchSourceBuilder, String fieldName) {
        TermsQueryBuilder filterBackendRoles = QueryBuilders.termsQuery(fieldName, user.getBackendRoles());
        if (searchSourceBuilder.query() instanceof BoolQueryBuilder) {
            BoolQueryBuilder queryBuilder = (BoolQueryBuilder) searchSourceBuilder.query();
            searchSourceBuilder.query(queryBuilder.filter(filterBackendRoles));
        } else {
            throw new ElasticsearchException("Search Results API does not support queries other than BoolQuery");
        }
    }
}
