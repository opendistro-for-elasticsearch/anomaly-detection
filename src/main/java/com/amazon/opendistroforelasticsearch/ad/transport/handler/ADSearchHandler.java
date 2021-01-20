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

package com.amazon.opendistroforelasticsearch.ad.transport.handler;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES;
import static com.amazon.opendistroforelasticsearch.ad.util.ParseUtils.addUserBackendRolesFilter;
import static com.amazon.opendistroforelasticsearch.ad.util.ParseUtils.getUserContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.commons.authuser.User;

/**
 * Handle general search request, check user role and return search response.
 */
public class ADSearchHandler {
    private final Logger logger = LogManager.getLogger(ADSearchHandler.class);
    private final Client client;
    private volatile Boolean filterEnabled;

    public ADSearchHandler(Settings settings, ClusterService clusterService, Client client) {
        this.client = client;
        filterEnabled = AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES, it -> filterEnabled = it);
    }

    /**
     * Validate user role, add backend role filter if filter enabled
     * and execute search.
     *
     * @param request search request
     * @param listener action listerner
     */
    public void search(SearchRequest request, ActionListener<SearchResponse> listener) {
        User user = getUserContext(client);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            validateRole(request, user, listener);
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    private void validateRole(SearchRequest request, User user, ActionListener<SearchResponse> listener) {
        if (user == null) {
            // Auth Header is empty when 1. Security is disabled. 2. When user is super-admin
            // Proceed with search
            searchDocs(request, listener);
        } else if (!filterEnabled) {
            // Security is enabled and filter is disabled
            // Proceed with search as user is already authenticated to hit this API.
            searchDocs(request, listener);
        } else {
            // Security is enabled and filter is enabled
            try {
                addUserBackendRolesFilter(user, request.source());
                logger.debug("Filtering result by " + user.getBackendRoles());
                searchDocs(request, listener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }

    private void searchDocs(SearchRequest request, ActionListener<SearchResponse> listener) {
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

}
