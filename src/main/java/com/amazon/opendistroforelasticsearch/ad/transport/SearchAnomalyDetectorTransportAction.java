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
import static com.amazon.opendistroforelasticsearch.ad.util.ParseUtils.addUserBackendRolesFilter;
import static com.amazon.opendistroforelasticsearch.ad.util.ParseUtils.getUserContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.commons.authuser.User;

public class SearchAnomalyDetectorTransportAction extends HandledTransportAction<SearchRequest, SearchResponse> {
    private final Logger logger = LogManager.getLogger(SearchAnomalyDetectorTransportAction.class);

    private final Client client;
    private volatile Boolean filterEnabled;
    private User user;

    @Inject
    public SearchAnomalyDetectorTransportAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        Client client
    ) {
        super(SearchAnomalyDetectorAction.NAME, transportService, actionFilters, SearchRequest::new);
        this.client = client;
        filterEnabled = AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES, it -> filterEnabled = it);
        user = null;
    }

    @Override
    protected void doExecute(Task task, SearchRequest request, ActionListener<SearchResponse> listener) {
        user = getUserContext(client);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            validateRole(request, listener);
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    private void validateRole(SearchRequest request, ActionListener<SearchResponse> listener) {
        if (user == null) {
            // Auth Header is empty when 1. Security is disabled. 2. When user is super-admin
            // Proceed with search
            search(request, listener);
        } else if (!filterEnabled) {
            // Security is enabled and filter is disabled
            // Proceed with search as user is already authenticated to hit this API.
            search(request, listener);
        } else {
            // Security is enabled and filter is enabled
            try {
                addUserBackendRolesFilter(user, request.source());
                logger.debug("Filtering result by " + user.getBackendRoles());
                search(request, listener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
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
}
