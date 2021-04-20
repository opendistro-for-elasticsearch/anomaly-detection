/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.commons.authuser.User;

public class DeleteAnomalyResultsTransportAction extends HandledTransportAction<DeleteByQueryRequest, BulkByScrollResponse> {

    private final Client client;
    private volatile Boolean filterEnabled;
    private static final Logger logger = LogManager.getLogger(DeleteAnomalyResultsTransportAction.class);

    @Inject
    public DeleteAnomalyResultsTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Settings settings,
        ClusterService clusterService,
        Client client
    ) {
        super(DeleteAnomalyResultsAction.NAME, transportService, actionFilters, DeleteByQueryRequest::new);
        this.client = client;
        filterEnabled = AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES, it -> filterEnabled = it);
    }

    @Override
    protected void doExecute(Task task, DeleteByQueryRequest request, ActionListener<BulkByScrollResponse> listener) {
        delete(request, listener);
    }

    public void delete(DeleteByQueryRequest request, ActionListener<BulkByScrollResponse> listener) {
        User user = getUserContext(client);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            validateRole(request, user, listener);
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    private void validateRole(DeleteByQueryRequest request, User user, ActionListener<BulkByScrollResponse> listener) {
        if (user == null || !filterEnabled) {
            // Case 1: user == null when 1. Security is disabled. 2. When user is super-admin
            // Case 2: If Security is enabled and filter is disabled, proceed with search as
            // user is already authenticated to hit this API.
            client.execute(DeleteByQueryAction.INSTANCE, request, listener);
        } else {
            // Security is enabled and filter is enabled
            try {
                addUserBackendRolesFilter(user, request.getSearchRequest().source());
                logger.info("++++++++++ Filtering result by " + user.getBackendRoles());
                logger.info("========================================================");
                logger.info(request.getSearchRequest().source());
                logger.info("========================================================");
                client.execute(DeleteByQueryAction.INSTANCE, request, listener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }
}
