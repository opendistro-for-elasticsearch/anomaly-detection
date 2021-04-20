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

import java.io.IOException;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.caching.CacheProvider;
import com.amazon.opendistroforelasticsearch.ad.caching.EntityCache;
import com.amazon.opendistroforelasticsearch.ad.cluster.HashRing;
import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.model.EntityProfileName;
import com.amazon.opendistroforelasticsearch.ad.model.ModelProfile;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;

/**
 * Transport action to get entity profile.
 */
public class EntityProfileTransportAction extends HandledTransportAction<EntityProfileRequest, EntityProfileResponse> {

    private static final Logger LOG = LogManager.getLogger(EntityProfileTransportAction.class);
    public static final String NO_NODE_FOUND_MSG = "Cannot find model hosting node";
    static final String FAIL_TO_GET_ENTITY_PROFILE_MSG = "Cannot get entity profile info";

    private final TransportService transportService;
    private final ModelManager modelManager;
    private final HashRing hashRing;
    private final TransportRequestOptions option;
    private final ClusterService clusterService;
    private final CacheProvider cacheProvider;

    @Inject
    public EntityProfileTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        Settings settings,
        ModelManager modelManager,
        HashRing hashRing,
        ClusterService clusterService,
        CacheProvider cacheProvider
    ) {
        super(EntityProfileAction.NAME, transportService, actionFilters, EntityProfileRequest::new);
        this.transportService = transportService;
        this.modelManager = modelManager;
        this.hashRing = hashRing;
        this.option = TransportRequestOptions
            .builder()
            .withType(TransportRequestOptions.Type.REG)
            .withTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings))
            .build();
        this.clusterService = clusterService;
        this.cacheProvider = cacheProvider;
    }

    @Override
    protected void doExecute(Task task, EntityProfileRequest request, ActionListener<EntityProfileResponse> listener) {

        String adID = request.getAdID();
        String entityValue = request.getEntityValue();
        String modelId = modelManager.getEntityModelId(adID, entityValue);
        Optional<DiscoveryNode> node = hashRing.getOwningNode(modelId); // TODO: get worker node from entity task, then forward the request
                                                                        // to that node directly?
        if (!node.isPresent()) {
            listener.onFailure(new AnomalyDetectionException(adID, NO_NODE_FOUND_MSG));
            return;
        }

        String nodeId = node.get().getId();
        DiscoveryNode localNode = clusterService.localNode();
        if (localNode.getId().equals(nodeId)) {
            EntityCache cache = cacheProvider.get();
            Set<EntityProfileName> profilesToCollect = request.getProfilesToCollect();
            EntityProfileResponse.Builder builder = new EntityProfileResponse.Builder();
            if (profilesToCollect.contains(EntityProfileName.ENTITY_INFO)) { // TODO: do we really need to support choosing at so
                                                                             // fine-granular level?
                builder.setActive(cache.isActive(adID, modelId));
                builder.setLastActiveMs(cache.getLastActiveMs(adID, modelId));
            }
            if (profilesToCollect.contains(EntityProfileName.INIT_PROGRESS) || profilesToCollect.contains(EntityProfileName.STATE)) {
                builder.setTotalUpdates(cache.getTotalUpdates(adID, modelId));
            }
            if (profilesToCollect.contains(EntityProfileName.MODELS)) {
                long modelSize = cache.getModelSize(adID, modelId);
                if (modelSize > 0) {
                    builder.setModelProfile(new ModelProfile(modelId, modelSize, localNode.getId()));
                }
            }
            listener.onResponse(builder.build());
        } else {
            // redirect
            LOG.debug("Sending entity profile request to {} for detector {}, entity {}", nodeId, adID, entityValue);

            try {
                transportService
                    .sendRequest(
                        node.get(),
                        EntityProfileAction.NAME,
                        request,
                        option,
                        new TransportResponseHandler<EntityProfileResponse>() {

                            @Override
                            public EntityProfileResponse read(StreamInput in) throws IOException {
                                return new EntityProfileResponse(in);
                            }

                            @Override
                            public void handleResponse(EntityProfileResponse response) {
                                listener.onResponse(response);
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                listener.onFailure(exp);
                            }

                            @Override
                            public String executor() {
                                return ThreadPool.Names.SAME;
                            }

                        }
                    );
            } catch (Exception e) {
                LOG.error(String.format(Locale.ROOT, "Fail to get entity profile for detector {}, entity {}", adID, entityValue), e);
                listener.onFailure(new AnomalyDetectionException(adID, FAIL_TO_GET_ENTITY_PROFILE_MSG, e));
            }

        }
    }
}
