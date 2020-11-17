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

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.PROFILE;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorProfileRunner;
import com.amazon.opendistroforelasticsearch.ad.EntityProfileRunner;
import com.amazon.opendistroforelasticsearch.ad.Name;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfile;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfileName;
import com.amazon.opendistroforelasticsearch.ad.model.EntityProfileName;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import com.google.common.collect.Sets;

public class GetAnomalyDetectorTransportAction extends HandledTransportAction<GetAnomalyDetectorRequest, GetAnomalyDetectorResponse> {

    private static final Logger LOG = LogManager.getLogger(GetAnomalyDetectorTransportAction.class);

    private final Client client;

    private final Set<String> allProfileTypeStrs;
    private final Set<DetectorProfileName> allProfileTypes;
    private final Set<DetectorProfileName> defaultDetectorProfileTypes;
    private final Set<String> allEntityProfileTypeStrs;
    private final Set<EntityProfileName> allEntityProfileTypes;
    private final Set<EntityProfileName> defaultEntityProfileTypes;
    private final NamedXContentRegistry xContentRegistry;
    private final DiscoveryNodeFilterer nodeFilter;

    @Inject
    public GetAnomalyDetectorTransportAction(
        TransportService transportService,
        DiscoveryNodeFilterer nodeFilter,
        ActionFilters actionFilters,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(GetAnomalyDetectorAction.NAME, transportService, actionFilters, GetAnomalyDetectorRequest::new);
        this.client = client;

        List<DetectorProfileName> allProfiles = Arrays.asList(DetectorProfileName.values());
        this.allProfileTypes = EnumSet.copyOf(allProfiles);
        this.allProfileTypeStrs = getProfileListStrs(allProfiles);
        List<DetectorProfileName> defaultProfiles = Arrays.asList(DetectorProfileName.ERROR, DetectorProfileName.STATE);
        this.defaultDetectorProfileTypes = new HashSet<DetectorProfileName>(defaultProfiles);

        List<EntityProfileName> allEntityProfiles = Arrays.asList(EntityProfileName.values());
        this.allEntityProfileTypes = EnumSet.copyOf(allEntityProfiles);
        this.allEntityProfileTypeStrs = getProfileListStrs(allEntityProfiles);
        List<EntityProfileName> defaultEntityProfiles = Arrays.asList(EntityProfileName.STATE);
        this.defaultEntityProfileTypes = new HashSet<EntityProfileName>(defaultEntityProfiles);

        this.xContentRegistry = xContentRegistry;
        this.nodeFilter = nodeFilter;
    }

    @Override
    protected void doExecute(Task task, GetAnomalyDetectorRequest request, ActionListener<GetAnomalyDetectorResponse> listener) {
        String detectorID = request.getDetectorID();
        Long version = request.getVersion();
        String typesStr = request.getTypeStr();
        String rawPath = request.getRawPath();
        String entityValue = request.getEntityValue();
        boolean all = request.isAll();
        boolean returnJob = request.isReturnJob();

        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            if (!Strings.isEmpty(typesStr) || rawPath.endsWith(PROFILE) || rawPath.endsWith(PROFILE + "/")) {
                if (entityValue != null) {
                    Set<EntityProfileName> entityProfilesToCollect = getEntityProfilesToCollect(typesStr, all);
                    EntityProfileRunner profileRunner = new EntityProfileRunner(
                        client,
                        xContentRegistry,
                        AnomalyDetectorSettings.NUM_MIN_SAMPLES
                    );
                    profileRunner
                        .profile(
                            detectorID,
                            entityValue,
                            entityProfilesToCollect,
                            ActionListener
                                .wrap(
                                    profile -> {
                                        listener
                                            .onResponse(
                                                new GetAnomalyDetectorResponse(0, null, 0, 0, null, null, false, null, null, profile, true)
                                            );
                                    },
                                    e -> listener.onFailure(e)
                                )
                        );
                } else {
                    Set<DetectorProfileName> profilesToCollect = getProfilesToCollect(typesStr, all);
                    AnomalyDetectorProfileRunner profileRunner = new AnomalyDetectorProfileRunner(
                        client,
                        xContentRegistry,
                        nodeFilter,
                        AnomalyDetectorSettings.NUM_MIN_SAMPLES
                    );
                    profileRunner.profile(detectorID, getProfileActionListener(listener, detectorID), profilesToCollect);
                }
            } else {
                MultiGetRequest.Item adItem = new MultiGetRequest.Item(ANOMALY_DETECTORS_INDEX, detectorID).version(version);
                MultiGetRequest multiGetRequest = new MultiGetRequest().add(adItem);
                if (returnJob) {
                    MultiGetRequest.Item adJobItem = new MultiGetRequest.Item(ANOMALY_DETECTOR_JOB_INDEX, detectorID).version(version);
                    multiGetRequest.add(adJobItem);
                }
                client.multiGet(multiGetRequest, onMultiGetResponse(listener, returnJob, detectorID));
            }
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }
    }

    private ActionListener<MultiGetResponse> onMultiGetResponse(
        ActionListener<GetAnomalyDetectorResponse> listener,
        boolean returnJob,
        String detectorId
    ) {
        return new ActionListener<MultiGetResponse>() {
            @Override
            public void onResponse(MultiGetResponse multiGetResponse) {
                MultiGetItemResponse[] responses = multiGetResponse.getResponses();
                AnomalyDetector detector = null;
                AnomalyDetectorJob adJob = null;
                String id = null;
                long version = 0;
                long seqNo = 0;
                long primaryTerm = 0;

                for (MultiGetItemResponse response : responses) {
                    if (ANOMALY_DETECTORS_INDEX.equals(response.getIndex())) {
                        if (response.getResponse() == null || !response.getResponse().isExists()) {
                            listener
                                .onFailure(
                                    new ElasticsearchStatusException("Can't find detector with id:  " + detectorId, RestStatus.NOT_FOUND)
                                );
                            return;
                        }
                        id = response.getId();
                        version = response.getResponse().getVersion();
                        primaryTerm = response.getResponse().getPrimaryTerm();
                        seqNo = response.getResponse().getSeqNo();
                        if (!response.getResponse().isSourceEmpty()) {
                            try (
                                XContentParser parser = RestHandlerUtils
                                    .createXContentParserFromRegistry(xContentRegistry, response.getResponse().getSourceAsBytesRef())
                            ) {
                                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                                detector = parser.namedObject(AnomalyDetector.class, AnomalyDetector.PARSE_FIELD_NAME, null);
                            } catch (Exception e) {
                                String message = "Failed to parse detector job " + detectorId;
                                listener.onFailure(buildInternalServerErrorResponse(e, message));
                                return;
                            }
                        }
                    }

                    if (ANOMALY_DETECTOR_JOB_INDEX.equals(response.getIndex())) {
                        if (response.getResponse() != null
                            && response.getResponse().isExists()
                            && !response.getResponse().isSourceEmpty()) {
                            try (
                                XContentParser parser = RestHandlerUtils
                                    .createXContentParserFromRegistry(xContentRegistry, response.getResponse().getSourceAsBytesRef())
                            ) {
                                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                                adJob = AnomalyDetectorJob.parse(parser);
                            } catch (Exception e) {
                                String message = "Failed to parse detector job " + detectorId;
                                listener.onFailure(buildInternalServerErrorResponse(e, message));
                                return;
                            }
                        }
                    }
                }
                listener
                    .onResponse(
                        new GetAnomalyDetectorResponse(
                            version,
                            id,
                            primaryTerm,
                            seqNo,
                            detector,
                            adJob,
                            returnJob,
                            RestStatus.OK,
                            null,
                            null,
                            false
                        )
                    );
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        };
    }

    private ActionListener<DetectorProfile> getProfileActionListener(
        ActionListener<GetAnomalyDetectorResponse> listener,
        String detectorId
    ) {
        return ActionListener.wrap(new CheckedConsumer<DetectorProfile, Exception>() {
            @Override
            public void accept(DetectorProfile profile) throws Exception {
                listener.onResponse(new GetAnomalyDetectorResponse(0, null, 0, 0, null, null, false, null, profile, null, true));
            }
        }, exception -> { listener.onFailure(exception); });
    }

    private ElasticsearchStatusException buildInternalServerErrorResponse(Exception e, String errorMsg) {
        LOG.error(errorMsg, e);
        return new ElasticsearchStatusException(errorMsg, RestStatus.INTERNAL_SERVER_ERROR);
    }

    /**
    *
    * @param typesStr a list of input profile types separated by comma
    * @param all whether we should return all profile in the response
    * @return profiles to collect for a detector
    */
    private Set<DetectorProfileName> getProfilesToCollect(String typesStr, boolean all) {
        if (all) {
            return this.allProfileTypes;
        } else if (Strings.isEmpty(typesStr)) {
            return this.defaultDetectorProfileTypes;
        } else {
            // Filter out unsupported types
            Set<String> typesInRequest = new HashSet<>(Arrays.asList(typesStr.split(",")));
            return DetectorProfileName.getNames(Sets.intersection(allProfileTypeStrs, typesInRequest));
        }
    }

    /**
     *
     * @param typesStr a list of input profile types separated by comma
     * @param all whether we should return all profile in the response
     * @return profiles to collect for an entity
     */
    private Set<EntityProfileName> getEntityProfilesToCollect(String typesStr, boolean all) {
        if (all) {
            return this.allEntityProfileTypes;
        } else if (Strings.isEmpty(typesStr)) {
            return this.defaultEntityProfileTypes;
        } else {
            // Filter out unsupported types
            Set<String> typesInRequest = new HashSet<>(Arrays.asList(typesStr.split(",")));
            return EntityProfileName.getNames(Sets.intersection(allEntityProfileTypeStrs, typesInRequest));
        }
    }

    private Set<String> getProfileListStrs(List<? extends Name> profileList) {
        return profileList.stream().map(profile -> profile.getName()).collect(Collectors.toSet());
    }
}
