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
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorProfileRunner;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfile;
import com.amazon.opendistroforelasticsearch.ad.model.ProfileName;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import com.google.common.collect.Sets;

public class GetAnomalyDetectorTransportAction extends HandledTransportAction<GetAnomalyDetectorRequest, GetAnomalyDetectorResponse> {

    private static final Logger LOG = LogManager.getLogger(GetAnomalyDetectorTransportAction.class);

    private final Client client;
    private final AnomalyDetectorProfileRunner profileRunner;
    private final Set<String> allProfileTypeStrs;
    private final Set<ProfileName> allProfileTypes;
    private final Set<ProfileName> defaultProfileTypes;
    private final NamedXContentRegistry xContentRegistry;

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
        List<ProfileName> allProfiles = Arrays.asList(ProfileName.values());
        this.allProfileTypes = new HashSet<ProfileName>(allProfiles);
        this.allProfileTypeStrs = getProfileListStrs(allProfiles);

        List<ProfileName> defaultProfiles = Arrays.asList(ProfileName.ERROR, ProfileName.STATE);
        this.defaultProfileTypes = new HashSet<ProfileName>(defaultProfiles);
        this.xContentRegistry = xContentRegistry;
        this.profileRunner = new AnomalyDetectorProfileRunner(
            client,
            xContentRegistry,
            nodeFilter,
            AnomalyDetectorSettings.NUM_MIN_SAMPLES
        );
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

        if (!Strings.isEmpty(typesStr) || rawPath.endsWith(PROFILE) || rawPath.endsWith(PROFILE + "/")) {
            if (entityValue != null) {
                profileRunner
                    .profileEntity(
                        detectorID,
                        entityValue,
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
                profileRunner.profile(detectorID, getProfileActionListener(listener, detectorID), getProfilesToCollect(typesStr, all));
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
                                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                                detector = parser.namedObject(AnomalyDetector.class, AnomalyDetector.PARSE_FIELD_NAME, null);
                            } catch (Exception e) {
                                String message = "Failed to parse detector job " + detectorId;
                                LOG.error(message, e);
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
                                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                                adJob = AnomalyDetectorJob.parse(parser);
                            } catch (Exception e) {
                                String message = "Failed to parse detector job " + detectorId;
                                LOG.error(message, e);
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

    private RestResponse buildInternalServerErrorResponse(Exception e, String errorMsg) {
        LOG.error(errorMsg, e);
        return new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, errorMsg);
    }

    private Set<ProfileName> getProfilesToCollect(String typesStr, boolean all) {
        if (all) {
            return this.allProfileTypes;
        } else if (Strings.isEmpty(typesStr)) {
            return this.defaultProfileTypes;
        } else {
            Set<String> typesInRequest = new HashSet<>(Arrays.asList(typesStr.split(",")));
            return ProfileName.getNames(Sets.intersection(this.allProfileTypeStrs, typesInRequest));
        }
    }

    private Set<String> getProfileListStrs(List<ProfileName> profileList) {
        return profileList.stream().map(profile -> profile.getName()).collect(Collectors.toSet());
    }
}
