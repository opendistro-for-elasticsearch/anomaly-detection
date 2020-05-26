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

package com.amazon.opendistroforelasticsearch.ad.rest;

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.DETECTOR_ID;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.PROFILE;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.TYPE;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.createXContentParser;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestResponseListener;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorProfileRunner;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfile;
import com.amazon.opendistroforelasticsearch.ad.model.ProfileName;
import com.amazon.opendistroforelasticsearch.ad.settings.EnabledSetting;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

/**
 * This class consists of the REST handler to retrieve an anomaly detector.
 */
public class RestGetAnomalyDetectorAction extends BaseRestHandler {

    private static final String GET_ANOMALY_DETECTOR_ACTION = "get_anomaly_detector";
    private static final Logger logger = LogManager.getLogger(RestGetAnomalyDetectorAction.class);
    private final AnomalyDetectorProfileRunner profileRunner;
    private final Set<String> allProfileTypeStrs;
    private final Set<ProfileName> allProfileTypes;
    private final Set<ProfileName> defaultProfileTypes;

    public RestGetAnomalyDetectorAction(AnomalyDetectorProfileRunner profileRunner) {
        this.profileRunner = profileRunner;

        List<ProfileName> allProfiles = Arrays.asList(ProfileName.values());
        this.allProfileTypes = new HashSet<ProfileName>(allProfiles);
        this.allProfileTypeStrs = getProfileListStrs(allProfiles);

        List<ProfileName> defaultProfiles = Arrays.asList(ProfileName.ERROR, ProfileName.STATE);
        this.defaultProfileTypes = new HashSet<ProfileName>(defaultProfiles);
    }

    @Override
    public String getName() {
        return GET_ANOMALY_DETECTOR_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }
        String detectorId = request.param(DETECTOR_ID);
        String typesStr = request.param(TYPE);
        String rawPath = request.rawPath();
        if (!Strings.isEmpty(typesStr) || rawPath.endsWith(PROFILE) || rawPath.endsWith(PROFILE + "/")) {
            boolean all = request.paramAsBoolean("_all", false);
            return channel -> profileRunner
                .profile(detectorId, getProfileActionListener(channel, detectorId), getProfilesToCollect(typesStr, all));
        } else {
            boolean returnJob = request.paramAsBoolean("job", false);
            MultiGetRequest.Item adItem = new MultiGetRequest.Item(ANOMALY_DETECTORS_INDEX, detectorId)
                .version(RestActions.parseVersion(request));
            MultiGetRequest multiGetRequest = new MultiGetRequest().add(adItem);
            if (returnJob) {
                MultiGetRequest.Item adJobItem = new MultiGetRequest.Item(ANOMALY_DETECTOR_JOB_INDEX, detectorId)
                    .version(RestActions.parseVersion(request));
                multiGetRequest.add(adJobItem);
            }

            return channel -> client.multiGet(multiGetRequest, onMultiGetResponse(channel, returnJob, detectorId));
        }
    }

    private ActionListener<MultiGetResponse> onMultiGetResponse(RestChannel channel, boolean returnJob, String detectorId) {
        return new RestResponseListener<MultiGetResponse>(channel) {
            @Override
            public RestResponse buildResponse(MultiGetResponse multiGetResponse) throws Exception {
                MultiGetItemResponse[] responses = multiGetResponse.getResponses();
                XContentBuilder builder = null;
                AnomalyDetector detector = null;
                AnomalyDetectorJob adJob = null;
                for (MultiGetItemResponse response : responses) {
                    if (ANOMALY_DETECTORS_INDEX.equals(response.getIndex())) {
                        if (response.getResponse() == null || !response.getResponse().isExists()) {
                            return new BytesRestResponse(RestStatus.NOT_FOUND, "Can't find detector with id: " + detectorId);
                        }
                        builder = channel
                            .newBuilder()
                            .startObject()
                            .field(RestHandlerUtils._ID, response.getId())
                            .field(RestHandlerUtils._VERSION, response.getResponse().getVersion())
                            .field(RestHandlerUtils._PRIMARY_TERM, response.getResponse().getPrimaryTerm())
                            .field(RestHandlerUtils._SEQ_NO, response.getResponse().getSeqNo());
                        if (!response.getResponse().isSourceEmpty()) {
                            try (
                                XContentParser parser = RestHandlerUtils
                                    .createXContentParser(channel, response.getResponse().getSourceAsBytesRef())
                            ) {
                                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                                detector = parser.namedObject(AnomalyDetector.class, AnomalyDetector.PARSE_FIELD_NAME, null);
                            } catch (Exception e) {
                                return buildInternalServerErrorResponse(e, "Failed to parse detector with id: " + detectorId);
                            }
                        }
                    }

                    if (ANOMALY_DETECTOR_JOB_INDEX.equals(response.getIndex())) {
                        if (response.getResponse() != null
                            && response.getResponse().isExists()
                            && !response.getResponse().isSourceEmpty()) {
                            try (XContentParser parser = createXContentParser(channel, response.getResponse().getSourceAsBytesRef())) {
                                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                                adJob = AnomalyDetectorJob.parse(parser);
                            } catch (Exception e) {
                                return buildInternalServerErrorResponse(e, "Failed to parse detector job with id: " + detectorId);
                            }
                        }
                    }
                }

                builder.field(RestHandlerUtils.ANOMALY_DETECTOR, detector);
                if (returnJob) {
                    builder.field(RestHandlerUtils.ANOMALY_DETECTOR_JOB, adJob);
                }
                builder.endObject();
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        };
    }

    private ActionListener<DetectorProfile> getProfileActionListener(RestChannel channel, String detectorId) {
        return ActionListener
            .wrap(
                profile -> { channel.sendResponse(new BytesRestResponse(RestStatus.OK, profile.toXContent(channel.newBuilder()))); },
                exception -> { channel.sendResponse(buildInternalServerErrorResponse(exception, exception.getMessage())); }
            );
    }

    private RestResponse buildInternalServerErrorResponse(Exception e, String errorMsg) {
        logger.error(errorMsg, e);
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

    @Override
    public List<Route> routes() {
        String path = String.format(Locale.ROOT, "%s/{%s}", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID);
        return ImmutableList
            .of(
                new Route(RestRequest.Method.GET, path),
                new Route(RestRequest.Method.HEAD, path),
                new Route(
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID, PROFILE)
                ),
                // types is a profile names. See a complete list of supported profiles names in
                // com.amazon.opendistroforelasticsearch.ad.model.ProfileName.
                new Route(
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/{%s}/%s/{%s}", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID, PROFILE, TYPE)
                )
            );
    }

    private Set<String> getProfileListStrs(List<ProfileName> profileList) {
        return profileList.stream().map(profile -> profile.getName()).collect(Collectors.toSet());
    }
}
