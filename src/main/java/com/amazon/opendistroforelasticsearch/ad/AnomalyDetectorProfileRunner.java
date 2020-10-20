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

package com.amazon.opendistroforelasticsearch.ad;

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.CATEGORY_FIELD_LIMIT;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.Throwables;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalCardinality;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.amazon.opendistroforelasticsearch.ad.common.exception.ResourceNotFoundException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorInternalState;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfile;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorState;
import com.amazon.opendistroforelasticsearch.ad.model.EntityProfile;
import com.amazon.opendistroforelasticsearch.ad.model.InitProgressProfile;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.model.ProfileName;
import com.amazon.opendistroforelasticsearch.ad.transport.EntityProfileAction;
import com.amazon.opendistroforelasticsearch.ad.transport.EntityProfileRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.ProfileAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ProfileRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.ProfileResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.RCFPollingAction;
import com.amazon.opendistroforelasticsearch.ad.transport.RCFPollingRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.RCFPollingResponse;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;
import com.amazon.opendistroforelasticsearch.ad.util.ExceptionUtil;
import com.amazon.opendistroforelasticsearch.ad.util.MultiResponsesDelegateActionListener;

public class AnomalyDetectorProfileRunner {
    private final Logger logger = LogManager.getLogger(AnomalyDetectorProfileRunner.class);
    private Client client;
    private NamedXContentRegistry xContentRegistry;
    private DiscoveryNodeFilterer nodeFilter;
    static String FAIL_TO_FIND_DETECTOR_MSG = "Fail to find detector with id: ";
    static String FAIL_TO_GET_PROFILE_MSG = "Fail to get profile for detector ";
    static String FAIL_TO_GET_TOTAL_ENTITIES = "Failed to get total entities for detector ";
    private long requiredSamples;

    public AnomalyDetectorProfileRunner(
        Client client,
        NamedXContentRegistry xContentRegistry,
        DiscoveryNodeFilterer nodeFilter,
        long requiredSamples
    ) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.nodeFilter = nodeFilter;
        if (requiredSamples <= 0) {
            throw new IllegalArgumentException("required samples should be a positive number, but was " + requiredSamples);
        }
        this.requiredSamples = requiredSamples;
    }

    public void profile(String detectorId, ActionListener<DetectorProfile> listener, Set<ProfileName> profilesToCollect) {

        if (profilesToCollect.isEmpty()) {
            listener.onFailure(new RuntimeException("Unsupported profile types."));
            return;
        }

        // total number of listeners we need to define. Needed by MultiResponsesDelegateActionListener to decide when to consolidate results
        // and return to users
        int totalListener = 0;

        if (profilesToCollect.contains(ProfileName.STATE)) {
            totalListener++;
        }

        if (profilesToCollect.contains(ProfileName.ERROR)) {
            totalListener++;
        }

        if (profilesToCollect.contains(ProfileName.INIT_PROGRESS)) {
            totalListener++;
        }

        if (profilesToCollect.contains(ProfileName.COORDINATING_NODE)
            || profilesToCollect.contains(ProfileName.SHINGLE_SIZE)
            || profilesToCollect.contains(ProfileName.TOTAL_SIZE_IN_BYTES)
            || profilesToCollect.contains(ProfileName.MODELS)
            || profilesToCollect.contains(ProfileName.ACTIVE_ENTITIES)) {
            totalListener++;
        }

        if (profilesToCollect.contains(ProfileName.TOTAL_ENTITIES)) {
            totalListener++;
        }

        MultiResponsesDelegateActionListener<DetectorProfile> delegateListener = new MultiResponsesDelegateActionListener<DetectorProfile>(
            listener,
            totalListener,
            "Fail to fetch profile for " + detectorId,
            false
        );

        prepareProfile(detectorId, delegateListener, profilesToCollect, totalListener);
    }

    private void prepareProfile(
        String detectorId,
        MultiResponsesDelegateActionListener<DetectorProfile> listener,
        Set<ProfileName> profilesToCollect,
        int totalListener
    ) {
        GetRequest getRequest = new GetRequest(ANOMALY_DETECTOR_JOB_INDEX, detectorId);
        client.get(getRequest, ActionListener.wrap(getResponse -> {
            if (getResponse != null && getResponse.isExists()) {
                try (
                    XContentParser parser = XContentType.JSON
                        .xContent()
                        .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, getResponse.getSourceAsString())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                    AnomalyDetectorJob job = AnomalyDetectorJob.parse(parser);
                    long enabledTimeMs = job.getEnabledTime().toEpochMilli();

                    if (profilesToCollect.contains(ProfileName.ERROR)) {
                        GetRequest getStateRequest = new GetRequest(DetectorInternalState.DETECTOR_STATE_INDEX, detectorId);
                        client.get(getStateRequest, onGetDetectorState(listener, detectorId, enabledTimeMs));
                    }

                    GetRequest getDetectorRequest = new GetRequest(ANOMALY_DETECTORS_INDEX, detectorId);
                    client.get(getDetectorRequest, ActionListener.wrap(getDetectorResponse -> {
                        if (getDetectorResponse != null && getDetectorResponse.isExists()) {
                            try (
                                XContentParser xContentParser = XContentType.JSON
                                    .xContent()
                                    .createParser(
                                        xContentRegistry,
                                        LoggingDeprecationHandler.INSTANCE,
                                        getDetectorResponse.getSourceAsString()
                                    )
                            ) {
                                ensureExpectedToken(
                                    XContentParser.Token.START_OBJECT,
                                    xContentParser.nextToken(),
                                    xContentParser::getTokenLocation
                                );
                                AnomalyDetector detector = AnomalyDetector.parse(xContentParser, detectorId);
                                boolean isHCDetector = isHCDetector(detector);
                                if (profilesToCollect.contains(ProfileName.TOTAL_ENTITIES)) {
                                    if (isHCDetector) {
                                        profileEntityStats(listener, detector);
                                    } else {
                                        listener.compareAndSetMaxResponseCount(totalListener, totalListener - 1);
                                    }
                                }
                                if (!isHCDetector
                                    && (profilesToCollect.contains(ProfileName.STATE)
                                        || profilesToCollect.contains(ProfileName.INIT_PROGRESS))) {
                                    profileStateRelated(detector, listener, job.isEnabled(), profilesToCollect);
                                }

                                if ((profilesToCollect.contains(ProfileName.COORDINATING_NODE)
                                    || profilesToCollect.contains(ProfileName.SHINGLE_SIZE)
                                    || profilesToCollect.contains(ProfileName.TOTAL_SIZE_IN_BYTES)
                                    || profilesToCollect.contains(ProfileName.MODELS)
                                    || profilesToCollect.contains(ProfileName.ACTIVE_ENTITIES))
                                    && (profilesToCollect.contains(ProfileName.INIT_PROGRESS) && isHCDetector)) {
                                    listener.compareAndSetMaxResponseCount(totalListener, totalListener - 1);
                                }

                                if (profilesToCollect.contains(ProfileName.COORDINATING_NODE)
                                    || profilesToCollect.contains(ProfileName.SHINGLE_SIZE)
                                    || profilesToCollect.contains(ProfileName.TOTAL_SIZE_IN_BYTES)
                                    || profilesToCollect.contains(ProfileName.MODELS)
                                    || profilesToCollect.contains(ProfileName.ACTIVE_ENTITIES)
                                    || (profilesToCollect.contains(ProfileName.INIT_PROGRESS) && isHCDetector)) {
                                    profileModels(detector, profilesToCollect, job.isEnabled(), listener);
                                }

                            } catch (Exception t) {
                                logger.error("Fail to parse detector {}", detectorId);
                                listener.failImmediately(FAIL_TO_FIND_DETECTOR_MSG + detectorId, t);
                            }
                        } else {
                            listener.failImmediately(FAIL_TO_FIND_DETECTOR_MSG + detectorId);
                        }
                    }, exception -> { listener.failImmediately(FAIL_TO_FIND_DETECTOR_MSG + detectorId, exception); }));
                } catch (IOException | XContentParseException | NullPointerException e) {
                    logger.error(e);
                    listener.failImmediately(FAIL_TO_GET_PROFILE_MSG, e);
                }
            } else {
                GetRequest getDetectorRequest = new GetRequest(ANOMALY_DETECTORS_INDEX, detectorId);
                client.get(getDetectorRequest, onGetDetectorForPrepare(listener, detectorId, profilesToCollect));
            }
        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                logger.info(exception.getMessage());
                GetRequest getDetectorRequest = new GetRequest(ANOMALY_DETECTORS_INDEX, detectorId);
                client.get(getDetectorRequest, onGetDetectorForPrepare(listener, detectorId, profilesToCollect));
            } else {
                logger.error(FAIL_TO_GET_PROFILE_MSG + detectorId);
                listener.onFailure(exception);
            }
        }));
    }

    /**
     * Get profile info of specific entity.
     *
     * @param detectorId detector identifier
     * @param entityValue entity value
     * @param listener action listener to handle exception and process entity profile response
     */
    public void profileEntity(String detectorId, String entityValue, ActionListener<EntityProfile> listener) {
        GetRequest getDetectorRequest = new GetRequest(ANOMALY_DETECTORS_INDEX, detectorId);
        client.get(getDetectorRequest, ActionListener.wrap(getResponse -> {
            if (getResponse != null && getResponse.isExists()) {
                try (
                    XContentParser parser = XContentType.JSON
                        .xContent()
                        .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, getResponse.getSourceAsString())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                    AnomalyDetector detector = AnomalyDetector.parse(parser, detectorId);
                    List<String> categoryField = detector.getCategoryField();
                    if (categoryField == null || categoryField.size() == 0) {
                        listener.onFailure(new InvalidParameterException("This is not a high cardinality detector"));
                    } else if (categoryField.size() > CATEGORY_FIELD_LIMIT) {
                        listener
                            .onFailure(new InvalidParameterException("We don't support more category fields than " + CATEGORY_FIELD_LIMIT));
                    } else {
                        EntityProfileRequest request = new EntityProfileRequest(detectorId, entityValue);
                        client
                            .execute(
                                EntityProfileAction.INSTANCE,
                                request,
                                ActionListener
                                    .wrap(
                                        r -> { listener.onResponse(new EntityProfile(categoryField.get(0), entityValue, r.isActive())); },
                                        e -> { listener.onFailure(e); }
                                    )
                            );
                    }
                } catch (Exception t) {
                    listener.onFailure(t);
                }
            } else {
                listener.onFailure(new InvalidParameterException(FAIL_TO_FIND_DETECTOR_MSG + detectorId));
            }
        }, exception -> { listener.onFailure(exception); }));
    }

    private void profileEntityStats(MultiResponsesDelegateActionListener<DetectorProfile> listener, AnomalyDetector detector) {
        List<String> categoryField = detector.getCategoryField();
        if (categoryField == null || categoryField.size() != 1) {
            listener.onResponse(new DetectorProfile.Builder().build());
        } else {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            CardinalityAggregationBuilder aggBuilder = new CardinalityAggregationBuilder(CommonName.TOTAL_ENTITIES);
            aggBuilder.field(categoryField.get(0));
            searchSourceBuilder.aggregation(aggBuilder);

            SearchRequest request = new SearchRequest(detector.getIndices().toArray(new String[0]), searchSourceBuilder);
            client.search(request, ActionListener.wrap(searchResponse -> {
                Map<String, Aggregation> aggMap = searchResponse.getAggregations().asMap();
                InternalCardinality totalEntities = (InternalCardinality) aggMap.get(CommonName.TOTAL_ENTITIES);
                long value = totalEntities.getValue();
                DetectorProfile.Builder profileBuilder = new DetectorProfile.Builder();
                DetectorProfile profile = profileBuilder.totalEntities(value).build();
                listener.onResponse(profile);
            }, searchException -> { listener.failImmediately(FAIL_TO_GET_TOTAL_ENTITIES + detector.getDetectorId()); }));
        }
    }

    private ActionListener<GetResponse> onGetDetectorForPrepare(
        MultiResponsesDelegateActionListener<DetectorProfile> listener,
        String detectorId,
        Set<ProfileName> profiles
    ) {
        return ActionListener.wrap(getResponse -> {
            if (getResponse != null && getResponse.isExists()) {
                DetectorProfile.Builder profileBuilder = new DetectorProfile.Builder();
                if (profiles.contains(ProfileName.STATE)) {
                    profileBuilder.state(DetectorState.DISABLED);
                }
                listener.respondImmediately(profileBuilder.build());
            } else {
                listener.failImmediately(FAIL_TO_FIND_DETECTOR_MSG + detectorId);
            }
        }, exception -> { listener.failImmediately(FAIL_TO_FIND_DETECTOR_MSG + detectorId, exception); });
    }

    /**
     * We expect three kinds of states:
     *  -Disabled: if get ad job api says the job is disabled;
     *  -Init: if rcf model's total updates is less than required
     *  -Running: if neither of the above applies and no exceptions.
     * @param detector anomaly detector
     * @param listener listener to process the returned state or exception
     * @param enabled whether the detector job is enabled or not
     * @param profilesToCollect target profiles to fetch
     */
    private void profileStateRelated(
        AnomalyDetector detector,
        MultiResponsesDelegateActionListener<DetectorProfile> listener,
        boolean enabled,
        Set<ProfileName> profilesToCollect
    ) {
        if (enabled) {
            RCFPollingRequest request = new RCFPollingRequest(detector.getDetectorId());
            client.execute(RCFPollingAction.INSTANCE, request, onPollRCFUpdates(detector, profilesToCollect, listener));
        } else {
            if (profilesToCollect.contains(ProfileName.STATE)) {
                listener.onResponse(new DetectorProfile.Builder().state(DetectorState.DISABLED).build());
            }
            if (profilesToCollect.contains(ProfileName.INIT_PROGRESS)) {
                listener.onResponse(new DetectorProfile.Builder().build());
            }
        }
    }

    /**
     * Action listener for a detector in running or init state
     * @param listener listener to consolidate results and return a final response
     * @param detectorId detector id
     * @param enabledTimeMs AD job enabled time
     * @return the listener for a detector in disabled state
     */
    private ActionListener<GetResponse> onGetDetectorState(
        MultiResponsesDelegateActionListener<DetectorProfile> listener,
        String detectorId,
        long enabledTimeMs
    ) {
        return ActionListener.wrap(getResponse -> {
            DetectorProfile.Builder profileBuilder = new DetectorProfile.Builder();
            if (getResponse != null && getResponse.isExists()) {
                try (
                    XContentParser parser = XContentType.JSON
                        .xContent()
                        .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, getResponse.getSourceAsString())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                    DetectorInternalState detectorState = DetectorInternalState.parse(parser);
                    long lastUpdateTimeMs = detectorState.getLastUpdateTime().toEpochMilli();

                    // if state index hasn't been updated, we should not use the error field
                    // For example, before a detector is enabled, if the error message contains
                    // the phrase "stopped due to blah", we should not show this when the detector
                    // is enabled.
                    if (lastUpdateTimeMs > enabledTimeMs && detectorState.getError() != null) {
                        profileBuilder.error(detectorState.getError());
                    }

                    listener.onResponse(profileBuilder.build());

                } catch (IOException | XContentParseException | NullPointerException e) {
                    logger.error(e);
                    listener.failImmediately(FAIL_TO_GET_PROFILE_MSG, e);
                }
            } else {
                // detector state for this detector does not exist
                listener.onResponse(profileBuilder.build());
            }
        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                // detector state index is not created yet
                listener.onResponse(new DetectorProfile.Builder().build());
            } else {
                logger.error("Fail to find any detector info for detector {}", detectorId);
                listener.onFailure(exception);
            }
        });
    }

    private InitProgressProfile computeInitProgressProfile(long totalUpdates, long intervalMins) {
        float percent = Math.min((100.0f * totalUpdates) / requiredSamples, 100.0f);
        int neededPoints = (int) (requiredSamples - totalUpdates);
        return new InitProgressProfile(
            // rounding: 93.456 => 93%, 93.556 => 94%
            String.format("%.0f%%", percent),
            intervalMins * neededPoints,
            neededPoints
        );
    }

    private void profileModels(
        AnomalyDetector detector,
        Set<ProfileName> profiles,
        boolean enabled,
        MultiResponsesDelegateActionListener<DetectorProfile> listener
    ) {
        DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
        ProfileRequest profileRequest = new ProfileRequest(detector.getDetectorId(), profiles, dataNodes);
        client.execute(ProfileAction.INSTANCE, profileRequest, onModelResponse(detector, profiles, enabled, listener));
    }

    private boolean isHCDetector(AnomalyDetector detector) {
        return detector != null && detector.getCategoryField() != null && detector.getCategoryField().size() > 0;
    }

    private ActionListener<ProfileResponse> onModelResponse(
        AnomalyDetector detector,
        Set<ProfileName> profiles,
        boolean enabled,
        MultiResponsesDelegateActionListener<DetectorProfile> listener
    ) {
        boolean isHCDetector = isHCDetector(detector);
        return ActionListener.wrap(profileResponse -> {
            DetectorProfile.Builder profile = new DetectorProfile.Builder();
            if (profiles.contains(ProfileName.COORDINATING_NODE)) {
                profile.coordinatingNode(profileResponse.getCoordinatingNode());
            }
            if (profiles.contains(ProfileName.SHINGLE_SIZE)) {
                profile.shingleSize(profileResponse.getShingleSize());
            }
            if (profiles.contains(ProfileName.TOTAL_SIZE_IN_BYTES)) {
                profile.totalSizeInBytes(profileResponse.getTotalSizeInBytes());
            }
            if (profiles.contains(ProfileName.MODELS)) {
                profile.modelProfile(profileResponse.getModelProfile());
            }
            if (isHCDetector && profiles.contains(ProfileName.ACTIVE_ENTITIES)) {
                profile.activeEntities(profileResponse.getActiveEntities());
            }
            if (isHCDetector && profiles.contains(ProfileName.INIT_PROGRESS)) {
                long totalUpdates = profileResponse.getTotalUpdates();
                long intervalMins = totalUpdates == 0
                    ? 0
                    : ((IntervalTimeConfiguration) detector.getDetectionInterval()).toDuration().toMinutes();
                InitProgressProfile initProgress = computeInitProgressProfile(totalUpdates, intervalMins);
                profile.initProgress(initProgress);
            }
            if (isHCDetector && profiles.contains(ProfileName.STATE)) {
                DetectorState state;
                if (enabled) {
                    state = profileResponse.getTotalUpdates() < requiredSamples ? DetectorState.INIT : DetectorState.RUNNING;
                } else {
                    state = DetectorState.DISABLED;
                }
                listener.onResponse(new DetectorProfile.Builder().state(state).build());
            }

            listener.onResponse(profile.build());
        }, listener::onFailure);
    }

    /**
     * Listener for polling rcf updates through transport messaging
     * @param detector anomaly detector
     * @param profilesToCollect profiles to collect like state
     * @param listener delegate listener
     * @return Listener for polling rcf updates through transport messaging
     */
    private ActionListener<RCFPollingResponse> onPollRCFUpdates(
        AnomalyDetector detector,
        Set<ProfileName> profilesToCollect,
        MultiResponsesDelegateActionListener<DetectorProfile> listener
    ) {
        return ActionListener.wrap(rcfPollResponse -> {
            long totalUpdates = rcfPollResponse.getTotalUpdates();
            if (totalUpdates < requiredSamples) {
                processInitResponse(detector, profilesToCollect, listener, totalUpdates, false);
            } else {
                if (profilesToCollect.contains(ProfileName.STATE)) {
                    listener.onResponse(new DetectorProfile.Builder().state(DetectorState.RUNNING).build());
                }

                if (profilesToCollect.contains(ProfileName.INIT_PROGRESS)) {
                    InitProgressProfile initProgress = new InitProgressProfile("100%", 0, 0);
                    listener.onResponse(new DetectorProfile.Builder().initProgress(initProgress).build());
                }
            }
        }, exception -> {
            // we will get an AnomalyDetectionException wrapping the real exception inside
            Throwable cause = Throwables.getRootCause(exception);

            // exception can be a RemoteTransportException
            Exception causeException = (Exception) cause;
            if (ExceptionUtil
                .isException(causeException, ResourceNotFoundException.class, ExceptionUtil.RESOURCE_NOT_FOUND_EXCEPTION_NAME_UNDERSCORE)
                || (causeException instanceof IndexNotFoundException
                    && causeException.getMessage().contains(CommonName.CHECKPOINT_INDEX_NAME))) {
                // cannot find checkpoint
                // We don't want to show the estimated time remaining to initialize
                // a detector before cold start finishes, where the actual
                // initialization time may be much shorter if sufficient historical
                // data exists.
                processInitResponse(detector, profilesToCollect, listener, 0L, true);
            } else {
                logger
                    .error(
                        new ParameterizedMessage("Fail to get init progress through messaging for {}", detector.getDetectorId()),
                        exception
                    );
                listener.failImmediately(FAIL_TO_GET_PROFILE_MSG + detector.getDetectorId(), exception);
            }
        });
    }

    private void processInitResponse(
        AnomalyDetector detector,
        Set<ProfileName> profilesToCollect,
        MultiResponsesDelegateActionListener<DetectorProfile> listener,
        long totalUpdates,
        boolean hideMinutesLeft
    ) {
        if (profilesToCollect.contains(ProfileName.STATE)) {
            listener.onResponse(new DetectorProfile.Builder().state(DetectorState.INIT).build());
        }

        if (profilesToCollect.contains(ProfileName.INIT_PROGRESS)) {
            if (hideMinutesLeft) {
                InitProgressProfile initProgress = computeInitProgressProfile(totalUpdates, 0);
                listener.onResponse(new DetectorProfile.Builder().initProgress(initProgress).build());
            } else {
                long intervalMins = ((IntervalTimeConfiguration) detector.getDetectionInterval()).toDuration().toMinutes();
                InitProgressProfile initProgress = computeInitProgressProfile(totalUpdates, intervalMins);

                listener.onResponse(new DetectorProfile.Builder().initProgress(initProgress).build());
            }
        }
    }
}
