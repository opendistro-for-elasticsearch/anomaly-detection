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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalCardinality;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.amazon.opendistroforelasticsearch.ad.common.exception.ResourceNotFoundException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorInternalState;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfile;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfileName;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorState;
import com.amazon.opendistroforelasticsearch.ad.model.InitProgressProfile;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.transport.ProfileAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ProfileRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.ProfileResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.RCFPollingAction;
import com.amazon.opendistroforelasticsearch.ad.transport.RCFPollingRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.RCFPollingResponse;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;
import com.amazon.opendistroforelasticsearch.ad.util.ExceptionUtil;
import com.amazon.opendistroforelasticsearch.ad.util.MultiResponsesDelegateActionListener;

public class AnomalyDetectorProfileRunner extends AbstractProfileRunner {
    private final Logger logger = LogManager.getLogger(AnomalyDetectorProfileRunner.class);
    private Client client;
    private NamedXContentRegistry xContentRegistry;
    private DiscoveryNodeFilterer nodeFilter;

    public AnomalyDetectorProfileRunner(
        Client client,
        NamedXContentRegistry xContentRegistry,
        DiscoveryNodeFilterer nodeFilter,
        long requiredSamples
    ) {
        super(requiredSamples);
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.nodeFilter = nodeFilter;
        if (requiredSamples <= 0) {
            throw new IllegalArgumentException("required samples should be a positive number, but was " + requiredSamples);
        }
    }

    public void profile(String detectorId, ActionListener<DetectorProfile> listener, Set<DetectorProfileName> profilesToCollect) {

        if (profilesToCollect.isEmpty()) {
            listener.onFailure(new InvalidParameterException(CommonErrorMessages.EMPTY_PROFILES_COLLECT));
            return;
        }

        calculateTotalResponsesToWait(detectorId, profilesToCollect, listener);
    }

    private void calculateTotalResponsesToWait(
        String detectorId,
        Set<DetectorProfileName> profilesToCollect,
        ActionListener<DetectorProfile> listener
    ) {
        GetRequest getDetectorRequest = new GetRequest(ANOMALY_DETECTORS_INDEX, detectorId);
        client.get(getDetectorRequest, ActionListener.wrap(getDetectorResponse -> {
            if (getDetectorResponse != null && getDetectorResponse.isExists()) {
                try (
                    XContentParser xContentParser = XContentType.JSON
                        .xContent()
                        .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, getDetectorResponse.getSourceAsString())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, xContentParser.nextToken(), xContentParser);
                    AnomalyDetector detector = AnomalyDetector.parse(xContentParser, detectorId);
                    boolean isMultiEntityDetector = detector.isMultientityDetector();

                    int totalResponsesToWait = 0;

                    if (profilesToCollect.contains(DetectorProfileName.ERROR)) {
                        totalResponsesToWait++;
                    }

                    // total number of listeners we need to define. Needed by MultiResponsesDelegateActionListener to decide
                    // when to consolidate results and return to users
                    if (isMultiEntityDetector) {
                        if (profilesToCollect.contains(DetectorProfileName.TOTAL_ENTITIES)) {
                            totalResponsesToWait++;
                        }
                        if (profilesToCollect.contains(DetectorProfileName.COORDINATING_NODE)
                            || profilesToCollect.contains(DetectorProfileName.SHINGLE_SIZE)
                            || profilesToCollect.contains(DetectorProfileName.TOTAL_SIZE_IN_BYTES)
                            || profilesToCollect.contains(DetectorProfileName.MODELS)
                            || profilesToCollect.contains(DetectorProfileName.ACTIVE_ENTITIES)
                            || profilesToCollect.contains(DetectorProfileName.INIT_PROGRESS)
                            || profilesToCollect.contains(DetectorProfileName.STATE)) {
                            totalResponsesToWait++;
                        }
                    } else {
                        if (profilesToCollect.contains(DetectorProfileName.STATE)
                            || profilesToCollect.contains(DetectorProfileName.INIT_PROGRESS)) {
                            totalResponsesToWait++;
                        }
                        if (profilesToCollect.contains(DetectorProfileName.COORDINATING_NODE)
                            || profilesToCollect.contains(DetectorProfileName.SHINGLE_SIZE)
                            || profilesToCollect.contains(DetectorProfileName.TOTAL_SIZE_IN_BYTES)
                            || profilesToCollect.contains(DetectorProfileName.MODELS)) {
                            totalResponsesToWait++;
                        }
                    }

                    MultiResponsesDelegateActionListener<DetectorProfile> delegateListener =
                        new MultiResponsesDelegateActionListener<DetectorProfile>(
                            listener,
                            totalResponsesToWait,
                            "Fail to fetch profile for " + detectorId,
                            false
                        );

                    prepareProfile(detector, delegateListener, profilesToCollect);
                } catch (Exception e) {
                    listener.onFailure(new RuntimeException(CommonErrorMessages.FAIL_TO_FIND_DETECTOR_MSG + detectorId, e));
                }
            } else {
                listener.onFailure(new RuntimeException(CommonErrorMessages.FAIL_TO_FIND_DETECTOR_MSG + detectorId));
            }
        }, exception -> listener.onFailure(new RuntimeException(CommonErrorMessages.FAIL_TO_FIND_DETECTOR_MSG + detectorId, exception))));
    }

    private void prepareProfile(
        AnomalyDetector detector,
        MultiResponsesDelegateActionListener<DetectorProfile> listener,
        Set<DetectorProfileName> profilesToCollect
    ) {
        String detectorId = detector.getDetectorId();
        GetRequest getRequest = new GetRequest(ANOMALY_DETECTOR_JOB_INDEX, detectorId);
        client.get(getRequest, ActionListener.wrap(getResponse -> {
            if (getResponse != null && getResponse.isExists()) {
                try (
                    XContentParser parser = XContentType.JSON
                        .xContent()
                        .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, getResponse.getSourceAsString())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    AnomalyDetectorJob job = AnomalyDetectorJob.parse(parser);
                    long enabledTimeMs = job.getEnabledTime().toEpochMilli();

                    if (profilesToCollect.contains(DetectorProfileName.ERROR)) {
                        GetRequest getStateRequest = new GetRequest(DetectorInternalState.DETECTOR_STATE_INDEX, detectorId);
                        client.get(getStateRequest, onGetDetectorState(listener, detectorId, enabledTimeMs));
                    }

                    boolean isMultiEntityDetector = detector.isMultientityDetector();

                    // total number of listeners we need to define. Needed by MultiResponsesDelegateActionListener to decide
                    // when to consolidate results and return to users
                    if (isMultiEntityDetector) {
                        if (profilesToCollect.contains(DetectorProfileName.TOTAL_ENTITIES)) {
                            profileEntityStats(listener, detector);
                        }
                        if (profilesToCollect.contains(DetectorProfileName.COORDINATING_NODE)
                            || profilesToCollect.contains(DetectorProfileName.SHINGLE_SIZE)
                            || profilesToCollect.contains(DetectorProfileName.TOTAL_SIZE_IN_BYTES)
                            || profilesToCollect.contains(DetectorProfileName.MODELS)
                            || profilesToCollect.contains(DetectorProfileName.ACTIVE_ENTITIES)
                            || profilesToCollect.contains(DetectorProfileName.INIT_PROGRESS)
                            || profilesToCollect.contains(DetectorProfileName.STATE)) {
                            profileModels(detector, profilesToCollect, job, true, listener);
                        }
                    } else {
                        if (profilesToCollect.contains(DetectorProfileName.STATE)
                            || profilesToCollect.contains(DetectorProfileName.INIT_PROGRESS)) {
                            profileStateRelated(detector, listener, job.isEnabled(), profilesToCollect);
                        }
                        if (profilesToCollect.contains(DetectorProfileName.COORDINATING_NODE)
                            || profilesToCollect.contains(DetectorProfileName.SHINGLE_SIZE)
                            || profilesToCollect.contains(DetectorProfileName.TOTAL_SIZE_IN_BYTES)
                            || profilesToCollect.contains(DetectorProfileName.MODELS)) {
                            profileModels(detector, profilesToCollect, job, false, listener);
                        }
                    }

                } catch (IOException | XContentParseException | NullPointerException e) {
                    logger.error(e);
                    listener.failImmediately(CommonErrorMessages.FAIL_TO_GET_PROFILE_MSG, e);
                }
            } else {
                onGetDetectorForPrepare(listener, profilesToCollect);
            }
        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                logger.info(exception.getMessage());
                onGetDetectorForPrepare(listener, profilesToCollect);
            } else {
                logger.error(CommonErrorMessages.FAIL_TO_GET_PROFILE_MSG + detectorId);
                listener.onFailure(exception);
            }
        }));
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
            }, searchException -> { listener.failImmediately(CommonErrorMessages.FAIL_TO_GET_TOTAL_ENTITIES + detector.getDetectorId()); })
            );
        }
    }

    private void onGetDetectorForPrepare(
        MultiResponsesDelegateActionListener<DetectorProfile> listener,
        Set<DetectorProfileName> profiles
    ) {
        DetectorProfile.Builder profileBuilder = new DetectorProfile.Builder();
        if (profiles.contains(DetectorProfileName.STATE)) {
            profileBuilder.state(DetectorState.DISABLED);
        }
        listener.respondImmediately(profileBuilder.build());
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
        Set<DetectorProfileName> profilesToCollect
    ) {
        if (enabled) {
            RCFPollingRequest request = new RCFPollingRequest(detector.getDetectorId());
            client.execute(RCFPollingAction.INSTANCE, request, onPollRCFUpdates(detector, profilesToCollect, listener));
        } else {
            DetectorProfile.Builder builder = new DetectorProfile.Builder();
            if (profilesToCollect.contains(DetectorProfileName.STATE)) {
                builder.state(DetectorState.DISABLED);
            }
            listener.onResponse(builder.build());
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
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
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
                    listener.failImmediately(CommonErrorMessages.FAIL_TO_GET_PROFILE_MSG, e);
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

    private void profileModels(
        AnomalyDetector detector,
        Set<DetectorProfileName> profiles,
        AnomalyDetectorJob job,
        boolean forMultiEntityDetector,
        MultiResponsesDelegateActionListener<DetectorProfile> listener
    ) {
        DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
        ProfileRequest profileRequest = new ProfileRequest(detector.getDetectorId(), profiles, forMultiEntityDetector, dataNodes);
        client.execute(ProfileAction.INSTANCE, profileRequest, onModelResponse(detector, profiles, job, listener));
    }

    private ActionListener<ProfileResponse> onModelResponse(
        AnomalyDetector detector,
        Set<DetectorProfileName> profilesToCollect,
        AnomalyDetectorJob job,
        MultiResponsesDelegateActionListener<DetectorProfile> listener
    ) {
        boolean isMultientityDetector = detector.isMultientityDetector();
        return ActionListener.wrap(profileResponse -> {
            DetectorProfile.Builder profile = new DetectorProfile.Builder();
            if (profilesToCollect.contains(DetectorProfileName.COORDINATING_NODE)) {
                profile.coordinatingNode(profileResponse.getCoordinatingNode());
            }
            if (profilesToCollect.contains(DetectorProfileName.SHINGLE_SIZE)) {
                profile.shingleSize(profileResponse.getShingleSize());
            }
            if (profilesToCollect.contains(DetectorProfileName.TOTAL_SIZE_IN_BYTES)) {
                profile.totalSizeInBytes(profileResponse.getTotalSizeInBytes());
            }
            if (profilesToCollect.contains(DetectorProfileName.MODELS)) {
                profile.modelProfile(profileResponse.getModelProfile());
            }
            if (isMultientityDetector && profilesToCollect.contains(DetectorProfileName.ACTIVE_ENTITIES)) {
                profile.activeEntities(profileResponse.getActiveEntities());
            }

            if (isMultientityDetector
                && (profilesToCollect.contains(DetectorProfileName.INIT_PROGRESS)
                    || profilesToCollect.contains(DetectorProfileName.STATE))) {
                profileMultiEntityDetectorStateRelated(job, profilesToCollect, profileResponse, profile, detector, listener);
            } else {
                listener.onResponse(profile.build());
            }
        }, listener::onFailure);
    }

    private void profileMultiEntityDetectorStateRelated(
        AnomalyDetectorJob job,
        Set<DetectorProfileName> profilesToCollect,
        ProfileResponse profileResponse,
        DetectorProfile.Builder profileBuilder,
        AnomalyDetector detector,
        MultiResponsesDelegateActionListener<DetectorProfile> listener
    ) {
        if (job.isEnabled()) {
            if (profileResponse.getTotalUpdates() < requiredSamples) {
                // need to double check since what ProfileResponse returns is the highest priority entity currently in memory, but
                // another entity might have already been initialized and sit somewhere else (in memory or on disk).
                confirmMultiEntityDetectorInitStatus(
                    detector,
                    job.getEnabledTime().toEpochMilli(),
                    profileBuilder,
                    profilesToCollect,
                    profileResponse.getTotalUpdates(),
                    listener
                );
            } else {
                createRunningStateAndInitProgress(profilesToCollect, profileBuilder);
                listener.onResponse(profileBuilder.build());
            }
        } else {
            if (profilesToCollect.contains(DetectorProfileName.STATE)) {
                profileBuilder.state(DetectorState.DISABLED);
            }
            listener.onResponse(profileBuilder.build());
        }
    }

    private void confirmMultiEntityDetectorInitStatus(
        AnomalyDetector detector,
        long enabledTime,
        DetectorProfile.Builder profile,
        Set<DetectorProfileName> profilesToCollect,
        long totalUpdates,
        MultiResponsesDelegateActionListener<DetectorProfile> listener
    ) {
        SearchRequest searchLatestResult = createInittedEverRequest(detector.getDetectorId(), enabledTime);
        client.search(searchLatestResult, onInittedEver(enabledTime, profile, profilesToCollect, detector, totalUpdates, listener));
    }

    private ActionListener<SearchResponse> onInittedEver(
        long lastUpdateTimeMs,
        DetectorProfile.Builder profileBuilder,
        Set<DetectorProfileName> profilesToCollect,
        AnomalyDetector detector,
        long totalUpdates,
        MultiResponsesDelegateActionListener<DetectorProfile> listener
    ) {
        return ActionListener.wrap(searchResponse -> {
            SearchHits hits = searchResponse.getHits();
            if (hits.getTotalHits().value == 0L) {
                processInitResponse(detector, profilesToCollect, totalUpdates, false, profileBuilder, listener);
            } else {
                createRunningStateAndInitProgress(profilesToCollect, profileBuilder);
            }
            listener.onResponse(profileBuilder.build());
        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                // anomaly result index is not created yet
                processInitResponse(detector, profilesToCollect, totalUpdates, false, profileBuilder, listener);
            } else {
                logger
                    .error(
                        "Fail to find any anomaly result with anomaly score larger than 0 after AD job enabled time for detector {}",
                        detector.getDetectorId()
                    );
                listener.failImmediately(new RuntimeException("Fail to find detector state: " + detector.getDetectorId(), exception));
            }
        });
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
        Set<DetectorProfileName> profilesToCollect,
        MultiResponsesDelegateActionListener<DetectorProfile> listener
    ) {
        return ActionListener.wrap(rcfPollResponse -> {
            long totalUpdates = rcfPollResponse.getTotalUpdates();
            if (totalUpdates < requiredSamples) {
                processInitResponse(detector, profilesToCollect, totalUpdates, false, new DetectorProfile.Builder(), listener);
            } else {
                DetectorProfile.Builder builder = new DetectorProfile.Builder();
                createRunningStateAndInitProgress(profilesToCollect, builder);
                listener.onResponse(builder.build());
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
                processInitResponse(detector, profilesToCollect, 0L, true, new DetectorProfile.Builder(), listener);
            } else {
                logger
                    .error(
                        new ParameterizedMessage("Fail to get init progress through messaging for {}", detector.getDetectorId()),
                        exception
                    );
                listener.failImmediately(CommonErrorMessages.FAIL_TO_GET_PROFILE_MSG + detector.getDetectorId(), exception);
            }
        });
    }

    private void createRunningStateAndInitProgress(Set<DetectorProfileName> profilesToCollect, DetectorProfile.Builder builder) {
        if (profilesToCollect.contains(DetectorProfileName.STATE)) {
            builder.state(DetectorState.RUNNING).build();
        }

        if (profilesToCollect.contains(DetectorProfileName.INIT_PROGRESS)) {
            InitProgressProfile initProgress = new InitProgressProfile("100%", 0, 0);
            builder.initProgress(initProgress);
        }
    }

    private void processInitResponse(
        AnomalyDetector detector,
        Set<DetectorProfileName> profilesToCollect,
        long totalUpdates,
        boolean hideMinutesLeft,
        DetectorProfile.Builder builder,
        MultiResponsesDelegateActionListener<DetectorProfile> listener
    ) {
        if (profilesToCollect.contains(DetectorProfileName.STATE)) {
            builder.state(DetectorState.INIT);
        }

        if (profilesToCollect.contains(DetectorProfileName.INIT_PROGRESS)) {
            if (hideMinutesLeft) {
                InitProgressProfile initProgress = computeInitProgressProfile(totalUpdates, 0);
                builder.initProgress(initProgress);
            } else {
                long intervalMins = ((IntervalTimeConfiguration) detector.getDetectionInterval()).toDuration().toMinutes();
                InitProgressProfile initProgress = computeInitProgressProfile(totalUpdates, intervalMins);

                builder.initProgress(initProgress);
            }
        }

        listener.onResponse(builder.build());
    }

    /**
     * Create search request to check if we have at least 1 anomaly score larger than 0 after AD job enabled time
     * @param detectorId detector id
     * @param enabledTime the time when AD job is enabled in milliseconds
     * @return the search request
     */
    private SearchRequest createInittedEverRequest(String detectorId, long enabledTime) {
        BoolQueryBuilder filterQuery = new BoolQueryBuilder();
        filterQuery.filter(QueryBuilders.termQuery(AnomalyResult.DETECTOR_ID_FIELD, detectorId));
        filterQuery.filter(QueryBuilders.rangeQuery(AnomalyResult.EXECUTION_END_TIME_FIELD).gte(enabledTime));
        filterQuery.filter(QueryBuilders.rangeQuery(AnomalyResult.ANOMALY_SCORE_FIELD).gt(0));

        SearchSourceBuilder source = new SearchSourceBuilder().query(filterQuery).size(1);

        SearchRequest request = new SearchRequest(CommonName.ANOMALY_RESULT_INDEX_ALIAS);
        request.source(source);
        return request;
    }
}
