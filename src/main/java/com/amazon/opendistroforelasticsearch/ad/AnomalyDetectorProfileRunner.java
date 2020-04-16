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
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfile;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorState;
import com.amazon.opendistroforelasticsearch.ad.model.ProfileName;
import com.amazon.opendistroforelasticsearch.ad.util.MultiResponsesDelegateActionListener;

public class AnomalyDetectorProfileRunner {
    private final Logger logger = LogManager.getLogger(AnomalyDetectorProfileRunner.class);
    private Client client;
    private NamedXContentRegistry xContentRegistry;
    static String FAIL_TO_FIND_DETECTOR_MSG = "Fail to find detector with id: ";
    static String FAIL_TO_GET_PROFILE_MSG = "Fail to get profile for detector ";

    public AnomalyDetectorProfileRunner(Client client, NamedXContentRegistry xContentRegistry) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
    }

    public void profile(String detectorId, ActionListener<DetectorProfile> listener, Set<ProfileName> profiles) {

        if (profiles.isEmpty()) {
            listener.onFailure(new RuntimeException("Unsupported profile types."));
            return;
        }

        MultiResponsesDelegateActionListener<DetectorProfile> delegateListener = new MultiResponsesDelegateActionListener<DetectorProfile>(
            listener,
            profiles.size(),
            "Fail to fetch profile for " + detectorId
        );

        prepareProfile(detectorId, delegateListener, profiles);
    }

    private void prepareProfile(
        String detectorId,
        MultiResponsesDelegateActionListener<DetectorProfile> listener,
        Set<ProfileName> profiles
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

                    if (profiles.contains(ProfileName.STATE)) {
                        profileState(detectorId, enabledTimeMs, listener, job.isEnabled());
                    }
                    if (profiles.contains(ProfileName.ERROR)) {
                        profileError(detectorId, enabledTimeMs, listener);
                    }
                } catch (IOException | XContentParseException | NullPointerException e) {
                    logger.error(e);
                    listener.failImmediately(FAIL_TO_GET_PROFILE_MSG, e);
                }
            } else {
                GetRequest getDetectorRequest = new GetRequest(ANOMALY_DETECTORS_INDEX, detectorId);
                client.get(getDetectorRequest, onGetDetectorResponse(listener, detectorId, profiles));
            }
        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                logger.info(exception.getMessage());
                GetRequest getDetectorRequest = new GetRequest(ANOMALY_DETECTORS_INDEX, detectorId);
                client.get(getDetectorRequest, onGetDetectorResponse(listener, detectorId, profiles));
            } else {
                logger.error(FAIL_TO_GET_PROFILE_MSG + detectorId);
                listener.onFailure(exception);
            }
        }));
    }

    private ActionListener<GetResponse> onGetDetectorResponse(
        MultiResponsesDelegateActionListener<DetectorProfile> listener,
        String detectorId,
        Set<ProfileName> profiles
    ) {
        return ActionListener.wrap(getResponse -> {
            if (getResponse != null && getResponse.isExists()) {
                DetectorProfile profile = new DetectorProfile();
                if (profiles.contains(ProfileName.STATE)) {
                    profile.setState(DetectorState.DISABLED);
                }
                listener.respondImmediately(profile);
            } else {
                listener.failImmediately(FAIL_TO_FIND_DETECTOR_MSG + detectorId);
            }
        }, exception -> { listener.failImmediately(FAIL_TO_FIND_DETECTOR_MSG + detectorId, exception); });
    }

    /**
     * We expect three kinds of states:
     *  -Disabled: if get ad job api says the job is disabled;
     *  -Init: if anomaly score after the last update time of the detector is larger than 0
     *  -Running: if neither of the above applies and no exceptions.
     * @param detectorId detector id
     * @param enabledTime the time when AD job is enabled in milliseconds
     * @param listener listener to process the returned state or exception
     * @param enabled whether the detector job is enabled or not
     */
    private void profileState(
        String detectorId,
        long enabledTime,
        MultiResponsesDelegateActionListener<DetectorProfile> listener,
        boolean enabled
    ) {
        if (enabled) {
            SearchRequest searchLatestResult = createInittedEverRequest(detectorId, enabledTime);
            client.search(searchLatestResult, onInittedEver(listener, detectorId, enabledTime));
        } else {
            DetectorProfile profile = new DetectorProfile();
            profile.setState(DetectorState.DISABLED);
            listener.onResponse(profile);
        }
    }

    private ActionListener<SearchResponse> onInittedEver(
        MultiResponsesDelegateActionListener<DetectorProfile> listener,
        String detectorId,
        long lastUpdateTimeMs
    ) {
        return ActionListener.wrap(searchResponse -> {
            SearchHits hits = searchResponse.getHits();
            DetectorProfile profile = new DetectorProfile();
            if (hits.getTotalHits().value == 0L) {
                profile.setState(DetectorState.INIT);
            } else {
                profile.setState(DetectorState.RUNNING);
            }

            listener.onResponse(profile);

        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                DetectorProfile profile = new DetectorProfile();
                // anomaly result index is not created yet
                profile.setState(DetectorState.INIT);
                listener.onResponse(profile);
            } else {
                logger
                    .error(
                        "Fail to find any anomaly result with anomaly score larger than 0 after AD job enabled time for detector {}",
                        detectorId
                    );
                listener.onFailure(new RuntimeException("Fail to find detector state: " + detectorId, exception));
            }
        });
    }

    /**
     * Error is populated if error of the latest anomaly result is not empty.
     * @param detectorId detector id
     * @param enabledTime the time when AD job is enabled in milliseconds
     * @param listener listener to process the returned error or exception
     */
    private void profileError(String detectorId, long enabledTime, MultiResponsesDelegateActionListener<DetectorProfile> listener) {
        SearchRequest searchLatestResult = createLatestAnomalyResultRequest(detectorId, enabledTime);
        client.search(searchLatestResult, onGetLatestAnomalyResult(listener, detectorId));
    }

    private ActionListener<SearchResponse> onGetLatestAnomalyResult(ActionListener<DetectorProfile> listener, String detectorId) {
        return ActionListener.wrap(searchResponse -> {
            SearchHits hits = searchResponse.getHits();
            if (hits.getTotalHits().value == 0L) {
                listener.onResponse(new DetectorProfile());
            } else {
                SearchHit hit = hits.getAt(0);

                try (
                    XContentParser parser = XContentType.JSON
                        .xContent()
                        .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, hit.getSourceAsString())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                    AnomalyResult result = parser.namedObject(AnomalyResult.class, AnomalyResult.PARSE_FIELD_NAME, null);

                    DetectorProfile profile = new DetectorProfile();
                    if (result.getError() != null) {
                        profile.setError(result.getError());
                    }
                    listener.onResponse(profile);
                } catch (IOException | XContentParseException | NullPointerException e) {
                    logger.error("Fail to parse anomaly result with " + hit.toString());
                    listener.onFailure(new RuntimeException("Fail to find detector error: " + detectorId, e));
                }
            }
        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                listener.onResponse(new DetectorProfile());
            } else {
                logger.error("Fail to find any anomaly result after AD job enabled time for detector {}", detectorId);
                listener.onFailure(new RuntimeException("Fail to find detector error: " + detectorId, exception));
            }
        });
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

        SearchRequest request = new SearchRequest(AnomalyResult.ANOMALY_RESULT_INDEX);
        request.source(source);
        return request;
    }

    /**
     * Create search request to get the latest anomaly result after AD job enabled time
     * @param detectorId detector id
     * @param enabledTime the time when AD job is enabled in milliseconds
     * @return the search request
     */
    private SearchRequest createLatestAnomalyResultRequest(String detectorId, long enabledTime) {
        BoolQueryBuilder filterQuery = new BoolQueryBuilder();
        filterQuery.filter(QueryBuilders.termQuery(AnomalyResult.DETECTOR_ID_FIELD, detectorId));
        filterQuery.filter(QueryBuilders.rangeQuery(AnomalyResult.EXECUTION_END_TIME_FIELD).gte(enabledTime));

        FieldSortBuilder sortQuery = new FieldSortBuilder(AnomalyResult.EXECUTION_END_TIME_FIELD).order(SortOrder.DESC);

        SearchSourceBuilder source = new SearchSourceBuilder().query(filterQuery).size(1).sort(sortQuery);

        SearchRequest request = new SearchRequest(AnomalyResult.ANOMALY_RESULT_INDEX);
        request.source(source);
        return request;
    }
}
