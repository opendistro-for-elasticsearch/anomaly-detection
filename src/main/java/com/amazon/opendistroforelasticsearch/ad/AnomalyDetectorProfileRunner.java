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

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfile;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorState;
import com.amazon.opendistroforelasticsearch.ad.model.ProfileName;
import com.amazon.opendistroforelasticsearch.ad.util.DelegateActionListener;

public class AnomalyDetectorProfileRunner {
    private final Logger logger = LogManager.getLogger(AnomalyDetectorProfileRunner.class);
    private Client client;
    private NamedXContentRegistry xContentRegistry;
    static String FAIL_TO_FIND_DETECTOR_MSG = "Fail to find detector with id: ";

    public AnomalyDetectorProfileRunner(Client client, NamedXContentRegistry xContentRegistry) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
    }

    public void profile(String detectorId, ActionListener<DetectorProfile> listener, Set<String> profiles) {
        DelegateActionListener<DetectorProfile> delegateListener = new DelegateActionListener<DetectorProfile>(
            listener,
            profiles.size(),
            "Fail to fetch profile for " + detectorId
        );

        if (profiles.isEmpty()) {
            listener.onFailure(new RuntimeException("Unsupported profile types."));
            return;
        }

        if (profiles.contains(ProfileName.STATE.getName()) || profiles.contains(ProfileName.ERROR.getName())) {
            prepareProfileStateNError(detectorId, delegateListener, profiles);
        }
    }

    private void prepareProfileStateNError(String detectorId, DelegateActionListener<DetectorProfile> listener, Set<String> profiles) {
        GetRequest getDetectorRequest = new GetRequest(ANOMALY_DETECTORS_INDEX, detectorId);
        client.get(getDetectorRequest, onGetDetectorResponse(listener, detectorId, profiles));
    }

    private ActionListener<GetResponse> onGetDetectorResponse(
        DelegateActionListener<DetectorProfile> listener,
        String detectorId,
        Set<String> profiles
    ) {
        return ActionListener.wrap(getResponse -> {
            if (getResponse != null && getResponse.isExists()) {
                try (
                    XContentParser parser = XContentType.JSON
                        .xContent()
                        .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, getResponse.getSourceAsString())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                    AnomalyDetector detector = parser.namedObject(AnomalyDetector.class, AnomalyDetector.PARSE_FIELD_NAME, null);
                    long lastUpdateTimeMs = detector.getLastUpdateTime().toEpochMilli();

                    if (profiles.contains(ProfileName.STATE.getName())) {
                        profileState(detectorId, lastUpdateTimeMs, listener);
                    }
                    if (profiles.contains(ProfileName.ERROR.getName())) {
                        profileError(detectorId, lastUpdateTimeMs, listener);
                    }

                } catch (IOException | XContentParseException | NullPointerException e) {
                    logger.error(e);
                    listener.failImmediately(new RuntimeException(FAIL_TO_FIND_DETECTOR_MSG + detectorId, e));
                }
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
     * @param lastUpdateTimeMs last update time of the detector in milliseconds
     * @param listener listener to process the returned state or exception
     */
    private void profileState(String detectorId, long lastUpdateTimeMs, DelegateActionListener<DetectorProfile> listener) {
        GetRequest getRequest = new GetRequest(ANOMALY_DETECTOR_JOB_INDEX, detectorId);
        client.get(getRequest, ActionListener.wrap(getResponse -> {
            if (getResponse.isExists()) {
                try (
                    XContentParser parser = XContentType.JSON
                        .xContent()
                        .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, getResponse.getSourceAsString())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                    AnomalyDetectorJob job = AnomalyDetectorJob.parse(parser);
                    if (job.isEnabled()) {
                        SearchRequest searchLatestResult = createInittedEverRequest(detectorId, lastUpdateTimeMs);
                        client.search(searchLatestResult, onInittedEver(listener, detectorId, lastUpdateTimeMs));
                    } else {
                        DetectorProfile profile = new DetectorProfile();
                        profile.setState(DetectorState.DISABLED);
                        listener.onResponse(profile);
                    }
                } catch (IOException | XContentParseException e) {
                    String error = "Fail to parse detector with id: " + detectorId;
                    logger.error(error);
                    listener.onFailure(new RuntimeException(error, e));
                }
            } else {
                DetectorProfile profile = new DetectorProfile();
                profile.setState(DetectorState.DISABLED);
                listener.onResponse(profile);
            }
        }, exception -> {
            logger.warn(exception);
            // detector job index does not exist
            if (exception instanceof IndexNotFoundException) {
                DetectorProfile profile = new DetectorProfile();
                profile.setState(DetectorState.DISABLED);
                listener.onResponse(profile);
            } else {
                logger.error("Fail to get detector state for " + detectorId);
                listener.onFailure(exception);
            }
        }));

    }

    private ActionListener<SearchResponse> onInittedEver(
        DelegateActionListener<DetectorProfile> listener,
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
                logger.error("Fail to find latest anomaly result of id: {}", detectorId);
                listener.onFailure(new RuntimeException("Fail to find detector state: " + detectorId, exception));
            }
        });
    }

    /**
     * Error is populated if error of the latest anomaly result is not empty.
     * @param detectorId detector id
     * @param lastUpdateTimeMs last update time of the detector in milliseconds
     * @param listener listener to process the returned error or exception
     */
    private void profileError(String detectorId, long lastUpdateTimeMs, DelegateActionListener<DetectorProfile> listener) {
        SearchRequest searchLatestResult = createLatestAnomalyResultRequest(detectorId, lastUpdateTimeMs);
        client.search(searchLatestResult, onGetLatestAnomalyResult(listener, detectorId));
    }

    private ActionListener<SearchResponse> onGetLatestAnomalyResult(ActionListener<DetectorProfile> listener, String detectorId) {
        return ActionListener.wrap(searchResponse -> {
            SearchHits hits = searchResponse.getHits();
            if (hits.getTotalHits().value == 0L) {
                logger.error("We should not get empty result: {}", detectorId);
                listener.onFailure(new RuntimeException("Unexpected error while looking for detector state:  " + detectorId));
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
                logger.error("Fail to find latest anomaly result of id: " + detectorId);
                listener.onFailure(new RuntimeException("Fail to find detector error: " + detectorId, exception));
            }
        });
    }

    /**
     * Create search request to check if we have at least 1 anomaly score larger than 0 after last update time
     * @param detectorId detector id
     * @param lastUpdateTimeEpochMs last update time in milliseconds
     * @return the search request
     */
    private SearchRequest createInittedEverRequest(String detectorId, long lastUpdateTimeEpochMs) {
        BoolQueryBuilder filterQuery = new BoolQueryBuilder();
        filterQuery.filter(QueryBuilders.termQuery(AnomalyResult.DETECTOR_ID_FIELD, detectorId));
        filterQuery.filter(QueryBuilders.rangeQuery(AnomalyResult.EXECUTION_END_TIME_FIELD).gte(lastUpdateTimeEpochMs));
        filterQuery.filter(QueryBuilders.rangeQuery(AnomalyResult.ANOMALY_SCORE_FIELD).gt(0));

        SearchSourceBuilder source = new SearchSourceBuilder().query(filterQuery).size(1);

        SearchRequest request = new SearchRequest(AnomalyResult.ANOMALY_RESULT_INDEX);
        request.source(source);
        return request;
    }

    private SearchRequest createLatestAnomalyResultRequest(String detectorId, long lastUpdateTimeEpochMs) {
        BoolQueryBuilder filterQuery = new BoolQueryBuilder();
        filterQuery.filter(QueryBuilders.termQuery(AnomalyResult.DETECTOR_ID_FIELD, detectorId));
        filterQuery.filter(QueryBuilders.rangeQuery(AnomalyResult.EXECUTION_END_TIME_FIELD).gte(lastUpdateTimeEpochMs));

        FieldSortBuilder sortQuery = new FieldSortBuilder(AnomalyResult.EXECUTION_END_TIME_FIELD).order(SortOrder.DESC);

        SearchSourceBuilder source = new SearchSourceBuilder().query(filterQuery).size(1).sort(sortQuery);

        SearchRequest request = new SearchRequest(AnomalyResult.ANOMALY_RESULT_INDEX);
        request.source(source);
        return request;
    }
}
