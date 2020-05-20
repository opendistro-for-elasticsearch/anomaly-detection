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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfile;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorState;
import com.amazon.opendistroforelasticsearch.ad.model.ProfileName;
import com.amazon.opendistroforelasticsearch.ad.transport.ProfileAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ProfileRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.ProfileResponse;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;
import com.amazon.opendistroforelasticsearch.ad.util.MultiResponsesDelegateActionListener;

public class AnomalyDetectorProfileRunner {
    private final Logger logger = LogManager.getLogger(AnomalyDetectorProfileRunner.class);
    private Client client;
    private NamedXContentRegistry xContentRegistry;
    private DiscoveryNodeFilterer nodeFilter;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    static String FAIL_TO_FIND_DETECTOR_MSG = "Fail to find detector with id: ";
    static String FAIL_TO_GET_PROFILE_MSG = "Fail to get profile for detector ";
    private final ClusterService clusterService;
    private Calendar calendar;

    public AnomalyDetectorProfileRunner(
        Client client,
        NamedXContentRegistry xContentRegistry,
        DiscoveryNodeFilterer nodeFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        Calendar calendar
    ) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.nodeFilter = nodeFilter;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.clusterService = clusterService;
        this.calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    }

    public void profile(String detectorId, ActionListener<DetectorProfile> listener, Set<ProfileName> profiles) {

        if (profiles.isEmpty()) {
            listener.onFailure(new RuntimeException("Unsupported profile types."));
            return;
        }

        // total number of listeners we need to define. Needed by MultiResponsesDelegateActionListener to decide when to consolidate results
        // and return to users
        int totalListener = 0;

        if (profiles.contains(ProfileName.STATE)) {
            totalListener++;
        }

        if (profiles.contains(ProfileName.ERROR)) {
            totalListener++;
        }

        if (profiles.contains(ProfileName.COORDINATING_NODE)
            || profiles.contains(ProfileName.SHINGLE_SIZE)
            || profiles.contains(ProfileName.TOTAL_SIZE_IN_BYTES)
            || profiles.contains(ProfileName.MODELS)) {
            totalListener++;
        }

        MultiResponsesDelegateActionListener<DetectorProfile> delegateListener = new MultiResponsesDelegateActionListener<DetectorProfile>(
            listener,
            totalListener,
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
                        profileError(detectorId, enabledTimeMs, job.getDisabledTime(), listener);
                    }

                    if (profiles.contains(ProfileName.COORDINATING_NODE)
                        || profiles.contains(ProfileName.SHINGLE_SIZE)
                        || profiles.contains(ProfileName.TOTAL_SIZE_IN_BYTES)
                        || profiles.contains(ProfileName.MODELS)) {
                        profileModels(detectorId, profiles, listener);
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
            if (hits.getHits().length == 0L) {
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
     * Precondition:
     * 1. Index are rotated with name pattern ".opendistro-anomaly-results-history-{now/d}-1" and now is using UTC.
     * 2. Latest entry with error is recorded within enabled and disabled time.  Note disabled time can be null.
     *
     * Error is populated if error of the latest anomaly result is not empty.
     *
     * Two optimization to avoid scanning all anomaly result indices to get a detector's most recent error
     *
     * First, when a detector is running, we only need to scan the current index, not all of the rolled over ones
     *  since we are interested in the latest error.
     * Second, when a detector is disabled, we only need to scan the latest anomaly result indices created before the
     *  detector's enable time.
     *
     * @param detectorId detector id
     * @param enabledTimeMillis the time when AD job is enabled in milliseconds
     * @param listener listener to process the returned error or exception
     */
    private void profileError(
        String detectorId,
        long enabledTimeMillis,
        Instant disabledTime,
        MultiResponsesDelegateActionListener<DetectorProfile> listener
    ) {
        String[] latestIndex = null;

        long disabledTimeMillis = 0;
        if (disabledTime != null) {
            disabledTimeMillis = disabledTime.toEpochMilli();
        }
        if (enabledTimeMillis > disabledTimeMillis) {
            // detector is still running
            latestIndex = new String[1];
            latestIndex[0] = AnomalyResult.ANOMALY_RESULT_INDEX;
        } else {
            String[] concreteIndices = indexNameExpressionResolver
                .concreteIndexNames(
                    clusterService.state(),
                    IndicesOptions.lenientExpandOpen(),
                    AnomalyDetectionIndices.ALL_AD_RESULTS_INDEX_PATTERN
                );

            // find the latest from result indices such as .opendistro-anomaly-results-history-2020.04.06-1 and
            // /.opendistro-anomaly-results-history-2020.04.07-000002
            long maxTimestamp = -1;
            TreeMap<Long, List<String>> candidateIndices = new TreeMap<>();
            for (String indexName : concreteIndices) {
                Matcher m = Pattern.compile("\\.opendistro-anomaly-results-history-(\\d{4})\\.(\\d{2})\\.(\\d{2})-\\d+").matcher(indexName);
                if (m.matches()) {
                    int year = Integer.parseInt(m.group(1));
                    int month = Integer.parseInt(m.group(2));
                    int date = Integer.parseInt(m.group(3));
                    // month starts with 0
                    calendar.clear();
                    calendar.set(year, month - 1, date);
                    // 2020.05.08 is translated to 1588896000000
                    long timestamp = calendar.getTimeInMillis();

                    // a candidate index can be created before or after enabled time, but the index is definitely created before disabled
                    // time
                    if (timestamp <= disabledTimeMillis && maxTimestamp <= timestamp) {
                        maxTimestamp = timestamp;
                        // we can have two rotations on the same day and we don't know which one has our data, so we keep all
                        List<String> indexList = candidateIndices.computeIfAbsent(timestamp, k -> new ArrayList<String>());
                        indexList.add(indexName);
                    }
                }
            }
            List<String> candidates = new ArrayList<String>();
            List<String> latestCandidate = candidateIndices.get(maxTimestamp);

            if (latestCandidate != null) {
                candidates.addAll(latestCandidate);
            }

            // look back one more index for an edge case:
            // Suppose detector interval is 1 minute. Detector last run is at 2020-05-07, 11:59:50 PM,
            // then AD result indices rolled over as .opendistro-anomaly-results-history-2020.05.07-001
            // Detector next run will be 2020-05-08, 00:00:50 AM. If a user stop the detector at
            // 2020-05-08 00:00:10 AM, detector will not have AD result on 2020-05-08.
            // We check AD result indices one day earlier to make sure we can always get AD result.
            Map.Entry<Long, List<String>> earlierCandidate = candidateIndices.lowerEntry(maxTimestamp);
            if (earlierCandidate != null) {
                candidates.addAll(earlierCandidate.getValue());
            }
            latestIndex = candidates.toArray(new String[0]);
        }

        if (latestIndex == null || latestIndex.length == 0) {
            // no result index found: can be due to anomaly result is not created yet or result indices for the detector have been deleted.
            listener.onResponse(new DetectorProfile());
            return;
        }
        SearchRequest searchLatestResult = createLatestAnomalyResultRequest(detectorId, enabledTimeMillis, disabledTimeMillis, latestIndex);
        client.search(searchLatestResult, onGetLatestAnomalyResult(listener, detectorId));
    }

    private ActionListener<SearchResponse> onGetLatestAnomalyResult(ActionListener<DetectorProfile> listener, String detectorId) {
        return ActionListener.wrap(searchResponse -> {
            SearchHits hits = searchResponse.getHits();
            if (hits.getHits().length == 0L) {
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

        // I am only looking for last 1 occurrence and have no interest in the total number of documents that match the query.
        // ES will not try to count the number of documents and will be able to terminate the query as soon as 1 document
        // have been collected per segment.
        SearchSourceBuilder source = new SearchSourceBuilder().query(filterQuery).size(1).trackTotalHits(false);

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
    private SearchRequest createLatestAnomalyResultRequest(String detectorId, long enabledTime, long disabledTime, String[] index) {
        BoolQueryBuilder filterQuery = new BoolQueryBuilder();
        filterQuery.filter(QueryBuilders.termQuery(AnomalyResult.DETECTOR_ID_FIELD, detectorId));
        RangeQueryBuilder rangeBuilder = QueryBuilders.rangeQuery(AnomalyResult.EXECUTION_END_TIME_FIELD).gte(enabledTime);
        if (disabledTime >= enabledTime) {
            rangeBuilder.lte(disabledTime);
        }
        filterQuery.filter(rangeBuilder);

        FieldSortBuilder sortQuery = new FieldSortBuilder(AnomalyResult.EXECUTION_END_TIME_FIELD).order(SortOrder.DESC);

        // I am only looking for last 1 occurrence and have no interest in the total number of documents that match the query.
        // ES will not try to count the number of documents and will be able to terminate the query as soon as 1 document
        // have been collected per segment.
        SearchSourceBuilder source = new SearchSourceBuilder().query(filterQuery).size(1).sort(sortQuery).trackTotalHits(false);

        SearchRequest request = new SearchRequest(index);
        request.source(source);
        return request;
    }

    private void profileModels(
        String detectorId,
        Set<ProfileName> profiles,
        MultiResponsesDelegateActionListener<DetectorProfile> listener
    ) {
        DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
        ProfileRequest profileRequest = new ProfileRequest(detectorId, profiles, dataNodes);
        client.execute(ProfileAction.INSTANCE, profileRequest, onModelResponse(detectorId, profiles, listener));
    }

    private ActionListener<ProfileResponse> onModelResponse(
        String detectorId,
        Set<ProfileName> profiles,
        MultiResponsesDelegateActionListener<DetectorProfile> listener
    ) {
        return ActionListener.wrap(profileResponse -> {
            DetectorProfile profile = new DetectorProfile();
            if (profiles.contains(ProfileName.COORDINATING_NODE)) {
                profile.setCoordinatingNode(profileResponse.getCoordinatingNode());
            }
            if (profiles.contains(ProfileName.SHINGLE_SIZE)) {
                profile.setShingleSize(profileResponse.getShingleSize());
            }
            if (profiles.contains(ProfileName.TOTAL_SIZE_IN_BYTES)) {
                profile.setTotalSizeInBytes(profileResponse.getTotalSizeInBytes());
            }
            if (profiles.contains(ProfileName.MODELS)) {
                profile.setModelProfile(profileResponse.getModelProfile());
            }

            listener.onResponse(profile);
        }, listener::onFailure);
    }
}
