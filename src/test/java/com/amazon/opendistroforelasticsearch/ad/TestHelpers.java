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

import static org.apache.http.entity.ContentType.APPLICATION_JSON;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.BUILT_IN_ROLES;
import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomDouble;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomLong;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonValue;
import com.amazon.opendistroforelasticsearch.ad.feature.Features;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingResult;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskState;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskType;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorExecutionInput;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorType;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.DetectionDateRange;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorInternalState;
import com.amazon.opendistroforelasticsearch.ad.model.Entity;
import com.amazon.opendistroforelasticsearch.ad.model.Feature;
import com.amazon.opendistroforelasticsearch.ad.model.FeatureData;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.model.TimeConfiguration;
import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TestHelpers {

    public static final String AD_BASE_DETECTORS_URI = "/_opendistro/_anomaly_detection/detectors";
    public static final String AD_BASE_RESULT_URI = "/_opendistro/_anomaly_detection/detectors/results";
    public static final String AD_BASE_PREVIEW_URI = "/_opendistro/_anomaly_detection/detectors/%s/_preview";
    public static final String AD_BASE_STATS_URI = "/_opendistro/_anomaly_detection/stats";
    private static final Logger logger = LogManager.getLogger(TestHelpers.class);
    public static final Random random = new Random(42);

    public static Response makeRequest(
        RestClient client,
        String method,
        String endpoint,
        Map<String, String> params,
        String jsonEntity,
        List<Header> headers
    ) throws IOException {
        HttpEntity httpEntity = Strings.isBlank(jsonEntity) ? null : new NStringEntity(jsonEntity, ContentType.APPLICATION_JSON);
        return makeRequest(client, method, endpoint, params, httpEntity, headers);
    }

    public static Response makeRequest(
        RestClient client,
        String method,
        String endpoint,
        Map<String, String> params,
        HttpEntity entity,
        List<Header> headers
    ) throws IOException {
        return makeRequest(client, method, endpoint, params, entity, headers, true);
    }

    public static Response makeRequest(
        RestClient client,
        String method,
        String endpoint,
        Map<String, String> params,
        HttpEntity entity,
        List<Header> headers,
        boolean strictDeprecationMode
    ) throws IOException {
        Request request = new Request(method, endpoint);

        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        if (headers != null) {
            headers.forEach(header -> options.addHeader(header.getName(), header.getValue()));
        }
        options.setWarningsHandler(strictDeprecationMode ? WarningsHandler.STRICT : WarningsHandler.PERMISSIVE);
        request.setOptions(options.build());

        if (params != null) {
            params.entrySet().forEach(it -> request.addParameter(it.getKey(), it.getValue()));
        }
        if (entity != null) {
            request.setEntity(entity);
        }
        return client.performRequest(request);
    }

    public static String xContentBuilderToString(XContentBuilder builder) {
        return BytesReference.bytes(builder).utf8ToString();
    }

    public static XContentBuilder builder() throws IOException {
        return XContentBuilder.builder(XContentType.JSON.xContent());
    }

    public static XContentParser parser(String xc) throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, xc);
        parser.nextToken();
        return parser;
    }

    public static Map<String, Object> XContentBuilderToMap(XContentBuilder builder) {
        return XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
    }

    public static NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public static AnomalyDetector randomAnomalyDetector(Map<String, Object> uiMetadata, Instant lastUpdateTime) throws IOException {
        return randomAnomalyDetector(ImmutableList.of(randomFeature()), uiMetadata, lastUpdateTime, null, null);
    }

    public static AnomalyDetector randomAnomalyDetector(Map<String, Object> uiMetadata, Instant lastUpdateTime, boolean featureEnabled)
        throws IOException {
        return randomAnomalyDetector(ImmutableList.of(randomFeature(featureEnabled)), uiMetadata, lastUpdateTime, null, null);
    }

    public static AnomalyDetector randomAnomalyDetector(List<Feature> features, Map<String, Object> uiMetadata, Instant lastUpdateTime)
        throws IOException {
        return randomAnomalyDetector(features, uiMetadata, lastUpdateTime, null, null);
    }

    public static AnomalyDetector randomAnomalyDetector(
        List<Feature> features,
        Map<String, Object> uiMetadata,
        Instant lastUpdateTime,
        String detectorType,
        DetectionDateRange dateRange
    ) throws IOException {
        return randomAnomalyDetector(features, uiMetadata, lastUpdateTime, detectorType, dateRange, true);
    }

    public static AnomalyDetector randomAnomalyDetector(
        List<Feature> features,
        Map<String, Object> uiMetadata,
        Instant lastUpdateTime,
        String detectorType,
        DetectionDateRange dateRange,
        boolean withUser
    ) throws IOException {
        return randomAnomalyDetector(
            ImmutableList.of(randomAlphaOfLength(10).toLowerCase()),
            features,
            uiMetadata,
            lastUpdateTime,
            detectorType,
            ESRestTestCase.randomLongBetween(1, 1000),
            dateRange,
            withUser
        );
    }

    public static AnomalyDetector randomAnomalyDetector(
        List<String> indices,
        List<Feature> features,
        Map<String, Object> uiMetadata,
        Instant lastUpdateTime,
        String detectorType,
        long detectionIntervalInMinutes,
        DetectionDateRange dateRange,
        boolean withUser
    ) throws IOException {
        User user = withUser ? randomUser() : null;
        return new AnomalyDetector(
            randomAlphaOfLength(10),
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            randomAlphaOfLength(5),
            indices,
            features,
            randomQuery(),
            new IntervalTimeConfiguration(detectionIntervalInMinutes, ChronoUnit.MINUTES),
            randomIntervalTimeConfiguration(),
            randomIntBetween(1, 2000),
            uiMetadata,
            randomInt(),
            lastUpdateTime,
            null,
            user,
            detectorType,
            dateRange
        );
    }

    public static AnomalyDetector randomDetector(
        DetectionDateRange dateRange,
        List<Feature> features,
        String indexName,
        int detectionIntervalInMinutes,
        String timeField
    ) throws IOException {
        String detectorType = dateRange == null
            ? AnomalyDetectorType.REALTIME_SINGLE_ENTITY.name()
            : AnomalyDetectorType.HISTORICAL_SINGLE_ENTITY.name();
        return new AnomalyDetector(
            randomAlphaOfLength(10),
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            timeField,
            ImmutableList.of(indexName),
            features,
            randomQuery("{\"bool\":{\"filter\":[{\"exists\":{\"field\":\"value\"}}]}}"),
            new IntervalTimeConfiguration(detectionIntervalInMinutes, ChronoUnit.MINUTES),
            new IntervalTimeConfiguration(ESRestTestCase.randomLongBetween(1, 5), ChronoUnit.MINUTES),
            8,
            null,
            randomInt(),
            Instant.now(),
            null,
            null,
            detectorType,
            dateRange
        );
    }

    public static DetectionDateRange randomDetectionDateRange() {
        return new DetectionDateRange(
            Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(10, ChronoUnit.DAYS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS)
        );
    }

    public static AnomalyDetector randomAnomalyDetectorUsingCategoryFields(String detectorId, List<String> categoryFields)
        throws IOException {
        return new AnomalyDetector(
            detectorId,
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(10).toLowerCase()),
            ImmutableList.of(randomFeature(true)),
            randomQuery(),
            randomIntervalTimeConfiguration(),
            new IntervalTimeConfiguration(0, ChronoUnit.MINUTES),
            randomIntBetween(1, 2000),
            null,
            randomInt(),
            Instant.now(),
            categoryFields,
            randomUser()
        );
    }

    public static AnomalyDetector randomAnomalyDetector(List<Feature> features) throws IOException {
        return new AnomalyDetector(
            randomAlphaOfLength(10),
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(10).toLowerCase()),
            features,
            randomQuery(),
            randomIntervalTimeConfiguration(),
            randomIntervalTimeConfiguration(),
            randomIntBetween(1, 2000),
            null,
            randomInt(),
            Instant.now(),
            null,
            randomUser()
        );
    }

    public static AnomalyDetector randomAnomalyDetectorWithEmptyFeature() throws IOException {
        return new AnomalyDetector(
            randomAlphaOfLength(10),
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(10).toLowerCase()),
            ImmutableList.of(),
            randomQuery(),
            randomIntervalTimeConfiguration(),
            randomIntervalTimeConfiguration(),
            randomIntBetween(1, 2000),
            null,
            randomInt(),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            null,
            randomUser()
        );
    }

    public static AnomalyDetector randomAnomalyDetectorWithInterval(TimeConfiguration interval) throws IOException {
        return randomAnomalyDetectorWithInterval(interval, false);
    }

    public static AnomalyDetector randomAnomalyDetectorWithInterval(TimeConfiguration interval, boolean hcDetector) throws IOException {
        List<String> categoryField = hcDetector ? ImmutableList.of(randomAlphaOfLength(5)) : null;
        return new AnomalyDetector(
            randomAlphaOfLength(10),
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(10).toLowerCase()),
            ImmutableList.of(randomFeature()),
            randomQuery(),
            interval,
            randomIntervalTimeConfiguration(),
            randomIntBetween(1, 2000),
            null,
            randomInt(),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            categoryField,
            randomUser()
        );
    }

    public static SearchSourceBuilder randomFeatureQuery() throws IOException {
        String query = "{\"query\":{\"match\":{\"user\":{\"query\":\"kimchy\",\"operator\":\"OR\",\"prefix_length\":0,"
            + "\"max_expansions\":50,\"fuzzy_transpositions\":true,\"lenient\":false,\"zero_terms_query\":\"NONE\","
            + "\"auto_generate_synonyms_phrase_query\":true,\"boost\":1}}}}";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        XContentParser parser = XContentType.JSON
            .xContent()
            .createParser(new NamedXContentRegistry(searchModule.getNamedXContents()), LoggingDeprecationHandler.INSTANCE, query);
        searchSourceBuilder.parseXContent(parser);
        return searchSourceBuilder;
    }

    public static QueryBuilder randomQuery() throws IOException {
        String query = "{\"bool\":{\"must\":{\"term\":{\"user\":\"kimchy\"}},\"filter\":{\"term\":{\"tag\":"
            + "\"tech\"}},\"must_not\":{\"range\":{\"age\":{\"gte\":10,\"lte\":20}}},\"should\":[{\"term\":"
            + "{\"tag\":\"wow\"}},{\"term\":{\"tag\":\"elasticsearch\"}}],\"minimum_should_match\":1,\"boost\":1}}";
        return randomQuery(query);
    }

    public static QueryBuilder randomQuery(String query) throws IOException {
        XContentParser parser = TestHelpers.parser(query);
        return parseInnerQueryBuilder(parser);
    }

    public static AggregationBuilder randomAggregation() throws IOException {
        return randomAggregation(randomAlphaOfLength(5));
    }

    public static AggregationBuilder randomAggregation(String aggregationName) throws IOException {
        XContentParser parser = parser("{\"" + aggregationName + "\":{\"value_count\":{\"field\":\"ok\"}}}");

        AggregatorFactories.Builder parsed = AggregatorFactories.parseAggregators(parser);
        return parsed.getAggregatorFactories().iterator().next();
    }

    /**
     * Parse string aggregation query into {@link AggregationBuilder}
     * Sample input:
     * "{\"test\":{\"value_count\":{\"field\":\"ok\"}}}"
     *
     * @param aggregationQuery aggregation builder
     * @return aggregation builder
     * @throws IOException IO exception
     */
    public static AggregationBuilder parseAggregation(String aggregationQuery) throws IOException {
        XContentParser parser = parser(aggregationQuery);

        AggregatorFactories.Builder parsed = AggregatorFactories.parseAggregators(parser);
        return parsed.getAggregatorFactories().iterator().next();
    }

    public static Map<String, Object> randomUiMetadata() {
        return ImmutableMap.of(randomAlphaOfLength(5), randomFeature());
    }

    public static TimeConfiguration randomIntervalTimeConfiguration() {
        return new IntervalTimeConfiguration(ESRestTestCase.randomLongBetween(1, 1000), ChronoUnit.MINUTES);
    }

    public static IntervalSchedule randomIntervalSchedule() {
        return new IntervalSchedule(
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            ESRestTestCase.randomIntBetween(1, 1000),
            ChronoUnit.MINUTES
        );
    }

    public static Feature randomFeature() {
        return randomFeature(randomAlphaOfLength(5), randomAlphaOfLength(5));
    }

    public static Feature randomFeature(String featureName, String aggregationName) {
        return randomFeature(featureName, aggregationName, randomBoolean());
    }

    public static Feature randomFeature(boolean enabled) {
        return randomFeature(randomAlphaOfLength(5), randomAlphaOfLength(5), enabled);
    }

    public static Feature randomFeature(String featureName, String aggregationName, boolean enabled) {
        AggregationBuilder testAggregation = null;
        try {
            testAggregation = randomAggregation(aggregationName);
        } catch (IOException e) {
            logger.error("Fail to generate test aggregation");
            throw new RuntimeException();
        }
        return new Feature(randomAlphaOfLength(5), featureName, enabled, testAggregation);
    }

    public static Features randomFeatures() {
        List<Map.Entry<Long, Long>> ranges = Arrays.asList(new AbstractMap.SimpleEntry<>(0L, 1L));
        double[][] unprocessed = new double[][] { { randomDouble(), randomDouble() } };
        double[][] processed = new double[][] { { randomDouble(), randomDouble() } };

        return new Features(ranges, unprocessed, processed);
    }

    public static List<ThresholdingResult> randomThresholdingResults() {
        double grade = 1.;
        double confidence = 0.5;
        double score = 1.;

        ThresholdingResult thresholdingResult = new ThresholdingResult(grade, confidence, score);
        List<ThresholdingResult> results = new ArrayList<>();
        results.add(thresholdingResult);
        return results;
    }

    public static User randomUser() {
        return new User(
            randomAlphaOfLength(8),
            ImmutableList.of(randomAlphaOfLength(10)),
            ImmutableList.of("all_access"),
            ImmutableList.of("attribute=test")
        );
    }

    public static <S, T> void assertFailWith(Class<S> clazz, Callable<T> callable) throws Exception {
        assertFailWith(clazz, null, callable);
    }

    public static <S, T> void assertFailWith(Class<S> clazz, String message, Callable<T> callable) throws Exception {
        try {
            callable.call();
        } catch (Throwable e) {
            if (e.getClass() != clazz) {
                throw e;
            }
            if (message != null && !e.getMessage().contains(message)) {
                throw e;
            }
        }
    }

    public static FeatureData randomFeatureData() {
        return new FeatureData(randomAlphaOfLength(5), randomAlphaOfLength(5), randomDouble());
    }

    public static AnomalyResult randomAnomalyDetectResult() {
        return randomAnomalyDetectResult(randomDouble(), randomAlphaOfLength(5), null);
    }

    public static AnomalyResult randomAnomalyDetectResult(double score) {
        return randomAnomalyDetectResult(randomDouble(), null, null);
    }

    public static AnomalyResult randomAnomalyDetectResult(String error) {
        return randomAnomalyDetectResult(Double.NaN, error, null);
    }

    public static AnomalyResult randomAnomalyDetectResult(double score, String error, String taskId) {
        return randomAnomalyDetectResult(score, error, taskId, true);
    }

    public static AnomalyResult randomAnomalyDetectResult(double score, String error, String taskId, boolean withUser) {
        User user = withUser ? randomUser() : null;
        return new AnomalyResult(
            randomAlphaOfLength(5),
            taskId,
            score,
            randomDouble(),
            randomDouble(),
            ImmutableList.of(randomFeatureData(), randomFeatureData()),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            error,
            null,
            user,
            CommonValue.NO_SCHEMA_VERSION
        );
    }

    public static AnomalyResult randomMultiEntityAnomalyDetectResult(double score, double grade) {
        return randomMutlEntityAnomalyDetectResult(score, grade, null);
    }

    public static AnomalyResult randomMutlEntityAnomalyDetectResult(double score, double grade, String error) {
        return new AnomalyResult(
            randomAlphaOfLength(5),
            score,
            grade,
            randomDouble(),
            ImmutableList.of(randomFeatureData(), randomFeatureData()),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            error,
            Arrays.asList(new Entity(randomAlphaOfLength(5), randomAlphaOfLength(5))),
            randomUser(),
            CommonValue.NO_SCHEMA_VERSION
        );
    }

    public static AnomalyDetectorJob randomAnomalyDetectorJob() {
        return randomAnomalyDetectorJob(true);
    }

    public static AnomalyDetectorJob randomAnomalyDetectorJob(boolean enabled, Instant enabledTime, Instant disabledTime) {
        return new AnomalyDetectorJob(
            randomAlphaOfLength(10),
            randomIntervalSchedule(),
            randomIntervalTimeConfiguration(),
            enabled,
            enabledTime,
            disabledTime,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            60L,
            randomUser()
        );
    }

    public static AnomalyDetectorJob randomAnomalyDetectorJob(boolean enabled) {
        return randomAnomalyDetectorJob(
            enabled,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS)
        );
    }

    public static AnomalyDetectorExecutionInput randomAnomalyDetectorExecutionInput() throws IOException {
        return new AnomalyDetectorExecutionInput(
            randomAlphaOfLength(5),
            Instant.now().minus(10, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            randomAnomalyDetector(null, Instant.now().truncatedTo(ChronoUnit.SECONDS))
        );
    }

    public static ActionListener<CreateIndexResponse> createActionListener(
        CheckedConsumer<CreateIndexResponse, ? extends Exception> consumer,
        Consumer<Exception> failureConsumer
    ) {
        return ActionListener.wrap(consumer, failureConsumer);
    }

    public static void waitForIndexCreationToComplete(Client client, final String indexName) {
        ClusterHealthResponse clusterHealthResponse = client
            .admin()
            .cluster()
            .prepareHealth(indexName)
            .setWaitForEvents(Priority.URGENT)
            .get();
        logger.info("Status of " + indexName + ": " + clusterHealthResponse.getStatus());
    }

    public static ClusterService createClusterService(ThreadPool threadPool, ClusterSettings clusterSettings) {
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "node",
            ESTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            BUILT_IN_ROLES,
            Version.CURRENT
        );
        return ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSettings);
    }

    public static ClusterState createIndexBlockedState(String indexName, Settings hackedSettings, String alias) {
        ClusterState blockedClusterState = null;
        IndexMetadata.Builder builder = IndexMetadata.builder(indexName);
        if (alias != null) {
            builder.putAlias(AliasMetadata.builder(alias));
        }
        IndexMetadata indexMetaData = builder
            .settings(
                Settings
                    .builder()
                    .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(hackedSettings)
            )
            .build();
        Metadata metaData = Metadata.builder().put(indexMetaData, false).build();
        blockedClusterState = ClusterState
            .builder(new ClusterName("test cluster"))
            .metadata(metaData)
            .blocks(ClusterBlocks.builder().addBlocks(indexMetaData))
            .build();
        return blockedClusterState;
    }

    public static ThreadContext createThreadContext() {
        Settings build = Settings.builder().put("request.headers.default", "1").build();
        ThreadContext context = new ThreadContext(build);
        context.putHeader("foo", "bar");
        context.putTransient("x", 1);
        return context;
    }

    public static ThreadPool createThreadPool() {
        ThreadPool pool = mock(ThreadPool.class);
        when(pool.getThreadContext()).thenReturn(createThreadContext());
        return pool;
    }

    public static CreateIndexResponse createIndex(AdminClient adminClient, String indexName, String indexMapping) {
        CreateIndexRequest request = new CreateIndexRequest(indexName).mapping(AnomalyDetector.TYPE, indexMapping, XContentType.JSON);
        return adminClient.indices().create(request).actionGet(5_000);
    }

    public static void createIndex(RestClient client, String indexName, HttpEntity data) throws IOException {
        TestHelpers
            .makeRequest(
                client,
                "POST",
                "/" + indexName + "/_doc/" + randomAlphaOfLength(5) + "?refresh=true",
                ImmutableMap.of(),
                data,
                null
            );
    }

    public static GetResponse createGetResponse(ToXContentObject o, String id, String indexName) throws IOException {
        XContentBuilder content = o.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS);
        return new GetResponse(
            new GetResult(
                indexName,
                MapperService.SINGLE_MAPPING_NAME,
                id,
                UNASSIGNED_SEQ_NO,
                0,
                -1,
                true,
                BytesReference.bytes(content),
                Collections.emptyMap(),
                Collections.emptyMap()
            )
        );
    }

    public static GetResponse createBrokenGetResponse(String id, String indexName) throws IOException {
        ByteBuffer[] buffers = new ByteBuffer[0];
        return new GetResponse(
            new GetResult(
                indexName,
                MapperService.SINGLE_MAPPING_NAME,
                id,
                UNASSIGNED_SEQ_NO,
                0,
                -1,
                true,
                BytesReference.fromByteBuffers(buffers),
                Collections.emptyMap(),
                Collections.emptyMap()
            )
        );
    }

    public static SearchResponse createSearchResponse(ToXContentObject o) throws IOException {
        XContentBuilder content = o.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS);

        SearchHit[] hits = new SearchHit[1];
        hits[0] = new SearchHit(0).sourceRef(BytesReference.bytes(content));

        return new SearchResponse(
            new InternalSearchResponse(
                new SearchHits(hits, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f),
                InternalAggregations.EMPTY,
                new Suggest(Collections.emptyList()),
                new SearchProfileShardResults(Collections.emptyMap()),
                false,
                false,
                1
            ),
            "",
            5,
            5,
            0,
            100,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
    }

    public static SearchResponse createEmptySearchResponse() throws IOException {
        return new SearchResponse(
            new InternalSearchResponse(
                new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 1.0f),
                InternalAggregations.EMPTY,
                new Suggest(Collections.emptyList()),
                new SearchProfileShardResults(Collections.emptyMap()),
                false,
                false,
                1
            ),
            "",
            5,
            5,
            0,
            100,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
    }

    public static DetectorInternalState randomDetectState(String error) {
        return randomDetectState(error, Instant.now());
    }

    public static DetectorInternalState randomDetectState(Instant lastUpdateTime) {
        return randomDetectState(randomAlphaOfLength(5), lastUpdateTime);
    }

    public static DetectorInternalState randomDetectState(String error, Instant lastUpdateTime) {
        return new DetectorInternalState.Builder().lastUpdateTime(lastUpdateTime).error(error).build();
    }

    public static Map<String, Map<String, Map<String, FieldMappingMetadata>>> createFieldMappings(
        String index,
        String fieldName,
        String fieldType
    ) throws IOException {
        Map<String, Map<String, Map<String, FieldMappingMetadata>>> mappings = new HashMap<>();
        FieldMappingMetadata fieldMappingMetadata = new FieldMappingMetadata(
            fieldName,
            new BytesArray("{\"" + fieldName + "\":{\"type\":\"" + fieldType + "\"}}")
        );
        mappings.put(index, Collections.singletonMap(CommonName.MAPPING_TYPE, Collections.singletonMap(fieldName, fieldMappingMetadata)));
        return mappings;
    }

    public static ADTask randomAdTask() throws IOException {
        return randomAdTask(
            randomAlphaOfLength(5),
            ADTaskState.RUNNING,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            randomAlphaOfLength(5),
            true
        );
    }

    public static ADTask randomAdTask(
        String taskId,
        ADTaskState state,
        Instant executionEndTime,
        String stoppedBy,
        String detectorId,
        AnomalyDetector detector
    ) {
        executionEndTime = executionEndTime == null ? null : executionEndTime.truncatedTo(ChronoUnit.SECONDS);
        ADTask task = ADTask
            .builder()
            .taskId(taskId)
            .taskType(ADTaskType.HISTORICAL.name())
            .detectorId(detectorId)
            .detector(detector)
            .state(state.name())
            .taskProgress(0.5f)
            .initProgress(1.0f)
            .currentPiece(Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(randomIntBetween(1, 100), ChronoUnit.MINUTES))
            .executionStartTime(Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(100, ChronoUnit.MINUTES))
            .executionEndTime(executionEndTime)
            .isLatest(true)
            .error(randomAlphaOfLength(5))
            .checkpointId(randomAlphaOfLength(5))
            .lastUpdateTime(Instant.now().truncatedTo(ChronoUnit.SECONDS))
            .startedBy(randomAlphaOfLength(5))
            .stoppedBy(stoppedBy)
            .build();
        return task;
    }

    public static ADTask randomAdTask(String taskId, ADTaskState state, Instant executionEndTime, String stoppedBy, boolean withDetector)
        throws IOException {
        AnomalyDetector detector = withDetector
            ? randomAnomalyDetector(ImmutableMap.of(), Instant.now().truncatedTo(ChronoUnit.SECONDS), true)
            : null;
        executionEndTime = executionEndTime == null ? null : executionEndTime.truncatedTo(ChronoUnit.SECONDS);
        ADTask task = ADTask
            .builder()
            .taskId(taskId)
            .taskType(ADTaskType.HISTORICAL.name())
            .detectorId(randomAlphaOfLength(5))
            .detector(detector)
            .state(state.name())
            .taskProgress(0.5f)
            .initProgress(1.0f)
            .currentPiece(Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(randomIntBetween(1, 100), ChronoUnit.MINUTES))
            .executionStartTime(Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(100, ChronoUnit.MINUTES))
            .executionEndTime(executionEndTime)
            .isLatest(true)
            .error(randomAlphaOfLength(5))
            .checkpointId(randomAlphaOfLength(5))
            .lastUpdateTime(Instant.now().truncatedTo(ChronoUnit.SECONDS))
            .startedBy(randomAlphaOfLength(5))
            .stoppedBy(stoppedBy)
            .build();
        return task;
    }

    public static HttpEntity toHttpEntity(ToXContentObject object) throws IOException {
        return new StringEntity(toJsonString(object), APPLICATION_JSON);
    }

    public static HttpEntity toHttpEntity(String jsonString) throws IOException {
        return new StringEntity(jsonString, APPLICATION_JSON);
    }

    public static String toJsonString(ToXContentObject object) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        return TestHelpers.xContentBuilderToString(object.toXContent(builder, ToXContent.EMPTY_PARAMS));
    }

    public static SearchHits createSearchHits(int totalHits) {
        List<SearchHit> hitList = new ArrayList<>();
        IntStream.range(0, totalHits).forEach(i -> hitList.add(new SearchHit(i)));
        SearchHit[] hitArray = new SearchHit[hitList.size()];
        return new SearchHits(hitList.toArray(hitArray), new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), 1.0F);
    }
}
