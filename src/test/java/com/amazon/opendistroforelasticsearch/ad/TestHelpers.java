/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorExecutionInput;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.Feature;
import com.amazon.opendistroforelasticsearch.ad.model.FeatureData;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.model.TimeConfiguration;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.UUIDs;
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

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.BUILT_IN_ROLES;
import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomDouble;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomLong;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

public class TestHelpers {

    public static final String AD_BASE_DETECTORS_URI = "/_opendistro/_anomaly_detection/detectors";
    public static final String AD_BASE_RESULT_URI = "/_opendistro/_anomaly_detection/detectors/results";
    public static final String AD_BASE_PREVIEW_URI = "/_opendistro/_anomaly_detection/detectors/%s/_preview";
    public static final String AD_BASE_STATS_URI = "/_opendistro/_anomaly_detection/stats";
    private static final Logger logger = LogManager.getLogger(TestHelpers.class);

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
        Request request = new Request(method, endpoint);

        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        if (headers != null) {
            headers.forEach(header -> options.addHeader(header.getName(), header.getValue()));
        }
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

    public static NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public static AnomalyDetector randomAnomalyDetector(Map<String, Object> uiMetadata, Instant lastUpdateTime) throws IOException {
        return randomAnomalyDetector(ImmutableList.of(randomFeature()), uiMetadata, lastUpdateTime);
    }

    public static AnomalyDetector randomAnomalyDetector(List<Feature> features, Map<String, Object> uiMetadata, Instant lastUpdateTime)
        throws IOException {
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
            uiMetadata,
            randomInt(),
            lastUpdateTime
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
            null,
            randomInt(),
            Instant.now()
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
            null,
            randomInt(),
            Instant.now().truncatedTo(ChronoUnit.SECONDS)
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
        AggregationBuilder testAggregation = null;
        try {
            testAggregation = randomAggregation(aggregationName);
        } catch (IOException e) {
            logger.error("Fail to generate test aggregation");
            throw new RuntimeException();
        }
        return new Feature(randomAlphaOfLength(5), featureName, ESRestTestCase.randomBoolean(), testAggregation);
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
        return randomAnomalyDetectResult(randomDouble(), randomAlphaOfLength(5));
    }

    public static AnomalyResult randomAnomalyDetectResult(double score) {
        return randomAnomalyDetectResult(randomDouble(), null);
    }

    public static AnomalyResult randomAnomalyDetectResult(String error) {
        return randomAnomalyDetectResult(Double.NaN, error);
    }

    public static AnomalyResult randomAnomalyDetectResult(double score, String error) {
        return new AnomalyResult(
            randomAlphaOfLength(5),
            score,
            randomDouble(),
            randomDouble(),
            ImmutableList.of(randomFeatureData(), randomFeatureData()),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            error
        );
    }

    public static AnomalyDetectorJob randomAnomalyDetectorJob() {
        return randomAnomalyDetectorJob(true);
    }

    public static AnomalyDetectorJob randomAnomalyDetectorJob(boolean enabled) {
        return new AnomalyDetectorJob(
            randomAlphaOfLength(10),
            randomIntervalSchedule(),
            randomIntervalTimeConfiguration(),
            enabled,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            60L
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
        client.admin().cluster().prepareHealth(indexName).setWaitForEvents(Priority.URGENT).get();
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
        IndexMetaData.Builder builder = IndexMetaData.builder(indexName);
        if (alias != null) {
            builder.putAlias(AliasMetaData.builder(alias));
        }
        IndexMetaData indexMetaData = builder
            .settings(
                Settings
                    .builder()
                    .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                    .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(hackedSettings)
            )
            .build();
        MetaData metaData = MetaData.builder().put(indexMetaData, false).build();
        blockedClusterState = ClusterState
            .builder(new ClusterName("test cluster"))
            .metaData(metaData)
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

    public static GetResponse createGetResponse(ToXContentObject o, String id) throws IOException {
        XContentBuilder content = o.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS);

        return new GetResponse(
            new GetResult(
                AnomalyDetector.ANOMALY_DETECTORS_INDEX,
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

    public static SearchResponse createSearchResponse(ToXContentObject o) throws IOException {
        XContentBuilder content = o.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS);

        SearchHit[] hits = new SearchHit[1];
        hits[0] = new SearchHit(0).sourceRef(BytesReference.bytes(content));

        return new SearchResponse(
            new InternalSearchResponse(
                new SearchHits(hits, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f),
                new InternalAggregations(Collections.emptyList()),
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
                new InternalAggregations(Collections.emptyList()),
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
}
