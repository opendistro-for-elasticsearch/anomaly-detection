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

package com.amazon.opendistroforelasticsearch.ad.feature;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.script.TemplateScript.Factory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalMax;
import org.elasticsearch.search.aggregations.metrics.InternalMin;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.Percentile;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.NodeStateManager;
import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
import com.amazon.opendistroforelasticsearch.ad.dataprocessor.Interpolator;
import com.amazon.opendistroforelasticsearch.ad.dataprocessor.LinearUniformInterpolator;
import com.amazon.opendistroforelasticsearch.ad.dataprocessor.SingleFeatureLinearUniformInterpolator;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.Feature;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.ParseUtils;
import com.google.common.collect.ImmutableList;

@PowerMockIgnore("javax.management.*")
@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(JUnitParamsRunner.class)
@PrepareForTest({ ParseUtils.class })
public class SearchFeatureDaoTests {
    private final Logger LOG = LogManager.getLogger(SearchFeatureDaoTests.class);

    private SearchFeatureDao searchFeatureDao;

    @Mock
    private Client client;
    @Mock
    private ScriptService scriptService;
    @Mock
    private NamedXContentRegistry xContent;
    @Mock
    private ClientUtil clientUtil;

    @Mock
    private Factory factory;
    @Mock
    private TemplateScript templateScript;
    @Mock
    private ActionFuture<SearchResponse> searchResponseFuture;
    @Mock
    private ActionFuture<MultiSearchResponse> multiSearchResponseFuture;
    @Mock
    private SearchResponse searchResponse;
    @Mock
    private MultiSearchResponse multiSearchResponse;
    @Mock
    private Item multiSearchResponseItem;
    @Mock
    private Aggregations aggs;
    @Mock
    private Max max;
    @Mock
    private NodeStateManager stateManager;

    @Mock
    private AnomalyDetector detector;

    @Mock
    private ThreadPool threadPool;

    @Mock
    private ClusterService clusterService;

    private SearchSourceBuilder featureQuery = new SearchSourceBuilder();
    // private Map<String, Object> searchRequestParams;
    private SearchRequest searchRequest;
    private SearchSourceBuilder searchSourceBuilder;
    private MultiSearchRequest multiSearchRequest;
    private Map<String, Aggregation> aggsMap;
    // private List<Aggregation> aggsList;
    private IntervalTimeConfiguration detectionInterval;
    // private Settings settings;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        PowerMockito.mockStatic(ParseUtils.class);

        Interpolator interpolator = new LinearUniformInterpolator(new SingleFeatureLinearUniformInterpolator());

        ExecutorService executorService = mock(ExecutorService.class);
        when(threadPool.executor(AnomalyDetectorPlugin.AD_THREAD_POOL_NAME)).thenReturn(executorService);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(executorService).execute(any(Runnable.class));

        Settings settings = Settings.EMPTY;
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.MAX_ENTITIES_PER_QUERY)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        searchFeatureDao = spy(new SearchFeatureDao(client, xContent, interpolator, clientUtil, threadPool, settings, clusterService));

        detectionInterval = new IntervalTimeConfiguration(1, ChronoUnit.MINUTES);
        when(detector.getTimeField()).thenReturn("testTimeField");
        when(detector.getIndices()).thenReturn(Arrays.asList("testIndices"));
        when(detector.generateFeatureQuery()).thenReturn(featureQuery);
        when(detector.getDetectionInterval()).thenReturn(detectionInterval);
        when(detector.getFilterQuery()).thenReturn(QueryBuilders.matchAllQuery());
        when(detector.getCategoryField()).thenReturn(Collections.singletonList("a"));

        searchSourceBuilder = SearchSourceBuilder
            .fromXContent(XContentType.JSON.xContent().createParser(xContent, LoggingDeprecationHandler.INSTANCE, "{}"));
        // searchRequestParams = new HashMap<>();
        searchRequest = new SearchRequest(detector.getIndices().toArray(new String[0]));
        aggsMap = new HashMap<>();
        // aggsList = new ArrayList<>();

        when(max.getName()).thenReturn(SearchFeatureDao.AGG_NAME_MAX);
        List<Aggregation> list = new ArrayList<>();
        list.add(max);
        Aggregations aggregations = new Aggregations(list);
        SearchHits hits = new SearchHits(new SearchHit[0], new TotalHits(1L, TotalHits.Relation.EQUAL_TO), 1f);
        when(searchResponse.getHits()).thenReturn(hits);

        doReturn(Optional.of(searchResponse))
            .when(clientUtil)
            .timedRequest(eq(searchRequest), anyObject(), Matchers.<BiConsumer<SearchRequest, ActionListener<SearchResponse>>>anyObject());
        when(searchResponse.getAggregations()).thenReturn(aggregations);

        doReturn(Optional.of(searchResponse))
            .when(clientUtil)
            .throttledTimedRequest(
                eq(searchRequest),
                anyObject(),
                Matchers.<BiConsumer<SearchRequest, ActionListener<SearchResponse>>>anyObject(),
                anyObject()
            );

        multiSearchRequest = new MultiSearchRequest();
        SearchRequest request = new SearchRequest(detector.getIndices().toArray(new String[0]));
        multiSearchRequest.add(request);
        doReturn(Optional.of(multiSearchResponse))
            .when(clientUtil)
            .timedRequest(
                eq(multiSearchRequest),
                anyObject(),
                Matchers.<BiConsumer<MultiSearchRequest, ActionListener<MultiSearchResponse>>>anyObject()
            );
        when(multiSearchResponse.getResponses()).thenReturn(new Item[] { multiSearchResponseItem });
        when(multiSearchResponseItem.getResponse()).thenReturn(searchResponse);
    }

    @Test
    public void test_getLatestDataTime_returnExpectedTime_givenData() {
        // pre-conditions
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .aggregation(AggregationBuilders.max(SearchFeatureDao.AGG_NAME_MAX).field(detector.getTimeField()))
            .size(0);
        searchRequest.source(searchSourceBuilder);

        long epochTime = 100L;
        aggsMap.put(SearchFeatureDao.AGG_NAME_MAX, max);
        when(max.getValue()).thenReturn((double) epochTime);

        // test
        Optional<Long> result = searchFeatureDao.getLatestDataTime(detector);

        // verify
        assertEquals(epochTime, result.get().longValue());
    }

    @Test
    public void test_getLatestDataTime_returnEmpty_givenNoData() {
        // pre-conditions
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .aggregation(AggregationBuilders.max(SearchFeatureDao.AGG_NAME_MAX).field(detector.getTimeField()))
            .size(0);
        searchRequest.source(searchSourceBuilder);

        when(searchResponse.getAggregations()).thenReturn(null);

        // test
        Optional<Long> result = searchFeatureDao.getLatestDataTime(detector);

        // verify
        assertFalse(result.isPresent());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getLatestDataTime_returnExpectedToListener() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .aggregation(AggregationBuilders.max(SearchFeatureDao.AGG_NAME_MAX).field(detector.getTimeField()))
            .size(0);
        searchRequest.source(searchSourceBuilder);
        long epochTime = 100L;
        aggsMap.put(SearchFeatureDao.AGG_NAME_MAX, max);
        when(max.getValue()).thenReturn((double) epochTime);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(eq(searchRequest), any(ActionListener.class));

        ActionListener<Optional<Long>> listener = mock(ActionListener.class);
        searchFeatureDao.getLatestDataTime(detector, listener);

        ArgumentCaptor<Optional<Long>> captor = ArgumentCaptor.forClass(Optional.class);
        verify(listener).onResponse(captor.capture());
        Optional<Long> result = captor.getValue();
        assertEquals(epochTime, result.get().longValue());
    }

    @SuppressWarnings("unchecked")
    private Object[] getFeaturesForPeriodData() {
        String maxName = "max";
        double maxValue = 2;
        Max max = mock(Max.class);
        when(max.value()).thenReturn(maxValue);
        when(max.getName()).thenReturn(maxName);

        String percentileName = "percentile";
        double percentileValue = 1;
        InternalTDigestPercentiles percentiles = mock(InternalTDigestPercentiles.class);
        Iterator<Percentile> percentilesIterator = mock(Iterator.class);
        Percentile percentile = mock(Percentile.class);
        when(percentiles.iterator()).thenReturn(percentilesIterator);
        when(percentilesIterator.hasNext()).thenReturn(true);
        when(percentilesIterator.next()).thenReturn(percentile);
        when(percentile.getValue()).thenReturn(percentileValue);
        when(percentiles.getName()).thenReturn(percentileName);

        String missingName = "missing";
        Max missing = mock(Max.class);
        when(missing.value()).thenReturn(Double.NaN);
        when(missing.getName()).thenReturn(missingName);

        String infinityName = "infinity";
        Max infinity = mock(Max.class);
        when(infinity.value()).thenReturn(Double.POSITIVE_INFINITY);
        when(infinity.getName()).thenReturn(infinityName);

        String emptyName = "empty";
        InternalTDigestPercentiles empty = mock(InternalTDigestPercentiles.class);
        Iterator<Percentile> emptyIterator = mock(Iterator.class);
        when(empty.iterator()).thenReturn(emptyIterator);
        when(emptyIterator.hasNext()).thenReturn(false);
        when(empty.getName()).thenReturn(emptyName);

        return new Object[] {
            new Object[] { asList(max), asList(maxName), new double[] { maxValue }, },
            new Object[] { asList(percentiles), asList(percentileName), new double[] { percentileValue } },
            new Object[] { asList(missing), asList(missingName), null },
            new Object[] { asList(infinity), asList(infinityName), null },
            new Object[] { asList(max, percentiles), asList(maxName, percentileName), new double[] { maxValue, percentileValue } },
            new Object[] { asList(max, percentiles), asList(percentileName, maxName), new double[] { percentileValue, maxValue } },
            new Object[] { asList(max, percentiles, missing), asList(maxName, percentileName, missingName), null }, };
    }

    @Test
    @Parameters(method = "getFeaturesForPeriodData")
    public void getFeaturesForPeriod_returnExpected_givenData(List<Aggregation> aggs, List<String> featureIds, double[] expected)
        throws Exception {

        long start = 100L;
        long end = 200L;

        // pre-conditions
        when(ParseUtils.generateInternalFeatureQuery(eq(detector), eq(start), eq(end), eq(xContent))).thenReturn(searchSourceBuilder);
        when(searchResponse.getAggregations()).thenReturn(new Aggregations(aggs));
        when(detector.getEnabledFeatureIds()).thenReturn(featureIds);

        // test
        Optional<double[]> result = searchFeatureDao.getFeaturesForPeriod(detector, start, end);

        // verify
        assertTrue(Arrays.equals(expected, result.orElse(null)));
    }

    @SuppressWarnings("unchecked")
    private Object[] getFeaturesForPeriodThrowIllegalStateData() {
        String aggName = "aggName";

        InternalTDigestPercentiles empty = mock(InternalTDigestPercentiles.class);
        Iterator<Percentile> emptyIterator = mock(Iterator.class);
        when(empty.iterator()).thenReturn(emptyIterator);
        when(emptyIterator.hasNext()).thenReturn(false);
        when(empty.getName()).thenReturn(aggName);

        MultiBucketsAggregation multiBucket = mock(MultiBucketsAggregation.class);
        when(multiBucket.getName()).thenReturn(aggName);

        return new Object[] {
            new Object[] { asList(empty), asList(aggName), null },
            new Object[] { asList(multiBucket), asList(aggName), null }, };
    }

    @Test(expected = IllegalStateException.class)
    @Parameters(method = "getFeaturesForPeriodThrowIllegalStateData")
    public void getFeaturesForPeriod_throwIllegalState_forUnknownAggregation(
        List<Aggregation> aggs,
        List<String> featureIds,
        double[] expected
    ) throws Exception {
        getFeaturesForPeriod_returnExpected_givenData(aggs, featureIds, expected);
    }

    @Test
    @Parameters(method = "getFeaturesForPeriodData")
    @SuppressWarnings("unchecked")
    public void getFeaturesForPeriod_returnExpectedToListener(List<Aggregation> aggs, List<String> featureIds, double[] expected)
        throws Exception {

        long start = 100L;
        long end = 200L;
        when(ParseUtils.generateInternalFeatureQuery(eq(detector), eq(start), eq(end), eq(xContent))).thenReturn(searchSourceBuilder);
        when(searchResponse.getAggregations()).thenReturn(new Aggregations(aggs));
        when(detector.getEnabledFeatureIds()).thenReturn(featureIds);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(eq(searchRequest), any(ActionListener.class));

        ActionListener<Optional<double[]>> listener = mock(ActionListener.class);
        searchFeatureDao.getFeaturesForPeriod(detector, start, end, listener);

        ArgumentCaptor<Optional<double[]>> captor = ArgumentCaptor.forClass(Optional.class);
        verify(listener).onResponse(captor.capture());
        Optional<double[]> result = captor.getValue();
        assertTrue(Arrays.equals(expected, result.orElse(null)));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getFeaturesForPeriod_throwToListener_whenSearchFails() throws Exception {

        long start = 100L;
        long end = 200L;
        when(ParseUtils.generateInternalFeatureQuery(eq(detector), eq(start), eq(end), eq(xContent))).thenReturn(searchSourceBuilder);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException());
            return null;
        }).when(client).search(eq(searchRequest), any(ActionListener.class));

        ActionListener<Optional<double[]>> listener = mock(ActionListener.class);
        searchFeatureDao.getFeaturesForPeriod(detector, start, end, listener);

        verify(listener).onFailure(any(Exception.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getFeaturesForPeriod_throwToListener_whenResponseParsingFails() throws Exception {

        long start = 100L;
        long end = 200L;
        when(ParseUtils.generateInternalFeatureQuery(eq(detector), eq(start), eq(end), eq(xContent))).thenReturn(searchSourceBuilder);
        when(detector.getEnabledFeatureIds()).thenReturn(null);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(eq(searchRequest), any(ActionListener.class));

        ActionListener<Optional<double[]>> listener = mock(ActionListener.class);
        searchFeatureDao.getFeaturesForPeriod(detector, start, end, listener);

        verify(listener).onFailure(any(Exception.class));
    }

    @Test
    public void test_getFeaturesForPeriod_returnEmpty_givenNoData() throws Exception {
        long start = 100L;
        long end = 200L;

        // pre-conditions
        when(ParseUtils.generateInternalFeatureQuery(eq(detector), eq(start), eq(end), eq(xContent))).thenReturn(searchSourceBuilder);
        when(searchResponse.getAggregations()).thenReturn(null);

        // test
        Optional<double[]> result = searchFeatureDao.getFeaturesForPeriod(detector, start, end);

        // verify
        assertFalse(result.isPresent());
    }

    @Test
    public void getFeaturesForPeriod_returnNonEmpty_givenDefaultValue() throws Exception {
        long start = 100L;
        long end = 200L;

        // pre-conditions
        when(ParseUtils.generateInternalFeatureQuery(eq(detector), eq(start), eq(end), eq(xContent))).thenReturn(searchSourceBuilder);
        when(searchResponse.getHits()).thenReturn(new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 1f));

        List<Aggregation> aggList = new ArrayList<>(1);

        NumericMetricsAggregation.SingleValue agg = mock(NumericMetricsAggregation.SingleValue.class);
        when(agg.getName()).thenReturn("deny_max");
        when(agg.value()).thenReturn(0d);

        aggList.add(agg);

        Aggregations aggregations = new Aggregations(aggList);
        when(searchResponse.getAggregations()).thenReturn(aggregations);

        // test
        Optional<double[]> result = searchFeatureDao.getFeaturesForPeriod(detector, start, end);

        // verify
        assertTrue(result.isPresent());
    }

    private Object[] getFeaturesForSampledPeriodsData() {
        long endTime = 300_000;
        int maxStride = 4;
        return new Object[] {

            // No data

            new Object[] { new Long[0][0], new double[0][0], endTime, 1, 1, Optional.empty() },

            // 1 data point

            new Object[] {
                new Long[][] { { 240_000L, 300_000L } },
                new double[][] { { 1, 2 } },
                endTime,
                1,
                1,
                Optional.of(new SimpleEntry<>(new double[][] { { 1, 2 } }, 1)) },

            new Object[] {
                new Long[][] { { 240_000L, 300_000L } },
                new double[][] { { 1, 2 } },
                endTime,
                1,
                3,
                Optional.of(new SimpleEntry<>(new double[][] { { 1, 2 } }, 1)) },

            // 2 data points

            new Object[] {
                new Long[][] { { 180_000L, 240_000L }, { 240_000L, 300_000L } },
                new double[][] { { 1, 2 }, { 2, 4 } },
                endTime,
                1,
                2,
                Optional.of(new SimpleEntry<>(new double[][] { { 1, 2 }, { 2, 4 } }, 1)) },

            new Object[] {
                new Long[][] { { 180_000L, 240_000L }, { 240_000L, 300_000L } },
                new double[][] { { 1, 2 }, { 2, 4 } },
                endTime,
                1,
                1,
                Optional.of(new SimpleEntry<>(new double[][] { { 2, 4 } }, 1)) },

            new Object[] {
                new Long[][] { { 180_000L, 240_000L }, { 240_000L, 300_000L } },
                new double[][] { { 1, 2 }, { 2, 4 } },
                endTime,
                4,
                2,
                Optional.of(new SimpleEntry<>(new double[][] { { 1, 2 }, { 2, 4 } }, 1)) },

            new Object[] {
                new Long[][] { { 0L, 60_000L }, { 240_000L, 300_000L } },
                new double[][] { { 1, 2 }, { 2, 4 } },
                endTime,
                4,
                2,
                Optional.of(new SimpleEntry<>(new double[][] { { 1, 2 }, { 2, 4 } }, 4)) },

            // 5 data points

            new Object[] {
                new Long[][] {
                    { 0L, 60_000L },
                    { 60_000L, 120_000L },
                    { 120_000L, 180_000L },
                    { 180_000L, 240_000L },
                    { 240_000L, 300_000L } },
                new double[][] { { 1, 2 }, { 3, 4 }, { 5, 6 }, { 7, 8 }, { 9, 10 } },
                endTime,
                4,
                10,
                Optional.of(new SimpleEntry<>(new double[][] { { 1, 2 }, { 3, 4 }, { 5, 6 }, { 7, 8 }, { 9, 10 } }, 1)) },

            new Object[] {
                new Long[][] { { 0L, 60_000L }, { 60_000L, 120_000L }, { 180_000L, 240_000L }, { 240_000L, 300_000L } },
                new double[][] { { 1, 2 }, { 3, 4 }, { 7, 8 }, { 9, 10 } },
                endTime,
                4,
                10,
                Optional.of(new SimpleEntry<>(new double[][] { { 1, 2 }, { 3, 4 }, { 5, 6 }, { 7, 8 }, { 9, 10 } }, 1)) },

            new Object[] {
                new Long[][] { { 0L, 60_000L }, { 120_000L, 180_000L }, { 240_000L, 300_000L } },
                new double[][] { { 1, 2 }, { 5, 6 }, { 9, 10 } },
                endTime,
                4,
                10,
                Optional.of(new SimpleEntry<>(new double[][] { { 1, 2 }, { 3, 4 }, { 5, 6 }, { 7, 8 }, { 9, 10 } }, 1)) },

            new Object[] {
                new Long[][] { { 0L, 60_000L }, { 240_000L, 300_000L } },
                new double[][] { { 1, 2 }, { 9, 10 } },
                endTime,
                4,
                10,
                Optional.of(new SimpleEntry<>(new double[][] { { 1, 2 }, { 3, 4 }, { 5, 6 }, { 7, 8 }, { 9, 10 } }, 1)) }, };
    }

    @Test
    @Parameters(method = "getFeaturesForSampledPeriodsData")
    public void getFeaturesForSampledPeriods_returnExpected(
        Long[][] queryRanges,
        double[][] queryResults,
        long endTime,
        int maxStride,
        int maxSamples,
        Optional<Entry<double[][], Integer>> expected
    ) {

        doReturn(Optional.empty()).when(searchFeatureDao).getFeaturesForPeriod(eq(detector), anyLong(), anyLong());
        for (int i = 0; i < queryRanges.length; i++) {
            doReturn(Optional.of(queryResults[i]))
                .when(searchFeatureDao)
                .getFeaturesForPeriod(detector, queryRanges[i][0], queryRanges[i][1]);
        }

        Optional<Entry<double[][], Integer>> result = searchFeatureDao
            .getFeaturesForSampledPeriods(detector, maxSamples, maxStride, endTime);

        assertEquals(expected.isPresent(), result.isPresent());
        if (expected.isPresent()) {
            assertTrue(Arrays.deepEquals(expected.get().getKey(), result.get().getKey()));
            assertEquals(expected.get().getValue(), result.get().getValue());
        }
    }

    @Test
    @Parameters(method = "getFeaturesForSampledPeriodsData")
    @SuppressWarnings("unchecked")
    public void getFeaturesForSampledPeriods_returnExpectedToListener(
        Long[][] queryRanges,
        double[][] queryResults,
        long endTime,
        int maxStride,
        int maxSamples,
        Optional<Entry<double[][], Integer>> expected
    ) {
        doAnswer(invocation -> {
            ActionListener<Optional<double[]>> listener = invocation.getArgument(3);
            listener.onResponse(Optional.empty());
            return null;
        }).when(searchFeatureDao).getFeaturesForPeriod(any(), anyLong(), anyLong(), any(ActionListener.class));
        for (int i = 0; i < queryRanges.length; i++) {
            double[] queryResult = queryResults[i];
            doAnswer(invocation -> {
                ActionListener<Optional<double[]>> listener = invocation.getArgument(3);
                listener.onResponse(Optional.of(queryResult));
                return null;
            })
                .when(searchFeatureDao)
                .getFeaturesForPeriod(eq(detector), eq(queryRanges[i][0]), eq(queryRanges[i][1]), any(ActionListener.class));
        }

        ActionListener<Optional<Entry<double[][], Integer>>> listener = mock(ActionListener.class);
        searchFeatureDao.getFeaturesForSampledPeriods(detector, maxSamples, maxStride, endTime, listener);

        ArgumentCaptor<Optional<Entry<double[][], Integer>>> captor = ArgumentCaptor.forClass(Optional.class);
        verify(listener).onResponse(captor.capture());
        Optional<Entry<double[][], Integer>> result = captor.getValue();
        assertEquals(expected.isPresent(), result.isPresent());
        if (expected.isPresent()) {
            assertTrue(Arrays.deepEquals(expected.get().getKey(), result.get().getKey()));
            assertEquals(expected.get().getValue(), result.get().getValue());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getFeaturesForSampledPeriods_throwToListener_whenSamplingFail() {
        doAnswer(invocation -> {
            ActionListener<Optional<double[]>> listener = invocation.getArgument(3);
            listener.onFailure(new RuntimeException());
            return null;
        }).when(searchFeatureDao).getFeaturesForPeriod(any(), anyLong(), anyLong(), any(ActionListener.class));

        ActionListener<Optional<Entry<double[][], Integer>>> listener = mock(ActionListener.class);
        searchFeatureDao.getFeaturesForSampledPeriods(detector, 1, 1, 0, listener);

        verify(listener).onFailure(any(Exception.class));
    }

    private <K, V> Entry<K, V> pair(K key, V value) {
        return new SimpleEntry<>(key, value);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNormalGetFeaturesByEntities() throws IOException {
        SearchHits hits = new SearchHits(new SearchHit[] {}, null, Float.NaN);

        String aggregationId = "deny_max";
        String featureName = "deny max";
        AggregationBuilder builder = new MaxAggregationBuilder("deny_max").field("deny");
        AggregatorFactories.Builder aggBuilder = AggregatorFactories.builder();
        aggBuilder.addAggregator(builder);
        when(detector.getEnabledFeatureIds()).thenReturn(Collections.singletonList(aggregationId));
        when(detector.getFeatureAttributes()).thenReturn(Collections.singletonList(new Feature(aggregationId, featureName, true, builder)));
        when(ParseUtils.parseAggregators(anyString(), any(), anyString())).thenReturn(aggBuilder);

        String app0Name = "app_0";
        double app0Max = 1976.0;
        InternalAggregation app0Agg = new InternalMax(aggregationId, app0Max, DocValueFormat.RAW, Collections.emptyMap());
        StringTerms.Bucket app0Bucket = new StringTerms.Bucket(
            new BytesRef(app0Name.getBytes(StandardCharsets.UTF_8), 0, app0Name.getBytes(StandardCharsets.UTF_8).length),
            3,
            InternalAggregations.from(Collections.singletonList(app0Agg)),
            false,
            0,
            DocValueFormat.RAW
        );

        String app1Name = "app_1";
        double app1Max = 3604.0;
        InternalAggregation app1Agg = new InternalMax(aggregationId, app1Max, DocValueFormat.RAW, Collections.emptyMap());
        StringTerms.Bucket app1Bucket = new StringTerms.Bucket(
            new BytesRef(app1Name.getBytes(StandardCharsets.UTF_8), 0, app1Name.getBytes(StandardCharsets.UTF_8).length),
            3,
            InternalAggregations.from(Collections.singletonList(app1Agg)),
            false,
            0,
            DocValueFormat.RAW
        );

        List<StringTerms.Bucket> stringBuckets = ImmutableList.of(app0Bucket, app1Bucket);

        StringTerms termsAgg = new StringTerms(
            "term_agg",
            BucketOrder.count(false),
            1,
            0,
            Collections.emptyMap(),
            DocValueFormat.RAW,
            1,
            false,
            0,
            stringBuckets,
            0
        );

        InternalAggregations internalAggregations = InternalAggregations.from(Collections.singletonList(termsAgg));

        SearchResponseSections searchSections = new SearchResponseSections(hits, internalAggregations, null, false, false, null, 1);

        // Simulate response:
        // {"took":507,"timed_out":false,"_shards":{"total":1,"successful":1,
        // "skipped":0,"failed":0},"hits":{"max_score":null,"hits":[]},
        // "aggregations":{"term_agg":{"doc_count_error_upper_bound":0,
        // "sum_other_doc_count":0,"buckets":[{"key":"app_0","doc_count":3,
        // "deny_max":{"value":1976.0}},{"key":"app_1","doc_count":3,
        // "deny_max":{"value":3604.0}}]}}}
        SearchResponse searchResponse = new SearchResponse(
            searchSections,
            null,
            1,
            1,
            0,
            507,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );

        doAnswer(invocation -> {
            SearchRequest request = invocation.getArgument(0);
            assertEquals(1, request.indices().length);
            assertTrue(detector.getIndices().contains(request.indices()[0]));
            AggregatorFactories.Builder aggs = request.source().aggregations();
            assertEquals(1, aggs.count());
            Collection<AggregationBuilder> factory = aggs.getAggregatorFactories();
            assertTrue(!factory.isEmpty());
            assertThat(factory.iterator().next(), instanceOf(TermsAggregationBuilder.class));

            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any(ActionListener.class));

        ActionListener<Map<String, double[]>> listener = mock(ActionListener.class);
        searchFeatureDao.getFeaturesByEntities(detector, 10L, 20L, listener);

        ArgumentCaptor<Map<String, double[]>> captor = ArgumentCaptor.forClass(Map.class);
        verify(listener).onResponse(captor.capture());
        Map<String, double[]> result = captor.getValue();
        assertEquals(2, result.size());
        assertEquals(app0Max, result.get(app0Name)[0], 0.001);
        assertEquals(app1Max, result.get(app1Name)[0], 0.001);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testEmptyGetFeaturesByEntities() {
        SearchResponseSections searchSections = new SearchResponseSections(null, null, null, false, false, null, 1);

        SearchResponse searchResponse = new SearchResponse(
            searchSections,
            null,
            1,
            1,
            0,
            507,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any(ActionListener.class));

        ActionListener<Map<String, double[]>> listener = mock(ActionListener.class);
        searchFeatureDao.getFeaturesByEntities(detector, 10L, 20L, listener);

        ArgumentCaptor<Map<String, double[]>> captor = ArgumentCaptor.forClass(Map.class);
        verify(listener).onResponse(captor.capture());
        Map<String, double[]> result = captor.getValue();
        assertEquals(0, result.size());
    }

    @SuppressWarnings("unchecked")
    @Test(expected = EndRunException.class)
    public void testParseIOException() throws Exception {
        String aggregationId = "deny_max";
        String featureName = "deny max";
        AggregationBuilder builder = new MaxAggregationBuilder("deny_max").field("deny");
        AggregatorFactories.Builder aggBuilder = AggregatorFactories.builder();
        aggBuilder.addAggregator(builder);
        when(detector.getEnabledFeatureIds()).thenReturn(Collections.singletonList(aggregationId));
        when(detector.getFeatureAttributes()).thenReturn(Collections.singletonList(new Feature(aggregationId, featureName, true, builder)));
        PowerMockito.doThrow(new IOException()).when(ParseUtils.class, "parseAggregators", anyString(), any(), anyString());

        ActionListener<Map<String, double[]>> listener = mock(ActionListener.class);
        searchFeatureDao.getFeaturesByEntities(detector, 10L, 20L, listener);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetEntityMinMaxDataTime() {
        // simulate response {"took":11,"timed_out":false,"_shards":{"total":1,
        // "successful":1,"skipped":0,"failed":0},"hits":{"max_score":null,"hits":[]},
        // "aggregations":{"min_timefield":{"value":1.602211285E12,
        // "value_as_string":"2020-10-09T02:41:25.000Z"},
        // "max_timefield":{"value":1.602348325E12,"value_as_string":"2020-10-10T16:45:25.000Z"}}}
        DocValueFormat dateFormat = new DocValueFormat.DateTime(
            DateFormatter.forPattern("strict_date_optional_time||epoch_millis"),
            ZoneId.of("UTC"),
            DateFieldMapper.Resolution.MILLISECONDS
        );
        double earliest = 1.602211285E12;
        double latest = 1.602348325E12;
        InternalMin minInternal = new InternalMin("min_timefield", earliest, dateFormat, new HashMap<>());
        InternalMax maxInternal = new InternalMax("max_timefield", latest, dateFormat, new HashMap<>());
        InternalAggregations internalAggregations = InternalAggregations.from(Arrays.asList(minInternal, maxInternal));
        SearchHits hits = new SearchHits(new SearchHit[] {}, null, Float.NaN);
        SearchResponseSections searchSections = new SearchResponseSections(hits, internalAggregations, null, false, false, null, 1);

        SearchResponse searchResponse = new SearchResponse(
            searchSections,
            null,
            1,
            1,
            0,
            11,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );

        doAnswer(invocation -> {
            SearchRequest request = invocation.getArgument(0);
            assertEquals(1, request.indices().length);
            assertTrue(detector.getIndices().contains(request.indices()[0]));
            AggregatorFactories.Builder aggs = request.source().aggregations();
            assertEquals(2, aggs.count());
            Collection<AggregationBuilder> factory = aggs.getAggregatorFactories();
            assertTrue(!factory.isEmpty());
            Iterator<AggregationBuilder> iterator = factory.iterator();
            while (iterator.hasNext()) {
                assertThat(iterator.next(), anyOf(instanceOf(MaxAggregationBuilder.class), instanceOf(MinAggregationBuilder.class)));
            }

            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any(ActionListener.class));

        ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = mock(ActionListener.class);
        searchFeatureDao.getEntityMinMaxDataTime(detector, "app_1", listener);

        ArgumentCaptor<Entry<Optional<Long>, Optional<Long>>> captor = ArgumentCaptor.forClass(Entry.class);
        verify(listener).onResponse(captor.capture());
        Entry<Optional<Long>, Optional<Long>> result = captor.getValue();
        assertEquals((long) earliest, result.getKey().get().longValue());
        assertEquals((long) latest, result.getValue().get().longValue());
    }
}
