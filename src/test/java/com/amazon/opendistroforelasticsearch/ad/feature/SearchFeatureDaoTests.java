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

package com.amazon.opendistroforelasticsearch.ad.feature;

import java.time.temporal.ChronoUnit;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.BiConsumer;

import com.amazon.opendistroforelasticsearch.ad.dataprocessor.Interpolator;
import com.amazon.opendistroforelasticsearch.ad.dataprocessor.LinearUniformInterpolator;
import com.amazon.opendistroforelasticsearch.ad.dataprocessor.SingleFeatureLinearUniformInterpolator;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.transport.ADStateManager;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.ParseUtils;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.apache.lucene.search.TotalHits;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.script.TemplateScript.Factory;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Percentile;
import org.elasticsearch.search.builder.SearchSourceBuilder;

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

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.mockito.Mockito.mock;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@PowerMockIgnore("javax.management.*")
@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(JUnitParamsRunner.class)
@PrepareForTest({ ParseUtils.class })
public class SearchFeatureDaoTests {

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
    private ADStateManager stateManager;

    @Mock
    private AnomalyDetector detector;

    private SearchSourceBuilder featureQuery = new SearchSourceBuilder();
    private Map<String, Object> searchRequestParams;
    private SearchRequest searchRequest;
    private SearchSourceBuilder searchSourceBuilder;
    private MultiSearchRequest multiSearchRequest;
    private Map<String, Aggregation> aggsMap;
    private List<Aggregation> aggsList;
    private IntervalTimeConfiguration detectionInterval;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        PowerMockito.mockStatic(ParseUtils.class);

        Interpolator interpolator = new LinearUniformInterpolator(new SingleFeatureLinearUniformInterpolator());
        searchFeatureDao = spy(new SearchFeatureDao(client, xContent, interpolator, clientUtil));

        detectionInterval = new IntervalTimeConfiguration(1, ChronoUnit.MINUTES);
        when(detector.getTimeField()).thenReturn("testTimeField");
        when(detector.getIndices()).thenReturn(Arrays.asList("testIndices"));
        when(detector.generateFeatureQuery()).thenReturn(featureQuery);
        when(detector.getDetectionInterval()).thenReturn(detectionInterval);

        searchSourceBuilder = SearchSourceBuilder
            .fromXContent(XContentType.JSON.xContent().createParser(xContent, LoggingDeprecationHandler.INSTANCE, "{}"));
        searchRequestParams = new HashMap<>();
        searchRequest = new SearchRequest(detector.getIndices().toArray(new String[0]));
        aggsMap = new HashMap<>();
        aggsList = new ArrayList<>();

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

    private Object[] getFeatureSamplesForPeriodsData() {
        String maxName = "max";
        double maxValue = 2;
        Max max = mock(Max.class);
        when(max.value()).thenReturn(maxValue);
        when(max.getName()).thenReturn(maxName);

        String missingName = "mising";
        Max missing = mock(Max.class);
        when(missing.value()).thenReturn(Double.NaN);
        when(missing.getName()).thenReturn(missingName);

        return new Object[] {
            new Object[] { asList(max), asList(maxName), asList(Optional.of(new double[] { maxValue })) },
            new Object[] { asList(missing), asList(missingName), asList(Optional.empty()) }, };
    }

    @Test
    @Parameters(method = "getFeatureSamplesForPeriodsData")
    public void getFeatureSamplesForPeriods_returnExpected(
        List<Aggregation> aggs,
        List<String> featureIds,
        List<Optional<double[]>> expected
    ) throws Exception {

        long start = 1L;
        long end = 2L;

        // pre-conditions
        when(ParseUtils.generateInternalFeatureQuery(eq(detector), eq(start), eq(end), eq(xContent))).thenReturn(searchSourceBuilder);
        when(searchResponse.getAggregations()).thenReturn(new Aggregations(aggs));
        when(detector.getEnabledFeatureIds()).thenReturn(featureIds);

        List<Optional<double[]>> results = searchFeatureDao.getFeatureSamplesForPeriods(detector, asList(pair(start, end)));

        assertEquals(expected.size(), results.size());
        for (int i = 0; i < expected.size(); i++) {
            assertTrue(Arrays.equals(expected.get(i).orElse(null), results.get(i).orElse(null)));
        }
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
    public void getFeaturesForPeriod_returnEmpty_givenNoHits() throws Exception {
        long start = 100L;
        long end = 200L;

        // pre-conditions
        when(ParseUtils.generateInternalFeatureQuery(eq(detector), eq(start), eq(end), eq(xContent))).thenReturn(searchSourceBuilder);
        when(searchResponse.getHits()).thenReturn(new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 1f));

        // test
        Optional<double[]> result = searchFeatureDao.getFeaturesForPeriod(detector, start, end);

        // verify
        assertFalse(result.isPresent());
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

    private <K, V> Entry<K, V> pair(K key, V value) {
        return new SimpleEntry<>(key, value);
    }
}
