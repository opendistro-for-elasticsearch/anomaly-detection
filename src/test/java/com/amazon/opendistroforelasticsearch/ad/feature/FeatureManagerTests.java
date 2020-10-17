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
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
import com.amazon.opendistroforelasticsearch.ad.dataprocessor.Interpolator;
import com.amazon.opendistroforelasticsearch.ad.dataprocessor.LinearUniformInterpolator;
import com.amazon.opendistroforelasticsearch.ad.dataprocessor.SingleFeatureLinearUniformInterpolator;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.util.ArrayEqMatcher;

@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("unchecked")
public class FeatureManagerTests {

    // configuration
    private int maxTrainSamples;
    private int maxSampleStride;
    private int trainSampleTimeRangeInHours;
    private int minTrainSamples;
    private int shingleSize;
    private double maxMissingPointsRate;
    private int maxNeighborDistance;
    private double previewSampleRate;
    private int maxPreviewSamples;
    private Duration featureBufferTtl;
    private long intervalInMilliseconds;

    @Mock
    private AnomalyDetector detector;

    @Mock
    private SearchFeatureDao searchFeatureDao;

    @Mock
    private Interpolator interpolator;

    @Mock
    private Clock clock;

    @Mock
    private ThreadPool threadPool;

    private FeatureManager featureManager;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        maxTrainSamples = 24;
        maxSampleStride = 100;
        trainSampleTimeRangeInHours = 1;
        minTrainSamples = 4;
        shingleSize = 3;
        maxMissingPointsRate = 0.67;
        maxNeighborDistance = 2;
        previewSampleRate = 0.5;
        maxPreviewSamples = 2;
        featureBufferTtl = Duration.ofMillis(1_000L);

        when(detector.getDetectorId()).thenReturn("id");
        when(detector.getShingleSize()).thenReturn(shingleSize);
        IntervalTimeConfiguration detectorIntervalTimeConfig = new IntervalTimeConfiguration(1, ChronoUnit.MINUTES);
        intervalInMilliseconds = detectorIntervalTimeConfig.toDuration().toMillis();
        when(detector.getDetectorIntervalInMilliseconds()).thenReturn(intervalInMilliseconds);

        Interpolator interpolator = new LinearUniformInterpolator(new SingleFeatureLinearUniformInterpolator());

        ExecutorService executorService = mock(ExecutorService.class);

        when(threadPool.executor(AnomalyDetectorPlugin.AD_THREAD_POOL_NAME)).thenReturn(executorService);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(executorService).execute(any(Runnable.class));

        this.featureManager = spy(
            new FeatureManager(
                searchFeatureDao,
                interpolator,
                clock,
                maxTrainSamples,
                maxSampleStride,
                trainSampleTimeRangeInHours,
                minTrainSamples,
                maxMissingPointsRate,
                maxNeighborDistance,
                previewSampleRate,
                maxPreviewSamples,
                featureBufferTtl,
                threadPool,
                AnomalyDetectorPlugin.AD_THREAD_POOL_NAME
            )
        );
    }

    private Object[] getColdStartDataTestData() {
        double[][] samples = new double[][] { { 1.0 } };
        return new Object[] {
            new Object[] { 1L, new SimpleEntry<>(samples, 1), 1, samples },
            new Object[] { 1L, null, 1, null },
            new Object[] { null, new SimpleEntry<>(samples, 1), 1, null },
            new Object[] { null, null, 1, null }, };
    }

    @Test
    @Parameters(method = "getColdStartDataTestData")
    public void getColdStartData_returnExpected(Long latestTime, Entry<double[][], Integer> data, int interpolants, double[][] expected) {
        when(searchFeatureDao.getLatestDataTime(detector)).thenReturn(ofNullable(latestTime));
        if (latestTime != null) {
            when(searchFeatureDao.getFeaturesForSampledPeriods(detector, maxTrainSamples, maxSampleStride, latestTime))
                .thenReturn(ofNullable(data));
        }
        if (data != null) {
            when(interpolator.interpolate(argThat(new ArrayEqMatcher<>(data.getKey())), eq(interpolants))).thenReturn(data.getKey());
            doReturn(data.getKey()).when(featureManager).batchShingle(argThat(new ArrayEqMatcher<>(data.getKey())), eq(shingleSize));
        }

        Optional<double[][]> results = featureManager.getColdStartData(detector);

        assertTrue(Arrays.deepEquals(expected, results.orElse(null)));
    }

    private Object[] getTrainDataTestData() {
        List<Entry<Long, Long>> ranges = asList(
            entry(0L, 900_000L),
            entry(900_000L, 1_800_000L),
            entry(1_800_000L, 2_700_000L),
            entry(2_700_000L, 3_600_000L)
        );
        return new Object[] {
            new Object[] { 3_600_000L, ranges, asList(ar(1), ar(2), ar(3), ar(4)), new double[][] { { 1, 2, 3, 4 } } },
            new Object[] { 3_600_000L, ranges, asList(ar(), ar(2), ar(3), ar(4)), new double[][] { { 2, 2, 3, 4 } } },
            new Object[] { 3_600_000L, ranges, asList(ar(1), ar(), ar(3), ar(4)), new double[][] { { 1, 3, 3, 4 } } },
            new Object[] { 3_600_000L, ranges, asList(ar(1), ar(2), ar(), ar(4)), new double[][] { { 1, 2, 4, 4 } } },
            new Object[] { 3_600_000L, ranges, asList(ar(1), ar(), ar(), ar(4)), new double[][] { { 1, 1, 4, 4 } } },
            new Object[] { 3_600_000L, ranges, asList(ar(), ar(2), ar(), ar(4)), new double[][] { { 2, 2, 4, 4 } } },
            new Object[] { 3_600_000L, ranges, asList(ar(), ar(), ar(3), ar(4)), null },
            new Object[] { 3_600_000L, ranges, asList(ar(1), empty(), empty(), empty()), null },
            new Object[] { 3_600_000L, ranges, asList(empty(), empty(), empty(), ar(4)), null },
            new Object[] { 3_600_000L, ranges, asList(empty(), empty(), empty(), empty()), null },
            new Object[] { null, null, null, null } };
    }

    @Test
    @SuppressWarnings("unchecked")
    @Parameters(method = "getTrainDataTestData")
    public void getColdStartData_returnExpectedToListener(
        Long latestTime,
        List<Entry<Long, Long>> sampleRanges,
        List<Optional<double[]>> samples,
        double[][] expected
    ) throws Exception {
        long detectionInterval = (new IntervalTimeConfiguration(15, ChronoUnit.MINUTES)).toDuration().toMillis();
        when(detector.getDetectorIntervalInMilliseconds()).thenReturn(detectionInterval);
        when(detector.getShingleSize()).thenReturn(4);
        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.ofNullable(latestTime));
            return null;
        }).when(searchFeatureDao).getLatestDataTime(eq(detector), any(ActionListener.class));
        if (latestTime != null) {
            doAnswer(invocation -> {
                ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(2);
                listener.onResponse(samples);
                return null;
            }).when(searchFeatureDao).getFeatureSamplesForPeriods(eq(detector), eq(sampleRanges), any(ActionListener.class));
        }

        ActionListener<Optional<double[][]>> listener = mock(ActionListener.class);
        featureManager = spy(
            new FeatureManager(
                searchFeatureDao,
                interpolator,
                clock,
                maxTrainSamples,
                maxSampleStride,
                trainSampleTimeRangeInHours,
                minTrainSamples,
                0.5, /*maxMissingPointsRate*/
                1, /*maxNeighborDistance*/
                previewSampleRate,
                maxPreviewSamples,
                featureBufferTtl,
                threadPool,
                AnomalyDetectorPlugin.AD_THREAD_POOL_NAME
            )
        );
        featureManager.getColdStartData(detector, listener);

        ArgumentCaptor<Optional<double[][]>> captor = ArgumentCaptor.forClass(Optional.class);
        verify(listener).onResponse(captor.capture());
        Optional<double[][]> result = captor.getValue();
        assertTrue(Arrays.deepEquals(expected, result.orElse(null)));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getColdStartData_throwToListener_whenSearchFail() {
        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException());
            return null;
        }).when(searchFeatureDao).getLatestDataTime(eq(detector), any(ActionListener.class));

        ActionListener<Optional<double[][]>> listener = mock(ActionListener.class);
        featureManager.getColdStartData(detector, listener);

        verify(listener).onFailure(any(Exception.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getColdStartData_throwToListener_onQueryCreationError() throws Exception {
        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.ofNullable(0L));
            return null;
        }).when(searchFeatureDao).getLatestDataTime(eq(detector), any(ActionListener.class));
        doThrow(IOException.class).when(searchFeatureDao).getFeatureSamplesForPeriods(eq(detector), any(), any(ActionListener.class));

        ActionListener<Optional<double[][]>> listener = mock(ActionListener.class);
        featureManager.getColdStartData(detector, listener);

        verify(listener).onFailure(any(EndRunException.class));
    }

    private Object[] batchShingleData() {
        return new Object[] {
            new Object[] { new double[][] { { 1.0 } }, 1, new double[][] { { 1.0 } } },
            new Object[] { new double[][] { { 1.0, 2.0 } }, 1, new double[][] { { 1.0, 2.0 } } },
            new Object[] { new double[][] { { 1.0 }, { 2, 0 }, { 3.0 } }, 1, new double[][] { { 1.0 }, { 2.0 }, { 3.0 } } },
            new Object[] { new double[][] { { 1.0 }, { 2, 0 }, { 3.0 } }, 2, new double[][] { { 1.0, 2.0 }, { 2.0, 3.0 } } },
            new Object[] { new double[][] { { 1.0 }, { 2, 0 }, { 3.0 } }, 3, new double[][] { { 1.0, 2.0, 3.0 } } },
            new Object[] { new double[][] { { 1.0, 2.0 }, { 3.0, 4.0 } }, 1, new double[][] { { 1.0, 2.0 }, { 3.0, 4.0 } } },
            new Object[] { new double[][] { { 1.0, 2.0 }, { 3.0, 4.0 } }, 2, new double[][] { { 1.0, 2.0, 3.0, 4.0 } } },
            new Object[] {
                new double[][] { { 1.0, 2.0 }, { 3.0, 4.0 }, { 5.0, 6.0 } },
                3,
                new double[][] { { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 } } } };
    };

    @Test
    @Parameters(method = "batchShingleData")
    public void batchShingle_returnExpected(double[][] points, int shingleSize, double[][] expected) {
        assertTrue(Arrays.deepEquals(expected, featureManager.batchShingle(points, shingleSize)));
    }

    private Object[] batchShingleIllegalArgumentData() {
        return new Object[] {
            new Object[] { new double[][] { { 1.0 } }, 0 },
            new Object[] { new double[][] { { 1.0 } }, 2 },
            new Object[] { new double[][] { { 1.0, 2.0 } }, 0 },
            new Object[] { new double[][] { { 1.0, 2.0 } }, 2 },
            new Object[] { new double[][] { { 1.0 }, { 2.0 } }, 0 },
            new Object[] { new double[][] { { 1.0 }, { 2.0 } }, 3 },
            new Object[] { new double[][] {}, 0 },
            new Object[] { new double[][] {}, 1 },
            new Object[] { new double[][] { {}, {} }, 0 },
            new Object[] { new double[][] { {}, {} }, 1 },
            new Object[] { new double[][] { {}, {} }, 2 },
            new Object[] { new double[][] { {}, {} }, 3 }, };
    };

    @Test(expected = IllegalArgumentException.class)
    @Parameters(method = "batchShingleIllegalArgumentData")
    public void batchShingle_throwExpected_forInvalidInput(double[][] points, int shingleSize) {
        featureManager.batchShingle(points, shingleSize);
    }

    @Test
    public void clear_deleteFeatures() throws IOException {
        long start = shingleSize * intervalInMilliseconds;
        long end = (shingleSize + 1) * intervalInMilliseconds;

        AtomicBoolean firstQuery = new AtomicBoolean(true);

        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> daoListener = invocation.getArgument(2);
            if (firstQuery.get()) {
                firstQuery.set(false);
                daoListener
                    .onResponse(asList(Optional.of(new double[] { 3 }), Optional.of(new double[] { 2 }), Optional.of(new double[] { 1 })));
            } else {
                daoListener.onResponse(asList(Optional.ofNullable(null), Optional.ofNullable(null), Optional.of(new double[] { 1 })));
            }
            return null;
        }).when(searchFeatureDao).getFeatureSamplesForPeriods(eq(detector), any(List.class), any(ActionListener.class));
        featureManager.getCurrentFeatures(detector, start, end, mock(ActionListener.class));

        SinglePointFeatures beforeMaintenance = getCurrentFeatures(detector, start, end);
        assertTrue(beforeMaintenance.getUnprocessedFeatures().isPresent());
        assertTrue(beforeMaintenance.getProcessedFeatures().isPresent());

        featureManager.clear(detector.getDetectorId());

        SinglePointFeatures afterMaintenance = getCurrentFeatures(detector, start, end);
        assertTrue(afterMaintenance.getUnprocessedFeatures().isPresent());
        assertFalse(afterMaintenance.getProcessedFeatures().isPresent());
    }

    private SinglePointFeatures getCurrentFeatures(AnomalyDetector detector, long start, long end) throws IOException {
        ActionListener<SinglePointFeatures> listener = mock(ActionListener.class);
        ArgumentCaptor<SinglePointFeatures> captor = ArgumentCaptor.forClass(SinglePointFeatures.class);
        featureManager.getCurrentFeatures(detector, start, end, listener);
        verify(listener).onResponse(captor.capture());
        return captor.getValue();
    }

    @Test
    public void maintenance_removeStaleData() throws IOException {
        long start = shingleSize * intervalInMilliseconds;
        long end = (shingleSize + 1) * intervalInMilliseconds;

        AtomicBoolean firstQuery = new AtomicBoolean(true);

        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> daoListener = invocation.getArgument(2);
            if (firstQuery.get()) {
                firstQuery.set(false);
                daoListener
                    .onResponse(asList(Optional.of(new double[] { 3 }), Optional.of(new double[] { 2 }), Optional.of(new double[] { 1 })));
            } else {
                daoListener.onResponse(asList(Optional.ofNullable(null), Optional.ofNullable(null), Optional.of(new double[] { 1 })));
            }
            return null;
        }).when(searchFeatureDao).getFeatureSamplesForPeriods(eq(detector), any(List.class), any(ActionListener.class));
        featureManager.getCurrentFeatures(detector, start, end, mock(ActionListener.class));

        SinglePointFeatures beforeMaintenance = getCurrentFeatures(detector, start, end);
        assertTrue(beforeMaintenance.getUnprocessedFeatures().isPresent());
        assertTrue(beforeMaintenance.getProcessedFeatures().isPresent());
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(end + 1).plus(featureBufferTtl));

        featureManager.maintenance();

        SinglePointFeatures afterMaintenance = getCurrentFeatures(detector, start, end);
        assertTrue(afterMaintenance.getUnprocessedFeatures().isPresent());
        assertFalse(afterMaintenance.getProcessedFeatures().isPresent());
    }

    @Test
    public void maintenance_keepRecentData() throws IOException {
        long start = shingleSize * intervalInMilliseconds;
        long end = (shingleSize + 1) * intervalInMilliseconds;

        AtomicBoolean firstQuery = new AtomicBoolean(true);

        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> daoListener = invocation.getArgument(2);
            if (firstQuery.get()) {
                firstQuery.set(false);
                daoListener
                    .onResponse(asList(Optional.of(new double[] { 3 }), Optional.of(new double[] { 2 }), Optional.of(new double[] { 1 })));
            } else {
                daoListener.onResponse(asList(Optional.ofNullable(null), Optional.ofNullable(null), Optional.of(new double[] { 1 })));
            }
            return null;
        }).when(searchFeatureDao).getFeatureSamplesForPeriods(eq(detector), any(List.class), any(ActionListener.class));
        featureManager.getCurrentFeatures(detector, start, end, mock(ActionListener.class));

        SinglePointFeatures beforeMaintenance = getCurrentFeatures(detector, start, end);
        assertTrue(beforeMaintenance.getUnprocessedFeatures().isPresent());
        assertTrue(beforeMaintenance.getProcessedFeatures().isPresent());
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(end));

        featureManager.maintenance();

        SinglePointFeatures afterMaintenance = getCurrentFeatures(detector, start, end);
        assertTrue(afterMaintenance.getUnprocessedFeatures().isPresent());
        assertTrue(afterMaintenance.getProcessedFeatures().isPresent());
    }

    @Test
    public void maintenance_doNotThrowException() {
        when(clock.instant()).thenThrow(new RuntimeException());

        featureManager.maintenance();
    }

    @SuppressWarnings("unchecked")
    private void getPreviewFeaturesTemplate(List<Optional<double[]>> samplesResults, boolean querySuccess, boolean previewSuccess)
        throws IOException {
        long start = 0L;
        long end = 240_000L;
        long detectionInterval = (new IntervalTimeConfiguration(1, ChronoUnit.MINUTES)).toDuration().toMillis();
        when(detector.getDetectorIntervalInMilliseconds()).thenReturn(detectionInterval);

        List<Entry<Long, Long>> sampleRanges = Arrays.asList(new SimpleEntry<>(0L, 60_000L), new SimpleEntry<>(120_000L, 180_000L));
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();

            ActionListener<List<Optional<double[]>>> listener = null;

            if (args[2] instanceof ActionListener) {
                listener = (ActionListener<List<Optional<double[]>>>) args[2];
            }

            if (querySuccess) {
                listener.onResponse(samplesResults);
            } else {
                listener.onFailure(new RuntimeException());
            }

            return null;
        }).when(searchFeatureDao).getFeatureSamplesForPeriods(eq(detector), eq(sampleRanges), any());

        when(interpolator.interpolate(argThat(new ArrayEqMatcher<>(new double[][] { { 1, 3 } })), eq(3)))
            .thenReturn(new double[][] { { 1, 2, 3 } });
        when(interpolator.interpolate(argThat(new ArrayEqMatcher<>(new double[][] { { 0, 120000 } })), eq(3)))
            .thenReturn(new double[][] { { 0, 60000, 120000 } });
        when(interpolator.interpolate(argThat(new ArrayEqMatcher<>(new double[][] { { 60000, 180000 } })), eq(3)))
            .thenReturn(new double[][] { { 60000, 120000, 180000 } });

        ActionListener<Features> listener = mock(ActionListener.class);
        featureManager.getPreviewFeatures(detector, start, end, listener);

        if (previewSuccess) {
            Features expected = new Features(
                asList(new SimpleEntry<>(120_000L, 180_000L)),
                new double[][] { { 3 } },
                new double[][] { { 1, 2, 3 } }
            );
            verify(listener).onResponse(expected);
        } else {
            verify(listener).onFailure(any(Exception.class));
        }

    }

    @Test
    public void getPreviewFeatures_returnExpectedToListener() throws IOException {
        getPreviewFeaturesTemplate(asList(Optional.of(new double[] { 1 }), Optional.of(new double[] { 3 })), true, true);
    }

    @Test
    public void getPreviewFeatures_returnExceptionToListener_whenNoDataToPreview() throws IOException {
        getPreviewFeaturesTemplate(asList(), true, false);
    }

    @Test
    public void getPreviewFeatures_returnExceptionToListener_whenQueryFail() throws IOException {
        getPreviewFeaturesTemplate(asList(Optional.of(new double[] { 1 }), Optional.of(new double[] { 3 })), false, false);
    }

    private void setupSearchFeatureDaoForGetCurrentFeatures(
        List<Optional<double[]>> preQueryResponse,
        Optional<List<Optional<double[]>>> testQueryResponse
    ) throws IOException {
        AtomicBoolean isPreQuery = new AtomicBoolean(true);

        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> daoListener = invocation.getArgument(2);
            if (isPreQuery.get()) {
                isPreQuery.set(false);
                daoListener.onResponse(preQueryResponse);
            } else {
                if (testQueryResponse.isPresent()) {
                    daoListener.onResponse(testQueryResponse.get());
                } else {
                    daoListener.onFailure(new IOException());
                }
            }
            return null;
        }).when(searchFeatureDao).getFeatureSamplesForPeriods(eq(detector), any(List.class), any(ActionListener.class));
    }

    private Object[] getCurrentFeaturesTestData_whenAfterQueryResultsFormFullShingle() {
        return new Object[] {
            new Object[] {
                asList(Optional.empty(), Optional.empty(), Optional.empty()),
                3,
                Optional.of(asList(Optional.of(new double[] { 1 }), Optional.of(new double[] { 2 }), Optional.of(new double[] { 3 }))),
                new double[] { 1, 2, 3 } },
            new Object[] {
                asList(Optional.empty(), Optional.of(new double[] { 1 }), Optional.of(new double[] { 5 })),
                1,
                Optional.of(asList(Optional.of(new double[] { 3 }))),
                new double[] { 1, 5, 3 } },
            new Object[] {
                asList(Optional.empty(), Optional.empty(), Optional.of(new double[] { 1, 2 })),
                2,
                Optional.of(asList(Optional.of(new double[] { 3, 4 }), Optional.of(new double[] { 5, 6 }))),
                new double[] { 1, 2, 3, 4, 5, 6 } }, };
    }

    @Test
    @Parameters(method = "getCurrentFeaturesTestData_whenAfterQueryResultsFormFullShingle")
    public void getCurrentFeatures_returnExpectedProcessedFeatures_whenAfterQueryResultsFormFullShingle(
        List<Optional<double[]>> preQueryResponse,
        long intervalOffsetFromPreviousQuery,
        Optional<List<Optional<double[]>>> testQueryResponse,
        double[] expectedProcessedFeatures
    ) throws IOException {
        int expectedNumQueriesToSearchFeatureDao = 2;
        long previousStartTime = shingleSize * intervalInMilliseconds;
        long previousEndTime = previousStartTime + intervalInMilliseconds;
        long testStartTime = previousStartTime + intervalOffsetFromPreviousQuery * intervalInMilliseconds;
        long testEndTime = testStartTime + intervalInMilliseconds;

        // Set up
        setupSearchFeatureDaoForGetCurrentFeatures(preQueryResponse, testQueryResponse);
        featureManager.getCurrentFeatures(detector, previousStartTime, previousEndTime, mock(ActionListener.class));

        // Start test
        SinglePointFeatures listenerResponse = getCurrentFeatures(detector, testStartTime, testEndTime);
        verify(searchFeatureDao, times(expectedNumQueriesToSearchFeatureDao))
            .getFeatureSamplesForPeriods(eq(detector), any(List.class), any(ActionListener.class));
        assertTrue(listenerResponse.getUnprocessedFeatures().isPresent());
        assertTrue(listenerResponse.getProcessedFeatures().isPresent());

        double[] actualProcessedFeatures = listenerResponse.getProcessedFeatures().get();
        for (int i = 0; i < expectedProcessedFeatures.length; i++) {
            assertEquals(expectedProcessedFeatures[i], actualProcessedFeatures[i], 0);
        }
    }

    private Object[] getCurrentFeaturesTestData_whenNoQueryNeededToFormFullShingle() {
        return new Object[] {
            new Object[] {
                asList(Optional.of(new double[] { 1 }), Optional.of(new double[] { 2 }), Optional.of(new double[] { 3 })),
                new double[] { 1, 2, 3 } },
            new Object[] {
                asList(Optional.of(new double[] { 1, 2 }), Optional.of(new double[] { 3, 4 }), Optional.of(new double[] { 5, 6 })),
                new double[] { 1, 2, 3, 4, 5, 6 } } };
    }

    @Test
    @Parameters(method = "getCurrentFeaturesTestData_whenNoQueryNeededToFormFullShingle")
    public void getCurrentFeatures_returnExpectedProcessedFeatures_whenNoQueryNeededToFormFullShingle(
        List<Optional<double[]>> preQueryResponse,
        double[] expectedProcessedFeatures
    ) throws IOException {
        int expectedNumQueriesToSearchFeatureDao = 1;
        long start = shingleSize * intervalInMilliseconds;
        long end = start + intervalInMilliseconds;

        // Set up
        setupSearchFeatureDaoForGetCurrentFeatures(preQueryResponse, Optional.empty());
        featureManager.getCurrentFeatures(detector, start, end, mock(ActionListener.class));

        // Start test
        SinglePointFeatures listenerResponse = getCurrentFeatures(detector, start, end);
        verify(searchFeatureDao, times(expectedNumQueriesToSearchFeatureDao))
            .getFeatureSamplesForPeriods(eq(detector), any(List.class), any(ActionListener.class));
        assertTrue(listenerResponse.getUnprocessedFeatures().isPresent());
        assertTrue(listenerResponse.getProcessedFeatures().isPresent());

        double[] actualProcessedFeatures = listenerResponse.getProcessedFeatures().get();
        for (int i = 0; i < expectedProcessedFeatures.length; i++) {
            assertEquals(expectedProcessedFeatures[i], actualProcessedFeatures[i], 0);
        }
    }

    private Object[] getCurrentFeaturesTestData_whenAfterQueryResultsAllowImputedShingle() {
        return new Object[] {
            new Object[] {
                asList(Optional.empty(), Optional.empty(), Optional.empty()),
                3,
                Optional.of(asList(Optional.of(new double[] { 1 }), Optional.empty(), Optional.of(new double[] { 3 }))),
                new double[] { 1, 3, 3 } },
            new Object[] {
                asList(Optional.of(new double[] { 1 }), Optional.empty(), Optional.of(new double[] { 5 })),
                1,
                Optional.of(asList(Optional.of(new double[] { 3 }))),
                new double[] { 5, 5, 3 } },
            new Object[] {
                asList(Optional.empty(), Optional.of(new double[] { 1 }), Optional.empty()),
                1,
                Optional.of(asList(Optional.of(new double[] { 2 }))),
                new double[] { 1, 2, 2 } },
            new Object[] {
                asList(Optional.empty(), Optional.empty(), Optional.of(new double[] { 1 })),
                2,
                Optional.of(asList(Optional.empty(), Optional.of(new double[] { 2 }))),
                new double[] { 1, 2, 2 } },
            new Object[] {
                asList(Optional.of(new double[] { 5, 6 }), Optional.empty(), Optional.empty()),
                2,
                Optional.of(asList(Optional.of(new double[] { 3, 4 }), Optional.of(new double[] { 1, 2 }))),
                new double[] { 3, 4, 3, 4, 1, 2 } }, };
    }

    @Test
    @Parameters(method = "getCurrentFeaturesTestData_whenAfterQueryResultsAllowImputedShingle")
    public void getCurrentFeatures_returnExpectedProcessedFeatures_whenAfterQueryResultsAllowImputedShingle(
        List<Optional<double[]>> preQueryResponse,
        long intervalOffsetFromPreviousQuery,
        Optional<List<Optional<double[]>>> testQueryResponse,
        double[] expectedProcessedFeatures
    ) throws IOException {
        int expectedNumQueriesToSearchFeatureDao = 2;
        long previousStartTime = (shingleSize + 1) * intervalInMilliseconds;
        long previousEndTime = previousStartTime + intervalInMilliseconds;
        long testStartTime = previousStartTime + (intervalOffsetFromPreviousQuery * intervalInMilliseconds);
        long testEndTime = testStartTime + intervalInMilliseconds;

        // Set up
        setupSearchFeatureDaoForGetCurrentFeatures(preQueryResponse, testQueryResponse);
        featureManager.getCurrentFeatures(detector, previousStartTime, previousEndTime, mock(ActionListener.class));

        // Start test
        SinglePointFeatures listenerResponse = getCurrentFeatures(detector, testStartTime, testEndTime);
        verify(searchFeatureDao, times(expectedNumQueriesToSearchFeatureDao))
            .getFeatureSamplesForPeriods(eq(detector), any(List.class), any(ActionListener.class));
        assertTrue(listenerResponse.getUnprocessedFeatures().isPresent());
        assertTrue(listenerResponse.getProcessedFeatures().isPresent());

        double[] actualProcessedFeatures = listenerResponse.getProcessedFeatures().get();
        for (int i = 0; i < expectedProcessedFeatures.length; i++) {
            assertEquals(expectedProcessedFeatures[i], actualProcessedFeatures[i], 0);
        }
    }

    private Object[] getCurrentFeaturesTestData_whenMissingCurrentDataPoint() {
        return new Object[] {
            new Object[] {
                asList(Optional.empty(), Optional.empty(), Optional.empty()),
                3,
                Optional.of(asList(Optional.of(new double[] { 1 }), Optional.of(new double[] { 3 }), Optional.empty())), },
            new Object[] {
                asList(Optional.of(new double[] { 1 }), Optional.of(new double[] { 1 }), Optional.empty()),
                1,
                Optional.of(asList(Optional.empty())), },
            new Object[] {
                asList(Optional.empty(), Optional.empty(), Optional.of(new double[] { 1, 2, 3 })),
                2,
                Optional.of(asList(Optional.of(new double[] { 4, 5, 6 }), Optional.empty())), } };
    }

    @Test
    @Parameters(method = "getCurrentFeaturesTestData_whenMissingCurrentDataPoint")
    public void getCurrentFeatures_returnNoProcessedOrUnprocessedFeatures_whenMissingCurrentDataPoint(
        List<Optional<double[]>> preQueryResponse,
        long intervalOffsetFromPreviousQuery,
        Optional<List<Optional<double[]>>> testQueryResponse
    ) throws IOException {
        int expectedNumQueriesToSearchFeatureDao = 2;
        long previousStartTime = shingleSize * intervalInMilliseconds;
        long previousEndTime = previousStartTime + intervalInMilliseconds;
        long testStartTime = previousStartTime + intervalOffsetFromPreviousQuery * intervalInMilliseconds;
        long testEndTime = testStartTime + intervalInMilliseconds;

        // Set up
        setupSearchFeatureDaoForGetCurrentFeatures(preQueryResponse, testQueryResponse);
        featureManager.getCurrentFeatures(detector, previousStartTime, previousEndTime, mock(ActionListener.class));

        // Start test
        SinglePointFeatures listenerResponse = getCurrentFeatures(detector, testStartTime, testEndTime);
        verify(searchFeatureDao, times(expectedNumQueriesToSearchFeatureDao))
            .getFeatureSamplesForPeriods(eq(detector), any(List.class), any(ActionListener.class));
        assertFalse(listenerResponse.getUnprocessedFeatures().isPresent());
        assertFalse(listenerResponse.getProcessedFeatures().isPresent());
    }

    private Object[] getCurrentFeaturesTestData_whenAfterQueryResultsCannotBeShingled() {
        return new Object[] {
            new Object[] {
                asList(Optional.of(new double[] { 1 }), Optional.of(new double[] { 2 }), Optional.of(new double[] { 3 })),
                3,
                Optional.of(asList(Optional.empty(), Optional.empty(), Optional.of(new double[] { 4 }))), },
            new Object[] {
                asList(Optional.of(new double[] { 1, 2 }), Optional.empty(), Optional.empty()),
                1,
                Optional.of(asList(Optional.of(new double[] { 3, 4 }))), } };
    }

    @Test
    @Parameters(method = "getCurrentFeaturesTestData_whenAfterQueryResultsCannotBeShingled")
    public void getCurrentFeatures_returnNoProcessedFeatures_whenAfterQueryResultsCannotBeShingled(
        List<Optional<double[]>> preQueryResponse,
        long intervalOffsetFromPreviousQuery,
        Optional<List<Optional<double[]>>> testQueryResponse
    ) throws IOException {
        int expectedNumQueriesToSearchFeatureDao = 2;
        long previousStartTime = shingleSize * intervalInMilliseconds;
        long previousEndTime = previousStartTime + intervalInMilliseconds;
        long testStartTime = previousStartTime + intervalOffsetFromPreviousQuery * intervalInMilliseconds;
        long testEndTime = testStartTime + intervalInMilliseconds;

        // Set up
        setupSearchFeatureDaoForGetCurrentFeatures(preQueryResponse, testQueryResponse);
        featureManager.getCurrentFeatures(detector, previousStartTime, previousEndTime, mock(ActionListener.class));

        // Start test
        SinglePointFeatures listenerResponse = getCurrentFeatures(detector, testStartTime, testEndTime);
        verify(searchFeatureDao, times(expectedNumQueriesToSearchFeatureDao))
            .getFeatureSamplesForPeriods(eq(detector), any(List.class), any(ActionListener.class));
        assertTrue(listenerResponse.getUnprocessedFeatures().isPresent());
        assertFalse(listenerResponse.getProcessedFeatures().isPresent());
    }

    private Object[] getCurrentFeaturesTestData_whenQueryThrowsIOException() {
        return new Object[] {
            new Object[] { asList(Optional.empty(), Optional.empty(), Optional.empty()), 3 },
            new Object[] { asList(Optional.empty(), Optional.of(new double[] { 1, 2 }), Optional.of(new double[] { 3, 4 })), 1 } };
    }

    @Test
    @Parameters(method = "getCurrentFeaturesTestData_whenQueryThrowsIOException")
    public void getCurrentFeatures_returnExceptionToListener_whenQueryThrowsIOException(
        List<Optional<double[]>> preQueryResponse,
        long intervalOffsetFromPreviousQuery
    ) throws IOException {
        int expectedNumQueriesToSearchFeatureDao = 2;
        long previousStartTime = shingleSize * intervalInMilliseconds;
        long previousEndTime = previousStartTime + intervalInMilliseconds;
        long testStartTime = previousStartTime + intervalOffsetFromPreviousQuery * intervalInMilliseconds;
        long testEndTime = testStartTime + intervalInMilliseconds;

        // Set up
        setupSearchFeatureDaoForGetCurrentFeatures(preQueryResponse, Optional.empty());
        featureManager.getCurrentFeatures(detector, previousStartTime, previousEndTime, mock(ActionListener.class));

        // Start test
        ActionListener<SinglePointFeatures> listener = mock(ActionListener.class);
        featureManager.getCurrentFeatures(detector, testStartTime, testEndTime, listener);
        verify(searchFeatureDao, times(expectedNumQueriesToSearchFeatureDao))
            .getFeatureSamplesForPeriods(eq(detector), any(List.class), any(ActionListener.class));
        verify(listener).onFailure(any(IOException.class));
    }

    private Object[] getCurrentFeaturesTestData_cacheMissingData() {
        return new Object[] {
            new Object[] {
                asList(Optional.empty(), Optional.empty(), Optional.empty()),
                Optional.of(asList(Optional.of(new double[] { 1 }))),
                Optional.empty() },
            new Object[] {
                asList(Optional.of(new double[] { 1, 2 }), Optional.empty(), Optional.of(new double[] { 3, 4 })),
                Optional.of(asList(Optional.of(new double[] { 5, 6 }))),
                Optional.of(new double[] { 3, 4, 3, 4, 5, 6 }) } };
    }

    @Test
    @Parameters(method = "getCurrentFeaturesTestData_cacheMissingData")
    public void getCurrentFeatures_returnExpectedFeatures_cacheMissingData(
        List<Optional<double[]>> firstQueryResponseToBeCached,
        Optional<List<Optional<double[]>>> secondQueryResponse,
        Optional<double[]> expectedProcessedFeaturesOptional
    ) throws IOException {
        long firstStartTime = shingleSize * intervalInMilliseconds;
        long firstEndTime = firstStartTime + intervalInMilliseconds;
        long secondStartTime = firstEndTime;
        long secondEndTime = secondStartTime + intervalInMilliseconds;

        setupSearchFeatureDaoForGetCurrentFeatures(firstQueryResponseToBeCached, secondQueryResponse);

        // first call to cache missing points
        featureManager.getCurrentFeatures(detector, firstStartTime, firstEndTime, mock(ActionListener.class));
        verify(searchFeatureDao, times(1))
            .getFeatureSamplesForPeriods(eq(detector), argThat(list -> list.size() == shingleSize), any(ActionListener.class));

        // second call should only fetch current point even if previous points missing
        SinglePointFeatures listenerResponse = getCurrentFeatures(detector, secondStartTime, secondEndTime);
        verify(searchFeatureDao, times(1))
            .getFeatureSamplesForPeriods(eq(detector), argThat(list -> list.size() == 1), any(ActionListener.class));

        assertTrue(listenerResponse.getUnprocessedFeatures().isPresent());
        if (expectedProcessedFeaturesOptional.isPresent()) {
            assertTrue(listenerResponse.getProcessedFeatures().isPresent());
            double[] expectedProcessedFeatures = expectedProcessedFeaturesOptional.get();
            double[] actualProcessedFeatures = listenerResponse.getProcessedFeatures().get();
            for (int i = 0; i < expectedProcessedFeatures.length; i++) {
                assertEquals(expectedProcessedFeatures[i], actualProcessedFeatures[i], 0);
            }
        } else {
            assertFalse(listenerResponse.getProcessedFeatures().isPresent());
        }
    }

    private Object[] getCurrentFeaturesTestData_withTimeJitterUpToHalfInterval() {
        return new Object[] {
            new Object[] {
                asList(Optional.empty(), Optional.empty(), Optional.of(new double[] { 1 })),
                2.1,
                Optional.of(asList(Optional.of(new double[] { 2 }), Optional.of(new double[] { 3 }))),
                new double[] { 1, 2, 3 } },
            new Object[] {
                asList(Optional.of(new double[] { 1 }), Optional.empty(), Optional.of(new double[] { 5 })),
                0.8,
                Optional.of(asList(Optional.of(new double[] { 3 }))),
                new double[] { 5, 5, 3 } },
            new Object[] {
                asList(Optional.empty(), Optional.empty(), Optional.of(new double[] { 1 })),
                1.49,
                Optional.of(asList(Optional.of(new double[] { 2 }))),
                new double[] { 1, 1, 2 } },
            new Object[] {
                asList(Optional.empty(), Optional.empty(), Optional.of(new double[] { 1 })),
                1.51,
                Optional.of(asList(Optional.empty(), Optional.of(new double[] { 2 }))),
                new double[] { 1, 1, 2 } },
            new Object[] {
                asList(Optional.empty(), Optional.empty(), Optional.of(new double[] { 1 })),
                2.49,
                Optional.of(asList(Optional.empty(), Optional.of(new double[] { 2 }))),
                new double[] { 1, 2, 2 } },
            new Object[] {
                asList(Optional.of(new double[] { 1, 2 }), Optional.of(new double[] { 3, 4 }), Optional.of(new double[] { 5, 6 })),
                2.5,
                Optional
                    .of(
                        asList(
                            Optional.of(new double[] { 7, 8 }),
                            Optional.of(new double[] { 9, 10 }),
                            Optional.of(new double[] { 11, 12 })
                        )
                    ),
                new double[] { 7, 8, 9, 10, 11, 12 } }, };
    }

    @Test
    @Parameters(method = "getCurrentFeaturesTestData_withTimeJitterUpToHalfInterval")
    public void getCurrentFeatures_returnExpectedFeatures_withTimeJitterUpToHalfInterval(
        List<Optional<double[]>> preQueryResponse,
        double intervalOffsetFromPreviousQuery,
        Optional<List<Optional<double[]>>> testQueryResponse,
        double[] expectedProcessedFeatures
    ) throws IOException {
        int expectedNumQueriesToSearchFeatureDao = 2;
        long previousStartTime = (shingleSize + 1) * intervalInMilliseconds;
        long previousEndTime = previousStartTime + intervalInMilliseconds;
        double millisecondsOffset = intervalOffsetFromPreviousQuery * intervalInMilliseconds;
        long testStartTime = previousStartTime + (long) millisecondsOffset;
        long testEndTime = testStartTime + intervalInMilliseconds;

        // Set up
        setupSearchFeatureDaoForGetCurrentFeatures(preQueryResponse, testQueryResponse);
        featureManager.getCurrentFeatures(detector, previousStartTime, previousEndTime, mock(ActionListener.class));

        // Start test
        SinglePointFeatures listenerResponse = getCurrentFeatures(detector, testStartTime, testEndTime);
        verify(searchFeatureDao, times(expectedNumQueriesToSearchFeatureDao))
            .getFeatureSamplesForPeriods(eq(detector), any(List.class), any(ActionListener.class));
        assertTrue(listenerResponse.getUnprocessedFeatures().isPresent());
        assertTrue(listenerResponse.getProcessedFeatures().isPresent());

        double[] actualProcessedFeatures = listenerResponse.getProcessedFeatures().get();
        for (int i = 0; i < expectedProcessedFeatures.length; i++) {
            assertEquals(expectedProcessedFeatures[i], actualProcessedFeatures[i], 0);
        }
    }

    private <K, V> Entry<K, V> entry(K key, V value) {
        return new SimpleEntry<>(key, value);
    }

    private Optional<double[]> ar(double... values) {
        if (values.length == 0) {
            return Optional.empty();
        } else {
            return Optional.of(values);
        }
    }

    private Object[] getCurrentFeaturesTestData_setsShingleSizeFromDetectorConfig() {
        return new Object[] { new Object[] { 1 }, new Object[] { 4 }, new Object[] { 8 }, new Object[] { 20 } };
    }

    @Test
    @Parameters(method = "getCurrentFeaturesTestData_setsShingleSizeFromDetectorConfig")
    public void getCurrentFeatures_setsShingleSizeFromDetectorConfig(int shingleSize) throws IOException {
        when(detector.getShingleSize()).thenReturn(shingleSize);

        doAnswer(invocation -> {
            List<Entry<Long, Long>> ranges = invocation.getArgument(1);
            assertEquals(ranges.size(), shingleSize);

            ActionListener<List<Optional<double[]>>> daoListener = invocation.getArgument(2);
            List<Optional<double[]>> response = new ArrayList<Optional<double[]>>();
            for (int i = 0; i < ranges.size(); i++) {
                response.add(Optional.of(new double[] { i }));
            }
            daoListener.onResponse(response);
            return null;
        }).when(searchFeatureDao).getFeatureSamplesForPeriods(eq(detector), any(List.class), any(ActionListener.class));

        SinglePointFeatures listenerResponse = getCurrentFeatures(detector, 0, intervalInMilliseconds);
        assertTrue(listenerResponse.getProcessedFeatures().isPresent());
        assertEquals(listenerResponse.getProcessedFeatures().get().length, shingleSize);
        assertEquals(featureManager.getShingleSize(detector.getDetectorId()), shingleSize);
    }
}
