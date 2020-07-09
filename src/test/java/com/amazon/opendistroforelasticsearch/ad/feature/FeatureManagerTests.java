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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.elasticsearch.action.ActionListener;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
import com.amazon.opendistroforelasticsearch.ad.dataprocessor.Interpolator;
import com.amazon.opendistroforelasticsearch.ad.dataprocessor.LinearUniformInterpolator;
import com.amazon.opendistroforelasticsearch.ad.dataprocessor.SingleFeatureLinearUniformInterpolator;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.transport.ADStateManager;
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
    private int maxMissingPoints;
    private int maxNeighborDistance;
    private double previewSampleRate;
    private int maxPreviewSamples;
    private Duration featureBufferTtl;

    @Mock
    private AnomalyDetector detector;

    @Mock
    private SearchFeatureDao searchFeatureDao;

    @Mock
    private Interpolator interpolator;

    @Mock
    private Clock clock;

    @Mock
    private ADStateManager stateManager;

    private FeatureManager featureManager;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        maxTrainSamples = 24;
        maxSampleStride = 100;
        trainSampleTimeRangeInHours = 1;
        minTrainSamples = 4;
        shingleSize = 3;
        maxMissingPoints = 2;
        maxNeighborDistance = 2;
        previewSampleRate = 0.5;
        maxPreviewSamples = 2;
        featureBufferTtl = Duration.ofMillis(1_000L);

        when(detector.getDetectorId()).thenReturn("id");
        when(detector.getDetectionInterval()).thenReturn(new IntervalTimeConfiguration(1, ChronoUnit.MINUTES));

        Interpolator interpolator = new LinearUniformInterpolator(new SingleFeatureLinearUniformInterpolator());
        this.featureManager = spy(
            new FeatureManager(
                searchFeatureDao,
                interpolator,
                clock,
                maxTrainSamples,
                maxSampleStride,
                trainSampleTimeRangeInHours,
                minTrainSamples,
                shingleSize,
                maxMissingPoints,
                maxNeighborDistance,
                previewSampleRate,
                maxPreviewSamples,
                featureBufferTtl
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
        when(detector.getDetectionInterval()).thenReturn(new IntervalTimeConfiguration(15, ChronoUnit.MINUTES));
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
                4, /*shingleSize*/
                2, /*maxMissingPoints*/
                1, /*maxNeighborDistance*/
                previewSampleRate,
                maxPreviewSamples,
                featureBufferTtl
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
    public void clear_deleteFeatures() {
        long start = 0;
        long end = 0;
        for (int i = 1; i <= shingleSize; i++) {
            start = i * 10;
            end = (i + 1) * 10;

            doAnswer(invocation -> {
                ActionListener<Optional<double[]>> daoListener = invocation.getArgument(3);
                daoListener.onResponse(Optional.of(new double[] { 1 }));
                return null;
            }).when(searchFeatureDao).getFeaturesForPeriod(eq(detector), eq(start), eq(end), any(ActionListener.class));
            featureManager.getCurrentFeatures(detector, start, end, mock(ActionListener.class));
        }
        SinglePointFeatures beforeMaintenance = getCurrentFeatures(detector, start, end);
        assertTrue(beforeMaintenance.getUnprocessedFeatures().isPresent());
        assertTrue(beforeMaintenance.getProcessedFeatures().isPresent());

        featureManager.clear(detector.getDetectorId());

        SinglePointFeatures afterMaintenance = getCurrentFeatures(detector, start, end);
        assertTrue(afterMaintenance.getUnprocessedFeatures().isPresent());
        assertFalse(afterMaintenance.getProcessedFeatures().isPresent());
    }

    private SinglePointFeatures getCurrentFeatures(AnomalyDetector detector, long start, long end) {
        ActionListener<SinglePointFeatures> listener = mock(ActionListener.class);
        ArgumentCaptor<SinglePointFeatures> captor = ArgumentCaptor.forClass(SinglePointFeatures.class);
        featureManager.getCurrentFeatures(detector, start, end, listener);
        verify(listener).onResponse(captor.capture());
        return captor.getValue();
    }

    @Test
    public void maintenance_removeStaleData() {
        long start = 0;
        long end = 0;
        for (int i = 1; i <= shingleSize; i++) {
            start = i * 10;
            end = (i + 1) * 10;

            doAnswer(invocation -> {
                ActionListener<Optional<double[]>> daoListener = invocation.getArgument(3);
                daoListener.onResponse(Optional.of(new double[] { 1 }));
                return null;
            }).when(searchFeatureDao).getFeaturesForPeriod(eq(detector), eq(start), eq(end), any(ActionListener.class));
            featureManager.getCurrentFeatures(detector, start, end, mock(ActionListener.class));
        }
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
    public void maintenance_keepRecentData() {
        long start = 0;
        long end = 0;
        for (int i = 1; i <= shingleSize; i++) {
            start = i * 10;
            end = (i + 1) * 10;

            doAnswer(invocation -> {
                ActionListener<Optional<double[]>> daoListener = invocation.getArgument(3);
                daoListener.onResponse(Optional.of(new double[] { 1 }));
                return null;
            }).when(searchFeatureDao).getFeaturesForPeriod(eq(detector), eq(start), eq(end), any(ActionListener.class));
            when(searchFeatureDao.getFeaturesForPeriod(detector, start, end)).thenReturn(Optional.of(new double[] { i }));
            featureManager.getCurrentFeatures(detector, start, end, mock(ActionListener.class));
        }
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
        IntervalTimeConfiguration detectionInterval = new IntervalTimeConfiguration(1, ChronoUnit.MINUTES);
        when(detector.getDetectionInterval()).thenReturn(detectionInterval);

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
}
