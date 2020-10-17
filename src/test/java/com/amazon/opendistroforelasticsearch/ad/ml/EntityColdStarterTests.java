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

package com.amazon.opendistroforelasticsearch.ad.ml;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.ThreadPool;

import test.com.amazon.opendistroforelasticsearch.ad.util.MLUtil;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.NodeStateManager;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.dataprocessor.IntegerSensitiveSingleFeatureLinearUniformInterpolator;
import com.amazon.opendistroforelasticsearch.ad.dataprocessor.Interpolator;
import com.amazon.opendistroforelasticsearch.ad.dataprocessor.LinearUniformInterpolator;
import com.amazon.opendistroforelasticsearch.ad.dataprocessor.SingleFeatureLinearUniformInterpolator;
import com.amazon.opendistroforelasticsearch.ad.feature.FeatureManager;
import com.amazon.opendistroforelasticsearch.ad.feature.SearchFeatureDao;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager.ModelType;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.randomcutforest.RandomCutForest;

public class EntityColdStarterTests extends AbstractADTest {
    int numMinSamples;
    String modelId;
    String entityName;
    String detectorId;
    ModelState<EntityModel> modelState;
    Clock clock;
    float priority;
    EntityColdStarter entityColdStarter;
    NodeStateManager stateManager;
    SearchFeatureDao searchFeatureDao;
    Interpolator interpolator;
    CheckpointDao checkpoint;
    FeatureManager featureManager;
    Settings settings;
    ThreadPool threadPool;

    @SuppressWarnings("unchecked")
    @Override
    public void setUp() throws Exception {
        super.setUp();
        numMinSamples = AnomalyDetectorSettings.NUM_MIN_SAMPLES;

        clock = mock(Clock.class);
        when(clock.instant()).thenReturn(Instant.now());

        threadPool = mock(ThreadPool.class);
        setUpADThreadPool(threadPool);

        settings = Settings.EMPTY;

        Client client = mock(Client.class);
        ClientUtil clientUtil = mock(ClientUtil.class);

        String categoryField = "a";
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetectorUsingCategoryFields(detectorId, Collections.singletonList(categoryField));
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(2);
            listener.onResponse(TestHelpers.createGetResponse(detector, detectorId, AnomalyDetector.ANOMALY_DETECTORS_INDEX));
            return null;
            // }).when(clientUtil).asyncRequest(eq(new GetRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX, detectorId)), any(),
            // any(ActionListener.class));
        }).when(clientUtil).asyncRequest(any(GetRequest.class), any(), any(ActionListener.class));

        ModelPartitioner modelPartitioner = mock(ModelPartitioner.class);
        stateManager = new NodeStateManager(
            client,
            xContentRegistry(),
            settings,
            clientUtil,
            clock,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            modelPartitioner
        );

        SingleFeatureLinearUniformInterpolator singleFeatureLinearUniformInterpolator =
            new IntegerSensitiveSingleFeatureLinearUniformInterpolator();
        interpolator = new LinearUniformInterpolator(singleFeatureLinearUniformInterpolator);

        searchFeatureDao = mock(SearchFeatureDao.class);
        checkpoint = mock(CheckpointDao.class);

        featureManager = new FeatureManager(
            searchFeatureDao,
            interpolator,
            clock,
            AnomalyDetectorSettings.MAX_TRAIN_SAMPLE,
            AnomalyDetectorSettings.MAX_SAMPLE_STRIDE,
            AnomalyDetectorSettings.TRAIN_SAMPLE_TIME_RANGE_IN_HOURS,
            AnomalyDetectorSettings.MIN_TRAIN_SAMPLES,
            AnomalyDetectorSettings.MAX_SHINGLE_PROPORTION_MISSING,
            AnomalyDetectorSettings.MAX_IMPUTATION_NEIGHBOR_DISTANCE,
            AnomalyDetectorSettings.PREVIEW_SAMPLE_RATE,
            AnomalyDetectorSettings.MAX_PREVIEW_SAMPLES,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            threadPool,
            AnomalyDetectorPlugin.AD_THREAD_POOL_NAME
        );

        entityColdStarter = new EntityColdStarter(
            clock,
            threadPool,
            stateManager,
            AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE,
            AnomalyDetectorSettings.MULTI_ENTITY_NUM_TREES,
            AnomalyDetectorSettings.TIME_DECAY,
            numMinSamples,
            AnomalyDetectorSettings.MAX_SAMPLE_STRIDE,
            AnomalyDetectorSettings.MAX_TRAIN_SAMPLE,
            interpolator,
            searchFeatureDao,
            AnomalyDetectorSettings.DEFAULT_MULTI_ENTITY_SHINGLE,
            AnomalyDetectorSettings.THRESHOLD_MIN_PVALUE,
            AnomalyDetectorSettings.THRESHOLD_MAX_RANK_ERROR,
            AnomalyDetectorSettings.THRESHOLD_MAX_SCORE,
            AnomalyDetectorSettings.THRESHOLD_NUM_LOGNORMAL_QUANTILES,
            AnomalyDetectorSettings.THRESHOLD_DOWNSAMPLES,
            AnomalyDetectorSettings.THRESHOLD_MAX_SAMPLES,
            featureManager,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            AnomalyDetectorSettings.MAX_SMALL_STATES,
            checkpoint,
            settings
        );

        detectorId = "123";
        modelId = "123_entity_abc";
        entityName = "abc";
        priority = 0.3f;
    }

    // train using samples directly
    public void testTrainUsingSamples() {
        Queue<double[]> samples = MLUtil.createQueueSamples(numMinSamples);
        EntityModel model = new EntityModel(modelId, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);
        entityColdStarter.trainModel(samples, modelId, entityName, detectorId, modelState);
        RandomCutForest forest = model.getRcf();
        assertTrue(forest != null);
        assertEquals(numMinSamples, forest.getTotalUpdates());
        assertTrue(model.getThreshold() != null);
    }

    public void testColdStart() throws InterruptedException, IOException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(modelId, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = invocation.getArgument(2);
            listener.onResponse(new SimpleImmutableEntry<>(Optional.of(1602269260000L), Optional.of(1602401500000L)));
            return null;
        }).when(searchFeatureDao).getEntityMinMaxDataTime(any(), any(), any());

        List<Optional<double[]>> coldStartSamples = new ArrayList<>();
        coldStartSamples.add(Optional.of(new double[] { 57.0 }));
        coldStartSamples.add(Optional.of(new double[] { 1.0 }));
        coldStartSamples.add(Optional.of(new double[] { -19.0 }));
        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(3);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), any());

        entityColdStarter.trainModel(samples, modelId, entityName, detectorId, modelState);

        waitForColdStartFinish();
        RandomCutForest forest = model.getRcf();
        assertTrue(forest != null);
        // maxSampleStride * (continuousSampledArray.length - 1) + 1 = 64 * 2 + 1 = 129
        assertEquals(129, forest.getTotalUpdates());
        assertTrue(model.getThreshold() != null);

        // sleep 1 secs to give time for the last timestamp record to expire when superShortLastColdStartTimeState = true
        Thread.sleep(1000L);

        // too frequent cold start of the same detector will fail
        samples = MLUtil.createQueueSamples(1);
        model = new EntityModel(modelId, samples, null, null);
        entityColdStarter.trainModel(samples, modelId, entityName, detectorId, modelState);
        waitForColdStartFinish();

        forest = model.getRcf();

        assertTrue(forest == null);
        assertTrue(model.getThreshold() == null);
    }

    private void waitForColdStartFinish() throws InterruptedException {
        int maxWaitTimes = 20;
        int i = 0;
        while (stateManager.isColdStartRunning(detectorId) && i < maxWaitTimes) {
            // wait for 1 second
            Thread.sleep(500L);
            i++;
        }
    }

    // cold start running, return immediately
    public void testColdStartRunning() {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(modelId, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        NodeStateManager spyNodeStateManager = spy(stateManager);
        spyNodeStateManager.markColdStartRunning(detectorId);
        entityColdStarter.trainModel(samples, modelId, entityName, detectorId, modelState);

        verify(spyNodeStateManager, never()).getAnomalyDetector(any(), any());
    }

    // min max: miss one
    public void testMissMin() throws IOException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(modelId, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = invocation.getArgument(2);
            listener.onResponse(new SimpleImmutableEntry<>(Optional.empty(), Optional.of(1602401500000L)));
            return null;
        }).when(searchFeatureDao).getEntityMinMaxDataTime(any(), any(), any());

        entityColdStarter.trainModel(samples, modelId, entityName, detectorId, modelState);

        verify(searchFeatureDao, never()).getColdStartSamplesForPeriods(any(), any(), any(), any());

        RandomCutForest forest = model.getRcf();
        assertTrue(forest == null);
        assertTrue(model.getThreshold() == null);
    }

    // two segments of samples, one segment has 3 samples, while another one has only 1
    public void testTwoSegmentsWithSingleSample() throws InterruptedException, IOException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(modelId, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = invocation.getArgument(2);
            listener.onResponse(new SimpleImmutableEntry<>(Optional.of(1602269260000L), Optional.of(1602401500000L)));
            return null;
        }).when(searchFeatureDao).getEntityMinMaxDataTime(any(), any(), any());

        List<Optional<double[]>> coldStartSamples = new ArrayList<>();
        coldStartSamples.add(Optional.of(new double[] { 57.0 }));
        coldStartSamples.add(Optional.of(new double[] { 1.0 }));
        coldStartSamples.add(Optional.of(new double[] { -19.0 }));
        coldStartSamples.add(Optional.empty());
        coldStartSamples.add(Optional.of(new double[] { -17.0 }));
        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(3);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), any());

        entityColdStarter.trainModel(samples, modelId, entityName, detectorId, modelState);

        int maxWaitTimes = 20;
        int i = 0;
        while (stateManager.isColdStartRunning(detectorId) && i < maxWaitTimes) {
            // wait for 1 second
            Thread.sleep(500L);
            i++;
        }
        RandomCutForest forest = model.getRcf();
        assertTrue(forest != null);
        // 1st segment: maxSampleStride * (continuousSampledArray.length - 1) + 1 = 64 * 2 + 1 = 129
        // 2nd segment: 1
        assertEquals(130, forest.getTotalUpdates());
        assertTrue(model.getThreshold() != null);
    }

    // two segments of samples, one segment has 3 samples, while another one 2 samples
    public void testTwoSegments() throws InterruptedException, IOException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(modelId, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = invocation.getArgument(2);
            listener.onResponse(new SimpleImmutableEntry<>(Optional.of(1602269260000L), Optional.of(1602401500000L)));
            return null;
        }).when(searchFeatureDao).getEntityMinMaxDataTime(any(), any(), any());

        List<Optional<double[]>> coldStartSamples = new ArrayList<>();
        coldStartSamples.add(Optional.of(new double[] { 57.0 }));
        coldStartSamples.add(Optional.of(new double[] { 1.0 }));
        coldStartSamples.add(Optional.of(new double[] { -19.0 }));
        coldStartSamples.add(Optional.empty());
        coldStartSamples.add(Optional.of(new double[] { -17.0 }));
        coldStartSamples.add(Optional.of(new double[] { -38.0 }));
        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(3);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), any());

        entityColdStarter.trainModel(samples, modelId, entityName, detectorId, modelState);

        int maxWaitTimes = 20;
        int i = 0;
        while (stateManager.isColdStartRunning(detectorId) && i < maxWaitTimes) {
            // wait for 1 second
            Thread.sleep(500L);
            i++;
        }
        RandomCutForest forest = model.getRcf();
        assertTrue(forest != null);
        // 1st segment: maxSampleStride * (continuousSampledArray.length - 1) + 1 = 64 * 2 + 1 = 129
        // 2nd segment: maxSampleStride * (continuousSampledArray.length - 1) + 1 = 64 * 1 + 1 = 65
        assertEquals(194, forest.getTotalUpdates());
        assertTrue(model.getThreshold() != null);
    }

    public void testThrottledColdStart() {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(modelId, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = invocation.getArgument(2);
            listener.onFailure(new EsRejectedExecutionException(""));
            return null;
        }).when(searchFeatureDao).getEntityMinMaxDataTime(any(), any(), any());

        entityColdStarter.trainModel(samples, modelId, entityName, detectorId, modelState);

        entityColdStarter.trainModel(samples, modelId, entityName, "456", modelState);

        // only the first one makes the call
        verify(searchFeatureDao, times(1)).getEntityMinMaxDataTime(any(), any(), any());
    }

    public void testColdStartException() {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(modelId, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = invocation.getArgument(2);
            listener.onFailure(new AnomalyDetectionException(detectorId, ""));
            return null;
        }).when(searchFeatureDao).getEntityMinMaxDataTime(any(), any(), any());

        entityColdStarter.trainModel(samples, modelId, entityName, detectorId, modelState);

        assertTrue(stateManager.getLastDetectionError(detectorId) != null);
    }

    public void testNotEnoughSamples() throws InterruptedException, IOException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(modelId, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = invocation.getArgument(2);
            listener.onResponse(new SimpleImmutableEntry<>(Optional.of(1602269260000L), Optional.of(1602401500000L)));
            return null;
        }).when(searchFeatureDao).getEntityMinMaxDataTime(any(), any(), any());

        List<Optional<double[]>> coldStartSamples = new ArrayList<>();
        coldStartSamples.add(Optional.of(new double[] { 57.0 }));
        coldStartSamples.add(Optional.of(new double[] { 1.0 }));
        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(3);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), any());

        entityColdStarter.trainModel(samples, modelId, entityName, detectorId, modelState);

        int maxWaitTimes = 20;
        int i = 0;
        while (stateManager.isColdStartRunning(detectorId) && i < maxWaitTimes) {
            // wait for 1 second
            Thread.sleep(500L);
            i++;
        }
        assertTrue(model.getRcf() == null);
        assertTrue(model.getThreshold() == null);
        // 1st segment: maxSampleStride * (continuousSampledArray.length - 1) + 1 = 64 * 1 + 1 = 65
        // 65 + origin 1 data points
        assertEquals(66, model.getSamples().size());

    }

}
