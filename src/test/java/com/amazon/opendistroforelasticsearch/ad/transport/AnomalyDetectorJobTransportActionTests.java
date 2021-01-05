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

package com.amazon.opendistroforelasticsearch.ad.transport;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.START_JOB;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import com.amazon.opendistroforelasticsearch.ad.HistoricalDetectorIntegTestCase;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskState;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.DetectionDateRange;
import com.amazon.opendistroforelasticsearch.ad.plugin.MockReindexPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2)
public class AnomalyDetectorJobTransportActionTests extends HistoricalDetectorIntegTestCase {
    private Instant startTime;
    private Instant endTime;
    private String type = "error";
    private int maxOldAdTaskDocsPerDetector = 2;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        startTime = Instant.now().minus(10, ChronoUnit.DAYS);
        endTime = Instant.now();
        ingestTestData(testIndex, startTime, detectionIntervalInMinutes, type, 2000);
        createDetectorIndex();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings
            .builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1)
            .put(MAX_BATCH_TASK_PER_NODE.getKey(), 1)
            .put(MAX_OLD_AD_TASK_DOCS_PER_DETECTOR.getKey(), maxOldAdTaskDocsPerDetector)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        final ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.add(MockReindexPlugin.class);
        plugins.addAll(super.getMockPlugins());
        return Collections.unmodifiableList(plugins);
    }

    public void testDetectorIndexNotFound() {
        deleteDetectorIndex();
        String detectorId = randomAlphaOfLength(5);
        AnomalyDetectorJobRequest request = new AnomalyDetectorJobRequest(
            detectorId,
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            START_JOB
        );
        IndexNotFoundException exception = expectThrows(
            IndexNotFoundException.class,
            () -> client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(3000)
        );
        assertTrue(exception.getMessage().contains("no such index [.opendistro-anomaly-detectors]"));
    }

    public void testDetectorNotFound() {
        String detectorId = randomAlphaOfLength(5);
        AnomalyDetectorJobRequest request = new AnomalyDetectorJobRequest(
            detectorId,
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            START_JOB
        );
        ElasticsearchStatusException exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(3000)
        );
        assertTrue(exception.getMessage().contains("AnomalyDetector is not found"));
    }

    public void testValidHistoricalDetector() throws IOException, InterruptedException {
        DetectionDateRange dateRange = new DetectionDateRange(startTime, endTime);
        AnomalyDetector detector = TestHelpers
            .randomDetector(dateRange, ImmutableList.of(maxValueFeature()), testIndex, detectionIntervalInMinutes, timeField);
        String detectorId = createDetector(detector);
        AnomalyDetectorJobRequest request = new AnomalyDetectorJobRequest(
            detectorId,
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            START_JOB
        );
        AnomalyDetectorJobResponse response = client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000);
        Thread.sleep(10000);
        GetResponse doc = getDoc(ADTask.DETECTION_STATE_INDEX, response.getId());
        assertEquals(ADTaskState.FINISHED.name(), doc.getSourceAsMap().get(ADTask.STATE_FIELD));
    }

    public void testRunMultipleTasksForHistoricalDetector() throws IOException, InterruptedException {
        DetectionDateRange dateRange = new DetectionDateRange(startTime, endTime);
        AnomalyDetector detector = TestHelpers
            .randomDetector(dateRange, ImmutableList.of(maxValueFeature()), testIndex, detectionIntervalInMinutes, timeField);
        String detectorId = createDetector(detector);
        AnomalyDetectorJobRequest request = new AnomalyDetectorJobRequest(
            detectorId,
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            START_JOB
        );
        AnomalyDetectorJobResponse response = client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000);
        assertNotNull(response.getId());
        ElasticsearchStatusException exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000)
        );
        assertTrue(exception.getMessage().contains("Detector is already running"));
    }

    public void testCleanOldTaskDocs() throws IOException, InterruptedException {
        updateTransientSettings(ImmutableMap.of(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1));
        DetectionDateRange dateRange = new DetectionDateRange(startTime, endTime);
        AnomalyDetector detector = TestHelpers
            .randomDetector(dateRange, ImmutableList.of(maxValueFeature()), testIndex, detectionIntervalInMinutes, timeField);
        String detectorId = createDetector(detector);

        createDetectionStateIndex();
        List<ADTaskState> states = ImmutableList.of(ADTaskState.FAILED, ADTaskState.FINISHED, ADTaskState.STOPPED);
        for (ADTaskState state : states) {
            ADTask task = randomADTask(randomAlphaOfLength(5), detector, detectorId, state);
            createADTask(task);
        }
        long count = countDocs(ADTask.DETECTION_STATE_INDEX);
        assertEquals(states.size(), count);

        AnomalyDetectorJobRequest request = new AnomalyDetectorJobRequest(detectorId, randomLong(), randomLong(), START_JOB);
        AtomicReference<AnomalyDetectorJobResponse> response = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        client().execute(AnomalyDetectorJobAction.INSTANCE, request, ActionListener.wrap(r -> {
            latch.countDown();
            response.set(r);
        }, e -> { latch.countDown(); }));
        latch.await();

        count = countDocs(ADTask.DETECTION_STATE_INDEX);
        // we have one latest task, so total count should add 1
        assertEquals(maxOldAdTaskDocsPerDetector + 1, count);
    }

    public void testStartRealtimeDetector() throws IOException {
        AnomalyDetector detector = TestHelpers
            .randomDetector(null, ImmutableList.of(maxValueFeature()), testIndex, detectionIntervalInMinutes, timeField);
        String detectorId = createDetector(detector);
        AnomalyDetectorJobRequest request = new AnomalyDetectorJobRequest(
            detectorId,
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            START_JOB
        );
        AnomalyDetectorJobResponse response = client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000);
        assertEquals(detectorId, response.getId());
        GetResponse doc = getDoc(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX, detectorId);
        AnomalyDetectorJob job = toADJob(doc);
        assertTrue(job.isEnabled());
        assertEquals(detectorId, job.getName());
    }

    public void testRealtimeDetectorWithoutFeature() throws IOException {
        AnomalyDetector detector = TestHelpers.randomDetector(null, ImmutableList.of(), testIndex, detectionIntervalInMinutes, timeField);
        testInvalidDetector(detector, "Can't start detector job as no features configured");
    }

    public void testHistoricalDetectorWithoutFeature() throws IOException {
        AnomalyDetector detector = TestHelpers
            .randomDetector(
                new DetectionDateRange(startTime, endTime),
                ImmutableList.of(),
                testIndex,
                detectionIntervalInMinutes,
                timeField
            );
        testInvalidDetector(detector, "Can't start detector job as no features configured");
    }

    public void testRealtimeDetectorWithoutEnabledFeature() throws IOException {
        AnomalyDetector detector = TestHelpers
            .randomDetector(null, ImmutableList.of(TestHelpers.randomFeature(false)), testIndex, detectionIntervalInMinutes, timeField);
        testInvalidDetector(detector, "Can't start detector job as no enabled features configured");
    }

    public void testHistoricalDetectorWithoutEnabledFeature() throws IOException {
        AnomalyDetector detector = TestHelpers
            .randomDetector(
                new DetectionDateRange(startTime, endTime),
                ImmutableList.of(TestHelpers.randomFeature(false)),
                testIndex,
                detectionIntervalInMinutes,
                timeField
            );
        testInvalidDetector(detector, "Can't start detector job as no enabled features configured");
    }

    private void testInvalidDetector(AnomalyDetector detector, String error) throws IOException {
        String detectorId = createDetector(detector);
        AnomalyDetectorJobRequest request = new AnomalyDetectorJobRequest(
            detectorId,
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            START_JOB
        );
        ElasticsearchStatusException exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000)
        );
        assertEquals(error, exception.getMessage());
    }
}
