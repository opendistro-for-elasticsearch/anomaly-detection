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
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.transport.RemoteTransportException;

import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ResourceNotFoundException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorInternalState;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfile;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfileName;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorState;
import com.amazon.opendistroforelasticsearch.ad.model.InitProgressProfile;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.model.ModelProfile;
import com.amazon.opendistroforelasticsearch.ad.transport.ProfileAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ProfileNodeResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.ProfileResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.RCFPollingAction;
import com.amazon.opendistroforelasticsearch.ad.transport.RCFPollingResponse;

public class AnomalyDetectorProfileRunnerTests extends AbstractProfileRunnerTests {
    enum RCFPollingStatus {
        INIT_NOT_EXIT,
        REMOTE_INIT_NOT_EXIT,
        INDEX_NOT_FOUND,
        REMOTE_INDEX_NOT_FOUND,
        INIT_DONE,
        EMPTY,
        EXCEPTION,
        INITTING
    }

    /**
     * Convenience methods for single-stream detector profile tests set up
     * @param detectorStatus Detector config status
     * @param jobStatus Detector job status
     * @param rcfPollingStatus RCF polling result status
     * @param errorResultStatus Error result status
     * @throws IOException when failing the getting request
     */
    @SuppressWarnings("unchecked")
    private void setUpClientGet(
        DetectorStatus detectorStatus,
        JobStatus jobStatus,
        RCFPollingStatus rcfPollingStatus,
        ErrorResultStatus errorResultStatus
    ) throws IOException {
        detector = TestHelpers.randomAnomalyDetectorWithInterval(new IntervalTimeConfiguration(detectorIntervalMin, ChronoUnit.MINUTES));

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            GetRequest request = (GetRequest) args[0];
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            if (request.index().equals(ANOMALY_DETECTORS_INDEX)) {
                switch (detectorStatus) {
                    case EXIST:
                        listener
                            .onResponse(
                                TestHelpers.createGetResponse(detector, detector.getDetectorId(), AnomalyDetector.ANOMALY_DETECTORS_INDEX)
                            );
                        break;
                    case INDEX_NOT_EXIST:
                        listener.onFailure(new IndexNotFoundException(ANOMALY_DETECTORS_INDEX));
                        break;
                    case NO_DOC:
                        when(detectorGetReponse.isExists()).thenReturn(false);
                        listener.onResponse(detectorGetReponse);
                        break;
                    default:
                        assertTrue("should not reach here", false);
                        break;
                }
            } else if (request.index().equals(ANOMALY_DETECTOR_JOB_INDEX)) {
                AnomalyDetectorJob job = null;
                switch (jobStatus) {
                    case INDEX_NOT_EXIT:
                        listener.onFailure(new IndexNotFoundException(ANOMALY_DETECTOR_JOB_INDEX));
                        break;
                    case DISABLED:
                        job = TestHelpers.randomAnomalyDetectorJob(false);
                        listener
                            .onResponse(
                                TestHelpers.createGetResponse(job, detector.getDetectorId(), AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX)
                            );
                        break;
                    case ENABLED:
                        job = TestHelpers.randomAnomalyDetectorJob(true);
                        listener
                            .onResponse(
                                TestHelpers.createGetResponse(job, detector.getDetectorId(), AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX)
                            );
                        break;
                    default:
                        assertTrue("should not reach here", false);
                        break;
                }
            } else {
                if (errorResultStatus == ErrorResultStatus.INDEX_NOT_EXIT) {
                    listener.onFailure(new IndexNotFoundException(DetectorInternalState.DETECTOR_STATE_INDEX));
                    return null;
                }
                DetectorInternalState.Builder result = new DetectorInternalState.Builder().lastUpdateTime(Instant.now());

                switch (errorResultStatus) {
                    case NO_ERROR:
                        break;
                    case SHINGLE_ERROR:
                        result.error(noFullShingleError);
                        break;
                    case STOPPED_ERROR:
                        result.error(stoppedError);
                        break;
                    default:
                        assertTrue("should not reach here", false);
                        break;
                }
                listener
                    .onResponse(
                        TestHelpers.createGetResponse(result.build(), detector.getDetectorId(), DetectorInternalState.DETECTOR_STATE_INDEX)
                    );

            }

            return null;
        }).when(client).get(any(), any());

        setUpClientExecuteRCFPollingAction(rcfPollingStatus);
    }

    public void testDetectorNotExist() throws IOException, InterruptedException {
        setUpClientGet(DetectorStatus.INDEX_NOT_EXIST, JobStatus.INDEX_NOT_EXIT, RCFPollingStatus.EMPTY, ErrorResultStatus.NO_ERROR);
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile("x123", ActionListener.wrap(response -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue(exception.getMessage().contains(CommonErrorMessages.FAIL_TO_FIND_DETECTOR_MSG));
            inProgressLatch.countDown();
        }), stateNError);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testDisabledJobIndexTemplate(JobStatus status) throws IOException, InterruptedException {
        setUpClientGet(DetectorStatus.EXIST, status, RCFPollingStatus.EMPTY, ErrorResultStatus.NO_ERROR);
        DetectorProfile expectedProfile = new DetectorProfile.Builder().state(DetectorState.DISABLED).build();
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile(detector.getDetectorId(), ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Should not reach here ", false);
            inProgressLatch.countDown();
        }), stateOnly);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testNoJobIndex() throws IOException, InterruptedException {
        testDisabledJobIndexTemplate(JobStatus.INDEX_NOT_EXIT);
    }

    public void testJobDisabled() throws IOException, InterruptedException {
        testDisabledJobIndexTemplate(JobStatus.DISABLED);
    }

    public void testInitOrRunningStateTemplate(RCFPollingStatus status, DetectorState expectedState) throws IOException,
        InterruptedException {
        setUpClientGet(DetectorStatus.EXIST, JobStatus.ENABLED, status, ErrorResultStatus.NO_ERROR);
        DetectorProfile expectedProfile = new DetectorProfile.Builder().state(expectedState).build();
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile(detector.getDetectorId(), ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            logger.error(exception);
            for (StackTraceElement ste : exception.getStackTrace()) {
                logger.info(ste);
            }
            assertTrue("Should not reach here ", false);
            inProgressLatch.countDown();
        }), stateOnly);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testResultNotExist() throws IOException, InterruptedException {
        testInitOrRunningStateTemplate(RCFPollingStatus.INIT_NOT_EXIT, DetectorState.INIT);
    }

    public void testRemoteResultNotExist() throws IOException, InterruptedException {
        testInitOrRunningStateTemplate(RCFPollingStatus.REMOTE_INIT_NOT_EXIT, DetectorState.INIT);
    }

    public void testCheckpointIndexNotExist() throws IOException, InterruptedException {
        testInitOrRunningStateTemplate(RCFPollingStatus.INDEX_NOT_FOUND, DetectorState.INIT);
    }

    public void testRemoteCheckpointIndexNotExist() throws IOException, InterruptedException {
        testInitOrRunningStateTemplate(RCFPollingStatus.REMOTE_INDEX_NOT_FOUND, DetectorState.INIT);
    }

    public void testResultEmpty() throws IOException, InterruptedException {
        testInitOrRunningStateTemplate(RCFPollingStatus.EMPTY, DetectorState.INIT);
    }

    public void testResultGreaterThanZero() throws IOException, InterruptedException {
        testInitOrRunningStateTemplate(RCFPollingStatus.INIT_DONE, DetectorState.RUNNING);
    }

    public void testErrorStateTemplate(
        RCFPollingStatus initStatus,
        ErrorResultStatus status,
        DetectorState state,
        String error,
        JobStatus jobStatus,
        Set<DetectorProfileName> profilesToCollect
    ) throws IOException,
        InterruptedException {
        setUpClientExecuteRCFPollingAction(initStatus);
        setUpClientGet(DetectorStatus.EXIST, jobStatus, initStatus, status);
        DetectorProfile.Builder builder = new DetectorProfile.Builder();
        if (profilesToCollect.contains(DetectorProfileName.STATE)) {
            builder.state(state);
        }
        if (profilesToCollect.contains(DetectorProfileName.ERROR)) {
            builder.error(error);
        }
        DetectorProfile expectedProfile = builder.build();
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile(detector.getDetectorId(), ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            logger.info(exception);
            for (StackTraceElement ste : exception.getStackTrace()) {
                logger.info(ste);
            }
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }), profilesToCollect);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testErrorStateTemplate(
        RCFPollingStatus initStatus,
        ErrorResultStatus status,
        DetectorState state,
        String error,
        JobStatus jobStatus
    ) throws IOException,
        InterruptedException {
        testErrorStateTemplate(initStatus, status, state, error, jobStatus, stateNError);
    }

    public void testRunningNoError() throws IOException, InterruptedException {
        testErrorStateTemplate(RCFPollingStatus.INIT_DONE, ErrorResultStatus.NO_ERROR, DetectorState.RUNNING, null, JobStatus.ENABLED);
    }

    public void testRunningWithError() throws IOException, InterruptedException {
        testErrorStateTemplate(
            RCFPollingStatus.INIT_DONE,
            ErrorResultStatus.SHINGLE_ERROR,
            DetectorState.RUNNING,
            noFullShingleError,
            JobStatus.ENABLED
        );
    }

    public void testDisabledForStateError() throws IOException, InterruptedException {
        testErrorStateTemplate(
            RCFPollingStatus.INITTING,
            ErrorResultStatus.STOPPED_ERROR,
            DetectorState.DISABLED,
            stoppedError,
            JobStatus.DISABLED
        );
    }

    public void testDisabledForStateInit() throws IOException, InterruptedException {
        testErrorStateTemplate(
            RCFPollingStatus.INITTING,
            ErrorResultStatus.STOPPED_ERROR,
            DetectorState.DISABLED,
            stoppedError,
            JobStatus.DISABLED,
            stateInitProgress
        );
    }

    public void testInitWithError() throws IOException, InterruptedException {
        testErrorStateTemplate(
            RCFPollingStatus.EMPTY,
            ErrorResultStatus.SHINGLE_ERROR,
            DetectorState.INIT,
            noFullShingleError,
            JobStatus.ENABLED
        );
    }

    @SuppressWarnings("unchecked")
    private void setUpClientExecuteProfileAction() {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<ProfileResponse> listener = (ActionListener<ProfileResponse>) args[2];

            node1 = "node1";
            nodeName1 = "nodename1";
            discoveryNode1 = new DiscoveryNode(
                nodeName1,
                node1,
                new TransportAddress(TransportAddress.META_ADDRESS, 9300),
                emptyMap(),
                emptySet(),
                Version.CURRENT
            );

            node2 = "node2";
            nodeName2 = "nodename2";
            discoveryNode2 = new DiscoveryNode(
                nodeName2,
                node2,
                new TransportAddress(TransportAddress.META_ADDRESS, 9301),
                emptyMap(),
                emptySet(),
                Version.CURRENT
            );

            modelSize = 4456448L;
            model1Id = "Pl536HEBnXkDrah03glg_model_rcf_1";
            model0Id = "Pl536HEBnXkDrah03glg_model_rcf_0";

            shingleSize = 6;

            String clusterName = "test-cluster-name";

            Map<String, Long> modelSizeMap1 = new HashMap<String, Long>() {
                {
                    put(model1Id, modelSize);
                }
            };

            Map<String, Long> modelSizeMap2 = new HashMap<String, Long>() {
                {
                    put(model0Id, modelSize);
                }
            };

            ProfileNodeResponse profileNodeResponse1 = new ProfileNodeResponse(discoveryNode1, modelSizeMap1, shingleSize, 0L, 0L);
            ProfileNodeResponse profileNodeResponse2 = new ProfileNodeResponse(discoveryNode2, modelSizeMap2, -1, 0L, 0L);
            List<ProfileNodeResponse> profileNodeResponses = Arrays.asList(profileNodeResponse1, profileNodeResponse2);
            List<FailedNodeException> failures = Collections.emptyList();
            ProfileResponse profileResponse = new ProfileResponse(new ClusterName(clusterName), profileNodeResponses, failures);

            listener.onResponse(profileResponse);

            return null;
        }).when(client).execute(any(ProfileAction.class), any(), any());

    }

    @SuppressWarnings("unchecked")
    private void setUpClientExecuteRCFPollingAction(RCFPollingStatus inittedEverResultStatus) {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<RCFPollingResponse> listener = (ActionListener<RCFPollingResponse>) args[2];

            Exception cause = null;
            String detectorId = "123";
            if (inittedEverResultStatus == RCFPollingStatus.INIT_NOT_EXIT
                || inittedEverResultStatus == RCFPollingStatus.REMOTE_INIT_NOT_EXIT
                || inittedEverResultStatus == RCFPollingStatus.INDEX_NOT_FOUND
                || inittedEverResultStatus == RCFPollingStatus.REMOTE_INDEX_NOT_FOUND) {
                switch (inittedEverResultStatus) {
                    case INIT_NOT_EXIT:
                    case REMOTE_INIT_NOT_EXIT:
                        cause = new ResourceNotFoundException(detectorId, messaingExceptionError);
                        break;
                    case INDEX_NOT_FOUND:
                    case REMOTE_INDEX_NOT_FOUND:
                        cause = new IndexNotFoundException(detectorId, CommonName.CHECKPOINT_INDEX_NAME);
                        break;
                    default:
                        assertTrue("should not reach here", false);
                        break;
                }
                cause = new AnomalyDetectionException(detectorId, cause);
                if (inittedEverResultStatus == RCFPollingStatus.REMOTE_INIT_NOT_EXIT
                    || inittedEverResultStatus == RCFPollingStatus.REMOTE_INDEX_NOT_FOUND) {
                    cause = new RemoteTransportException(RCFPollingAction.NAME, new NotSerializableExceptionWrapper(cause));
                }
                listener.onFailure(cause);
            } else {
                RCFPollingResponse result = null;
                switch (inittedEverResultStatus) {
                    case INIT_DONE:
                        result = new RCFPollingResponse(requiredSamples + 1);
                        break;
                    case INITTING:
                        result = new RCFPollingResponse(requiredSamples - neededSamples);
                        break;
                    case EMPTY:
                        result = new RCFPollingResponse(0);
                        break;
                    case EXCEPTION:
                        listener.onFailure(new RuntimeException());
                        break;
                    default:
                        assertTrue("should not reach here", false);
                        break;
                }

                listener.onResponse(result);
            }
            return null;
        }).when(client).execute(any(RCFPollingAction.class), any(), any());

    }

    public void testProfileModels() throws InterruptedException, IOException {
        setUpClientGet(DetectorStatus.EXIST, JobStatus.ENABLED, RCFPollingStatus.EMPTY, ErrorResultStatus.NO_ERROR);
        setUpClientExecuteProfileAction();

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile(detector.getDetectorId(), ActionListener.wrap(profileResponse -> {
            assertEquals(node1, profileResponse.getCoordinatingNode());
            assertEquals(shingleSize, profileResponse.getShingleSize());
            assertEquals(modelSize * 2, profileResponse.getTotalSizeInBytes());
            assertEquals(2, profileResponse.getModelProfile().length);
            for (ModelProfile profile : profileResponse.getModelProfile()) {
                assertTrue(node1.equals(profile.getNodeId()) || node2.equals(profile.getNodeId()));
                assertEquals(modelSize, profile.getModelSize());
                if (node1.equals(profile.getNodeId())) {
                    assertEquals(model1Id, profile.getModelId());
                }
                if (node2.equals(profile.getNodeId())) {
                    assertEquals(model0Id, profile.getModelId());
                }
            }
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Should not reach here ", false);
            inProgressLatch.countDown();
        }), modelProfile);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testInitProgress() throws IOException, InterruptedException {
        setUpClientGet(DetectorStatus.EXIST, JobStatus.ENABLED, RCFPollingStatus.INITTING, ErrorResultStatus.NO_ERROR);
        DetectorProfile expectedProfile = new DetectorProfile.Builder().state(DetectorState.INIT).build();

        // 123 / 128 rounded to 96%
        InitProgressProfile profile = new InitProgressProfile("96%", neededSamples * detectorIntervalMin, neededSamples);
        expectedProfile.setInitProgress(profile);
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile(detector.getDetectorId(), ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Should not reach here ", false);
            inProgressLatch.countDown();
        }), stateInitProgress);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testInitProgressFailImmediately() throws IOException, InterruptedException {
        setUpClientGet(DetectorStatus.NO_DOC, JobStatus.ENABLED, RCFPollingStatus.INITTING, ErrorResultStatus.NO_ERROR);
        DetectorProfile expectedProfile = new DetectorProfile.Builder().state(DetectorState.INIT).build();

        // 123 / 128 rounded to 96%
        InitProgressProfile profile = new InitProgressProfile("96%", neededSamples * detectorIntervalMin, neededSamples);
        expectedProfile.setInitProgress(profile);
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile(detector.getDetectorId(), ActionListener.wrap(response -> {
            assertTrue("Should not reach here ", false);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue(exception.getMessage().contains(CommonErrorMessages.FAIL_TO_FIND_DETECTOR_MSG));
            inProgressLatch.countDown();
        }), stateInitProgress);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testInitNoUpdateNoIndex() throws IOException, InterruptedException {
        setUpClientGet(DetectorStatus.EXIST, JobStatus.ENABLED, RCFPollingStatus.EMPTY, ErrorResultStatus.NO_ERROR);
        DetectorProfile expectedProfile = new DetectorProfile.Builder()
            .state(DetectorState.INIT)
            .initProgress(new InitProgressProfile("0%", detectorIntervalMin * requiredSamples, requiredSamples))
            .build();
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile(detector.getDetectorId(), ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            LOG.error(exception);
            for (StackTraceElement ste : exception.getStackTrace()) {
                LOG.info(ste);
            }
            assertTrue("Should not reach here ", false);
            inProgressLatch.countDown();
        }), stateInitProgress);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testInitNoIndex() throws IOException, InterruptedException {
        setUpClientGet(DetectorStatus.EXIST, JobStatus.ENABLED, RCFPollingStatus.INDEX_NOT_FOUND, ErrorResultStatus.NO_ERROR);
        DetectorProfile expectedProfile = new DetectorProfile.Builder()
            .state(DetectorState.INIT)
            .initProgress(new InitProgressProfile("0%", 0, requiredSamples))
            .build();
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile(detector.getDetectorId(), ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            LOG.error(exception);
            for (StackTraceElement ste : exception.getStackTrace()) {
                LOG.info(ste);
            }
            assertTrue("Should not reach here ", false);
            inProgressLatch.countDown();
        }), stateInitProgress);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testInvalidRequiredSamples() {
        expectThrows(IllegalArgumentException.class, () -> new AnomalyDetectorProfileRunner(client, xContentRegistry(), nodeFilter, 0));
    }

    public void testFailRCFPolling() throws IOException, InterruptedException {
        setUpClientGet(DetectorStatus.EXIST, JobStatus.ENABLED, RCFPollingStatus.EXCEPTION, ErrorResultStatus.NO_ERROR);
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile(detector.getDetectorId(), ActionListener.wrap(response -> {
            assertTrue("Should not reach here ", false);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue(exception instanceof RuntimeException);
            // this means we don't exit with failImmediately. failImmediately can make we return early when there are other concurrent
            // requests
            assertTrue(exception.getMessage(), exception.getMessage().contains("Exceptions:"));
            inProgressLatch.countDown();
        }), stateNError);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }
}
