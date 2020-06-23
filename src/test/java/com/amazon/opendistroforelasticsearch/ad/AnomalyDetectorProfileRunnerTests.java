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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.BeforeClass;

import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfile;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorState;
import com.amazon.opendistroforelasticsearch.ad.model.ModelProfile;
import com.amazon.opendistroforelasticsearch.ad.model.ProfileName;
import com.amazon.opendistroforelasticsearch.ad.transport.ProfileNodeResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.ProfileResponse;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;

public class AnomalyDetectorProfileRunnerTests extends ESTestCase {
    private static final Logger LOG = LogManager.getLogger(AnomalyDetectorProfileRunnerTests.class);
    private AnomalyDetectorProfileRunner runner;
    private Client client;
    private DiscoveryNodeFilterer nodeFilter;
    private AnomalyDetector detector;
    private IndexNameExpressionResolver resolver;
    private ClusterService clusterService;

    private static Set<ProfileName> stateOnly;
    private static Set<ProfileName> stateNError;
    private static Set<ProfileName> modelProfile;
    private static String noFullShingleError = "No full shingle in current detection window";
    private static String stoppedError = "Stopped detector as job failed consecutively for more than 3 times: Having trouble querying data."
        + " Maybe all of your features have been disabled.";
    private Calendar calendar;
    private String indexWithRequiredError1 = ".opendistro-anomaly-results-history-2020.04.06-1";
    private String indexWithRequiredError2 = ".opendistro-anomaly-results-history-2020.04.07-000002";

    // profile model related
    String node1;
    String nodeName1;
    DiscoveryNode discoveryNode1;

    String node2;
    String nodeName2;
    DiscoveryNode discoveryNode2;

    long modelSize;
    String model1Id;
    String model0Id;

    int shingleSize;

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        List<NamedXContentRegistry.Entry> entries = searchModule.getNamedXContents();
        entries.addAll(Arrays.asList(AnomalyDetector.XCONTENT_REGISTRY, AnomalyResult.XCONTENT_REGISTRY));
        return new NamedXContentRegistry(entries);
    }

    @BeforeClass
    public static void setUpOnce() {
        stateOnly = new HashSet<ProfileName>();
        stateOnly.add(ProfileName.STATE);
        stateNError = new HashSet<ProfileName>();
        stateNError.add(ProfileName.ERROR);
        stateNError.add(ProfileName.STATE);
        modelProfile = new HashSet<ProfileName>(
            Arrays.asList(ProfileName.SHINGLE_SIZE, ProfileName.MODELS, ProfileName.COORDINATING_NODE, ProfileName.TOTAL_SIZE_IN_BYTES)
        );
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        nodeFilter = mock(DiscoveryNodeFilterer.class);
        calendar = mock(Calendar.class);
        resolver = mock(IndexNameExpressionResolver.class);
        clusterService = mock(ClusterService.class);
        when(resolver.concreteIndexNames(any(), any(), any()))
            .thenReturn(
                new String[] { indexWithRequiredError1, indexWithRequiredError2, ".opendistro-anomaly-results-history-2020.04.08-000003" }
            );
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("test cluster")).build());

        runner = new AnomalyDetectorProfileRunner(client, xContentRegistry(), nodeFilter, resolver, clusterService, calendar);
    }

    enum JobStatus {
        INDEX_NOT_EXIT,
        DISABLED,
        ENABLED,
        DISABLED_ROTATED_1,
        DISABLED_ROTATED_2,
        DISABLED_ROTATED_3
    }

    enum InittedEverResultStatus {
        INDEX_NOT_EXIT,
        GREATER_THAN_ZERO,
        EMPTY,
        EXCEPTION
    }

    enum ErrorResultStatus {
        INDEX_NOT_EXIT,
        NO_ERROR,
        SHINGLE_ERROR,
        STOPPED_ERROR_1,
        STOPPED_ERROR_2
    }

    @SuppressWarnings("unchecked")
    private void setUpClientGet(boolean detectorExists, JobStatus jobStatus) throws IOException {
        detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), Instant.now());
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            GetRequest request = (GetRequest) args[0];
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            if (request.index().equals(ANOMALY_DETECTORS_INDEX)) {
                if (detectorExists) {
                    listener.onResponse(TestHelpers.createGetResponse(detector, detector.getDetectorId()));
                } else {
                    listener.onFailure(new IndexNotFoundException(ANOMALY_DETECTORS_INDEX));
                }
            } else {
                AnomalyDetectorJob job = null;
                switch (jobStatus) {
                    case INDEX_NOT_EXIT:
                        listener.onFailure(new IndexNotFoundException(ANOMALY_DETECTOR_JOB_INDEX));
                        break;
                    case DISABLED:
                        job = TestHelpers.randomAnomalyDetectorJob(false);
                        listener.onResponse(TestHelpers.createGetResponse(job, detector.getDetectorId()));
                        break;
                    case ENABLED:
                        job = TestHelpers.randomAnomalyDetectorJob(true);
                        listener.onResponse(TestHelpers.createGetResponse(job, detector.getDetectorId()));
                        break;
                    case DISABLED_ROTATED_1:
                        // enabled time is smaller than 1586217600000, while disabled time is larger than 1586217600000
                        // which is April 7, 2020 12:00:00 AM.
                        job = TestHelpers
                            .randomAnomalyDetectorJob(false, Instant.ofEpochMilli(1586217500000L), Instant.ofEpochMilli(1586227600000L));
                        listener.onResponse(TestHelpers.createGetResponse(job, detector.getDetectorId()));
                        break;
                    case DISABLED_ROTATED_2:
                        // both enabled and disabled time are larger than 1586217600000,
                        // which is April 7, 2020 12:00:00 AM.
                        job = TestHelpers
                            .randomAnomalyDetectorJob(false, Instant.ofEpochMilli(1586217500000L), Instant.ofEpochMilli(1586227600000L));
                        listener.onResponse(TestHelpers.createGetResponse(job, detector.getDetectorId()));
                        break;
                    case DISABLED_ROTATED_3:
                        // both enabled and disabled time are larger than 1586131200000,
                        // which is April 6, 2020 12:00:00 AM.
                        job = TestHelpers
                            .randomAnomalyDetectorJob(false, Instant.ofEpochMilli(1586131300000L), Instant.ofEpochMilli(1586131400000L));
                        listener.onResponse(TestHelpers.createGetResponse(job, detector.getDetectorId()));
                        break;
                    default:
                        assertTrue("should not reach here", false);
                        break;
                }
            }

            return null;
        }).when(client).get(any(), any());
    }

    @SuppressWarnings("unchecked")
    private void setUpClientSearch(InittedEverResultStatus inittedEverResultStatus, ErrorResultStatus errorResultStatus) {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            SearchRequest request = (SearchRequest) args[0];
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];
            if (errorResultStatus == ErrorResultStatus.INDEX_NOT_EXIT
                || inittedEverResultStatus == InittedEverResultStatus.INDEX_NOT_EXIT) {
                listener.onFailure(new IndexNotFoundException(AnomalyResult.ANOMALY_RESULT_INDEX));
                return null;
            }
            AnomalyResult result = null;
            if (request.source().query().toString().contains(AnomalyResult.ANOMALY_SCORE_FIELD)) {
                switch (inittedEverResultStatus) {
                    case GREATER_THAN_ZERO:
                        result = TestHelpers.randomAnomalyDetectResult(0.87);
                        listener.onResponse(TestHelpers.createSearchResponse(result));
                        break;
                    case EMPTY:
                        listener.onResponse(TestHelpers.createEmptySearchResponse());
                        break;
                    case EXCEPTION:
                        listener.onFailure(new RuntimeException());
                        break;
                    default:
                        assertTrue("should not reach here", false);
                        break;
                }
            } else {
                switch (errorResultStatus) {
                    case NO_ERROR:
                        result = TestHelpers.randomAnomalyDetectResult(null);
                        listener.onResponse(TestHelpers.createSearchResponse(result));
                        break;
                    case SHINGLE_ERROR:
                        result = TestHelpers.randomAnomalyDetectResult(noFullShingleError);
                        listener.onResponse(TestHelpers.createSearchResponse(result));
                        break;
                    case STOPPED_ERROR_2:
                        if (request.indices().length == 2) {
                            for (int i = 0; i < 2; i++) {
                                assertTrue(
                                    request.indices()[i].equals(indexWithRequiredError1)
                                        || request.indices()[i].equals(indexWithRequiredError2)
                                );
                            }
                            result = TestHelpers.randomAnomalyDetectResult(stoppedError);
                            listener.onResponse(TestHelpers.createSearchResponse(result));
                        } else {
                            assertTrue("should not reach here", false);
                        }
                        break;
                    case STOPPED_ERROR_1:
                        if (request.indices().length == 1 && request.indices()[0].equals(indexWithRequiredError1)) {
                            result = TestHelpers.randomAnomalyDetectResult(stoppedError);
                            listener.onResponse(TestHelpers.createSearchResponse(result));
                        } else {
                            assertTrue("should not reach here", false);
                        }
                        break;
                    default:
                        assertTrue("should not reach here", false);
                        break;
                }
            }

            return null;
        }).when(client).search(any(), any());

    }

    public void testDetectorNotExist() throws IOException, InterruptedException {
        setUpClientGet(false, JobStatus.INDEX_NOT_EXIT);
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile("x123", ActionListener.wrap(response -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue(exception.getMessage().contains(AnomalyDetectorProfileRunner.FAIL_TO_FIND_DETECTOR_MSG));
            inProgressLatch.countDown();
        }), stateNError);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testDisabledJobIndexTemplate(JobStatus status) throws IOException, InterruptedException {
        setUpClientGet(true, status);
        DetectorProfile expectedProfile = new DetectorProfile();
        expectedProfile.setState(DetectorState.DISABLED);
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

    public void testInitOrRunningStateTemplate(InittedEverResultStatus status, DetectorState expectedState) throws IOException,
        InterruptedException {
        setUpClientGet(true, JobStatus.ENABLED);
        setUpClientSearch(status, ErrorResultStatus.NO_ERROR);
        DetectorProfile expectedProfile = new DetectorProfile();
        expectedProfile.setState(expectedState);
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

    public void testResultNotExist() throws IOException, InterruptedException {
        testInitOrRunningStateTemplate(InittedEverResultStatus.INDEX_NOT_EXIT, DetectorState.INIT);
    }

    public void testResultEmpty() throws IOException, InterruptedException {
        testInitOrRunningStateTemplate(InittedEverResultStatus.EMPTY, DetectorState.INIT);
    }

    public void testResultGreaterThanZero() throws IOException, InterruptedException {
        testInitOrRunningStateTemplate(InittedEverResultStatus.GREATER_THAN_ZERO, DetectorState.RUNNING);
    }

    public void testErrorStateTemplate(InittedEverResultStatus initStatus, ErrorResultStatus status, DetectorState state, String error)
        throws IOException,
        InterruptedException {
        setUpClientGet(true, JobStatus.ENABLED);
        setUpClientSearch(initStatus, status);
        DetectorProfile expectedProfile = new DetectorProfile();
        expectedProfile.setState(state);
        expectedProfile.setError(error);
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile(detector.getDetectorId(), ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Should not reach here ", false);
            inProgressLatch.countDown();
        }), stateNError);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testInitNoError() throws IOException, InterruptedException {
        testErrorStateTemplate(InittedEverResultStatus.INDEX_NOT_EXIT, ErrorResultStatus.INDEX_NOT_EXIT, DetectorState.INIT, null);
    }

    public void testRunningNoError() throws IOException, InterruptedException {
        testErrorStateTemplate(InittedEverResultStatus.GREATER_THAN_ZERO, ErrorResultStatus.NO_ERROR, DetectorState.RUNNING, null);
    }

    public void testRunningWithError() throws IOException, InterruptedException {
        testErrorStateTemplate(
            InittedEverResultStatus.GREATER_THAN_ZERO,
            ErrorResultStatus.SHINGLE_ERROR,
            DetectorState.RUNNING,
            noFullShingleError
        );
    }

    public void testInitWithError() throws IOException, InterruptedException {
        testErrorStateTemplate(InittedEverResultStatus.EMPTY, ErrorResultStatus.SHINGLE_ERROR, DetectorState.INIT, noFullShingleError);
    }

    public void testExceptionOnStateFetching() throws IOException, InterruptedException {
        setUpClientGet(true, JobStatus.ENABLED);
        setUpClientSearch(InittedEverResultStatus.EXCEPTION, ErrorResultStatus.NO_ERROR);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile(detector.getDetectorId(), ActionListener.wrap(response -> {
            assertTrue("Should not reach here ", false);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Unexcpeted exception " + exception.getMessage(), exception instanceof RuntimeException);
            inProgressLatch.countDown();
        }), stateOnly);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    @SuppressWarnings("unchecked")
    private void setUpClientExecute() {
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

            LOG.info("hello");
            ProfileNodeResponse profileNodeResponse1 = new ProfileNodeResponse(discoveryNode1, modelSizeMap1, shingleSize);
            ProfileNodeResponse profileNodeResponse2 = new ProfileNodeResponse(discoveryNode2, modelSizeMap2, -1);
            List<ProfileNodeResponse> profileNodeResponses = Arrays.asList(profileNodeResponse1, profileNodeResponse2);
            List<FailedNodeException> failures = Collections.emptyList();
            ProfileResponse profileResponse = new ProfileResponse(new ClusterName(clusterName), profileNodeResponses, failures);

            listener.onResponse(profileResponse);

            return null;
        }).when(client).execute(any(), any(), any());

    }

    public void testProfileModels() throws InterruptedException, IOException {
        setUpClientGet(true, JobStatus.ENABLED);
        setUpClientExecute();

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

    /**
     * A detector's error message can be on a rotated index. This test makes sure we get error info
     *  from .opendistro-anomaly-results index that has been rolled over.
     * @param state expected detector state
     * @param jobStatus job status to config in the test case
     * @throws IOException when profile API throws it
     * @throws InterruptedException when our CountDownLatch has been interruptted
     */
    private void stoppedDetectorErrorTemplate(DetectorState state, JobStatus jobStatus, ErrorResultStatus errorStatus) throws IOException,
        InterruptedException {
        setUpClientGet(true, jobStatus);
        setUpClientSearch(InittedEverResultStatus.GREATER_THAN_ZERO, errorStatus);
        DetectorProfile expectedProfile = new DetectorProfile();
        expectedProfile.setState(state);
        expectedProfile.setError(stoppedError);
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile(detector.getDetectorId(), ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Should not reach here ", false);
            inProgressLatch.countDown();
        }), stateNError);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    /**
     * Job enabled time is earlier than and disabled time is later than index 2 creation date, we expect to search 2 indices
     */
    public void testDetectorStoppedEnabledTimeLtIndex2Date() throws IOException, InterruptedException {
        stoppedDetectorErrorTemplate(DetectorState.DISABLED, JobStatus.DISABLED_ROTATED_1, ErrorResultStatus.STOPPED_ERROR_2);
    }

    /**
     * Both job enabled and disabled time are later than index 2 creation date, we expect to search 2 indices
     */
    public void testDetectorStoppedEnabledTimeGtIndex2Date() throws IOException, InterruptedException {
        stoppedDetectorErrorTemplate(DetectorState.DISABLED, JobStatus.DISABLED_ROTATED_2, ErrorResultStatus.STOPPED_ERROR_2);
    }

    /**
     * Both job enabled and disabled time are earlier than index 2 creation date, we expect to search 1 indices
     */
    public void testDetectorStoppedEnabledTimeGtIndex1Date() throws IOException, InterruptedException {
        stoppedDetectorErrorTemplate(DetectorState.DISABLED, JobStatus.DISABLED_ROTATED_3, ErrorResultStatus.STOPPED_ERROR_1);
    }

    public void testAssumption() {
        assertEquals(
            "profileError depends on this assumption.",
            ".opendistro-anomaly-results*",
            AnomalyDetectionIndices.ALL_AD_RESULTS_INDEX_PATTERN
        );
    }
}
