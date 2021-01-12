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

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.transport.TransportAddress;
import org.junit.Before;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorInternalState;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfile;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfileName;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorState;
import com.amazon.opendistroforelasticsearch.ad.task.ADTaskManager;
import com.amazon.opendistroforelasticsearch.ad.transport.ProfileAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ProfileNodeResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.ProfileResponse;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;

public class MultiEntityProfileRunnerTests extends AbstractADTest {
    private AnomalyDetectorProfileRunner runner;
    private Client client;
    private DiscoveryNodeFilterer nodeFilter;
    private int requiredSamples;
    private AnomalyDetector detector;
    private String detectorId;
    private Set<DetectorProfileName> stateNError;
    private DetectorInternalState.Builder result;
    private String node1;
    private String nodeName1;
    private DiscoveryNode discoveryNode1;

    private String node2;
    private String nodeName2;
    private DiscoveryNode discoveryNode2;

    private long modelSize;
    private String model1Id;
    private String model0Id;

    private int shingleSize;
    private AnomalyDetectorJob job;
    private ADTaskManager adTaskManager;

    enum InittedEverResultStatus {
        INITTED,
        NOT_INITTED,
    }

    @SuppressWarnings("unchecked")
    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        nodeFilter = mock(DiscoveryNodeFilterer.class);
        requiredSamples = 128;

        detectorId = "A69pa3UBHuCbh-emo9oR";
        detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList("a"));
        result = new DetectorInternalState.Builder().lastUpdateTime(Instant.now());
        job = TestHelpers.randomAnomalyDetectorJob(true);
        adTaskManager = mock(ADTaskManager.class);

        runner = new AnomalyDetectorProfileRunner(client, xContentRegistry(), nodeFilter, requiredSamples, adTaskManager);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            GetRequest request = (GetRequest) args[0];
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            String indexName = request.index();
            if (indexName.equals(ANOMALY_DETECTORS_INDEX)) {
                listener
                    .onResponse(TestHelpers.createGetResponse(detector, detector.getDetectorId(), AnomalyDetector.ANOMALY_DETECTORS_INDEX));
            } else if (indexName.equals(CommonName.DETECTION_STATE_INDEX)) {
                listener
                    .onResponse(TestHelpers.createGetResponse(result.build(), detector.getDetectorId(), CommonName.DETECTION_STATE_INDEX));
            } else if (indexName.equals(ANOMALY_DETECTOR_JOB_INDEX)) {
                listener
                    .onResponse(
                        TestHelpers.createGetResponse(job, detector.getDetectorId(), AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX)
                    );
            }

            return null;
        }).when(client).get(any(), any());

        stateNError = new HashSet<DetectorProfileName>();
        stateNError.add(DetectorProfileName.ERROR);
        stateNError.add(DetectorProfileName.STATE);
    }

    @SuppressWarnings("unchecked")
    private void setUpClientExecuteProfileAction(InittedEverResultStatus initted) {
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

            modelSize = 712480L;
            model1Id = "A69pa3UBHuCbh-emo9oR_entity_host1";
            model0Id = "A69pa3UBHuCbh-emo9oR_entity_host0";

            shingleSize = -1;

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

            // one model in each node; all fully initialized
            long updates = requiredSamples - 1;
            if (InittedEverResultStatus.INITTED == initted) {
                updates = requiredSamples + 1;
            }
            ProfileNodeResponse profileNodeResponse1 = new ProfileNodeResponse(discoveryNode1, modelSizeMap1, shingleSize, 1L, updates);
            ProfileNodeResponse profileNodeResponse2 = new ProfileNodeResponse(discoveryNode2, modelSizeMap2, shingleSize, 1L, updates);
            List<ProfileNodeResponse> profileNodeResponses = Arrays.asList(profileNodeResponse1, profileNodeResponse2);
            List<FailedNodeException> failures = Collections.emptyList();
            ProfileResponse profileResponse = new ProfileResponse(new ClusterName(clusterName), profileNodeResponses, failures);

            listener.onResponse(profileResponse);

            return null;
        }).when(client).execute(any(ProfileAction.class), any(), any());

    }

    @SuppressWarnings("unchecked")
    private void setUpClientSearch(InittedEverResultStatus inittedEverResultStatus) {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            SearchRequest request = (SearchRequest) args[0];
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];

            AnomalyResult result = null;
            if (request.source().query().toString().contains(AnomalyResult.ANOMALY_SCORE_FIELD)) {
                switch (inittedEverResultStatus) {
                    case INITTED:
                        result = TestHelpers.randomAnomalyDetectResult(0.87);
                        listener.onResponse(TestHelpers.createSearchResponse(result));
                        break;
                    case NOT_INITTED:
                        listener.onResponse(TestHelpers.createEmptySearchResponse());
                        break;
                    default:
                        assertTrue("should not reach here", false);
                        break;
                }
            }

            return null;
        }).when(client).search(any(), any());
    }

    public void testInit() throws InterruptedException {
        setUpClientExecuteProfileAction(InittedEverResultStatus.NOT_INITTED);
        setUpClientSearch(InittedEverResultStatus.NOT_INITTED);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        DetectorProfile expectedProfile = new DetectorProfile.Builder().state(DetectorState.INIT).build();
        runner.profile(detectorId, ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }), stateNError);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testRunning() throws InterruptedException {
        setUpClientExecuteProfileAction(InittedEverResultStatus.INITTED);
        setUpClientSearch(InittedEverResultStatus.INITTED);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        DetectorProfile expectedProfile = new DetectorProfile.Builder().state(DetectorState.RUNNING).build();
        runner.profile(detectorId, ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }), stateNError);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    /**
     * Although profile action results indicate initted, we trust what result index tells us
     * @throws InterruptedException if CountDownLatch is interrupted while waiting
     */
    public void testResultIndexFinalTruth() throws InterruptedException {
        setUpClientExecuteProfileAction(InittedEverResultStatus.NOT_INITTED);
        setUpClientSearch(InittedEverResultStatus.INITTED);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        DetectorProfile expectedProfile = new DetectorProfile.Builder().state(DetectorState.RUNNING).build();
        runner.profile(detectorId, ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }), stateNError);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }
}
