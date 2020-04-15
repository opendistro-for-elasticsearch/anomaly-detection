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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.BeforeClass;

import com.amazon.opendistroforelasticsearch.ad.cluster.ADMetaData;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfile;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorState;
import com.amazon.opendistroforelasticsearch.ad.model.ProfileName;

public class AnomalyDetectorProfileRunnerTests extends ESTestCase {
    private static final Logger LOG = LogManager.getLogger(AnomalyDetectorProfileRunnerTests.class);
    private AnomalyDetectorProfileRunner runner;
    private Client client;
    private AnomalyDetector detector;
    private static Set<ProfileName> stateOnly;
    private static Set<ProfileName> stateNError;
    private static String error = "No full shingle in current detection window";

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        List<NamedXContentRegistry.Entry> entries = searchModule.getNamedXContents();
        entries.addAll(Arrays.asList(AnomalyDetector.XCONTENT_REGISTRY, ADMetaData.XCONTENT_REGISTRY, AnomalyResult.XCONTENT_REGISTRY));
        return new NamedXContentRegistry(entries);
    }

    @BeforeClass
    public static void setUpOnce() {
        stateOnly = new HashSet<ProfileName>();
        stateOnly.add(ProfileName.STATE);
        stateNError = new HashSet<ProfileName>();
        stateNError.add(ProfileName.ERROR);
        stateNError.add(ProfileName.STATE);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        runner = new AnomalyDetectorProfileRunner(client, xContentRegistry());
    }

    enum JobStatus {
        INDEX_NOT_EXIT,
        DISABLED,
        ENABLED
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
        ERROR
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
                    case ERROR:
                        result = TestHelpers.randomAnomalyDetectResult(error);
                        listener.onResponse(TestHelpers.createSearchResponse(result));
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
        testErrorStateTemplate(InittedEverResultStatus.GREATER_THAN_ZERO, ErrorResultStatus.ERROR, DetectorState.RUNNING, error);
    }

    public void testInitWithError() throws IOException, InterruptedException {
        testErrorStateTemplate(InittedEverResultStatus.EMPTY, ErrorResultStatus.ERROR, DetectorState.INIT, error);
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
}
