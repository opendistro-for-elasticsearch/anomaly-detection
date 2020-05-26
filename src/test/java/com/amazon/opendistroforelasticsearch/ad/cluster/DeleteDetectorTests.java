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

package com.amazon.opendistroforelasticsearch.ad.cluster;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.junit.After;
import org.junit.Before;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;

public class DeleteDetectorTests extends AbstractADTest {
    private Client client;
    private ClusterService clusterService;
    private Clock clock;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        super.setUpLog4jForJUnit(DeleteDetector.class);
        client = mock(Client.class);
        clusterService = mock(ClusterService.class);
        clock = mock(Clock.class);
    }

    private enum DetectorExecutionMode {
        DELETE_RESULT_NORMAL,
        DELETE_RESULT_INDEX_NOT_FOUND,
        DELETE_RESULT_FAILURE,
        DELETE_PARTIAL_FAILURE,
        MARK_NORMAL,
        MARK_FAILURE
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        super.tearDownLog4jForJUnit();
    }

    @SuppressWarnings("unchecked")
    public void deleteDetectorResponseTemplate(DetectorExecutionMode mode) throws Exception {

        long deletedDocNum = 10L;
        BulkByScrollResponse deleteByQueryResponse = mock(BulkByScrollResponse.class);
        when(deleteByQueryResponse.getDeleted()).thenReturn(deletedDocNum);

        AnomalyDetectorGraveyard daedDetector = new AnomalyDetectorGraveyard("123", 1L);
        Set<AnomalyDetectorGraveyard> deadDetectors = new HashSet<>();
        deadDetectors.add(daedDetector);
        MetaData metaData = MetaData.builder().putCustom(ADMetaData.TYPE, new ADMetaData(deadDetectors)).build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test cluster")).metaData(metaData).build();

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length >= 3);
            assertTrue(args[2] instanceof ActionListener);

            ActionListener<BulkByScrollResponse> listener = (ActionListener<BulkByScrollResponse>) args[2];

            assertTrue(listener != null);
            if (mode == DetectorExecutionMode.DELETE_RESULT_INDEX_NOT_FOUND) {
                listener.onFailure(new IndexNotFoundException(".opendistro-anomaly-result"));
            } else if (mode == DetectorExecutionMode.DELETE_RESULT_FAILURE) {
                listener.onFailure(new ElasticsearchException(""));
            } else {
                if (mode == DetectorExecutionMode.DELETE_PARTIAL_FAILURE) {
                    when(deleteByQueryResponse.getSearchFailures())
                        .thenReturn(
                            Collections
                                .singletonList(new ScrollableHitSource.SearchFailure(new ElasticsearchException("foo"), "bar", 1, "blah"))
                        );
                }
                listener.onResponse(deleteByQueryResponse);
            }

            return null;
        }).when(client).execute(eq(DeleteByQueryAction.INSTANCE), any(), any());

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length == 2);
            assertTrue(args[1] instanceof ClusterStateUpdateTask);

            ClusterStateUpdateTask task = (ClusterStateUpdateTask) args[1];

            ClusterState newState = task.execute(clusterState);
            if (mode == DetectorExecutionMode.DELETE_RESULT_FAILURE || mode == DetectorExecutionMode.DELETE_PARTIAL_FAILURE) {
                assertTrue(ADMetaData.getADMetaData(newState).equals(new ADMetaData(deadDetectors)));
            } else {
                assertTrue(ADMetaData.getADMetaData(newState) == ADMetaData.EMPTY_METADATA);
            }

            return null;
        }).when(clusterService).submitStateUpdateTask(any(String.class), any());

        when(clusterService.state()).thenReturn(clusterState);

        DeleteDetector deleteDetector = new DeleteDetector(clusterService, clock);

        deleteDetector.deleteDetectorResult(client);
    }

    public void testDeleteSingleNormal() throws Exception {
        deleteDetectorResponseTemplate(DetectorExecutionMode.DELETE_RESULT_NORMAL);
        assertTrue(testAppender.containsMessage(DeleteDetector.DOC_GOT_DELETED_LOG_MSG));
    }

    public void testDeleteSingleIndexNotFound() throws Exception {
        deleteDetectorResponseTemplate(DetectorExecutionMode.DELETE_RESULT_INDEX_NOT_FOUND);
        assertTrue(testAppender.containsMessage(DeleteDetector.INDEX_DELETED_LOG_MSG));
    }

    public void testDeleteSingleResultFailure() throws Exception {
        deleteDetectorResponseTemplate(DetectorExecutionMode.DELETE_RESULT_FAILURE);
        assertTrue(testAppender.containsMessage(DeleteDetector.NOT_ABLE_TO_DELETE_LOG_MSG));
    }

    public void testDeleteSingleResultPartialFailure() throws Exception {
        deleteDetectorResponseTemplate(DetectorExecutionMode.DELETE_PARTIAL_FAILURE);
        assertTrue(testAppender.containsMessage(DeleteDetector.SEARCH_FAILURE_LOG_MSG));
        assertTrue(testAppender.containsMessage(DeleteDetector.DOC_GOT_DELETED_LOG_MSG));
    }

    public void markDeleteTemplate(DetectorExecutionMode mode) {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test cluster")).build();

        String detectorID = "123";
        long epoch = 1000L;
        Clock clock = mock(Clock.class);
        when(clock.millis()).thenReturn(epoch);
        DeleteDetector deleteDetector = new DeleteDetector(clusterService, clock);

        String errorMsg = "blah";

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length == 2);
            assertTrue(args[1] instanceof ClusterStateUpdateTask);

            ClusterStateUpdateTask task = (ClusterStateUpdateTask) args[1];

            if (mode == DetectorExecutionMode.MARK_NORMAL) {
                ClusterState newState = task.execute(clusterState);

                assertTrue(
                    ADMetaData
                        .getADMetaData(newState)
                        .equals(new ADMetaData(Collections.singleton(new AnomalyDetectorGraveyard(detectorID, epoch))))
                );

                return newState;
            } else {
                task.onFailure("blah", new ElasticsearchException(errorMsg));
                return null;
            }

        }).when(clusterService).submitStateUpdateTask(any(String.class), any());

        deleteDetector.markAnomalyResultDeleted(detectorID, ActionListener.wrap(response -> {}, e -> {
            if (mode == DetectorExecutionMode.MARK_NORMAL) {
                fail(e.getMessage());
            } else {
                assertThat(e instanceof ElasticsearchException, equalTo(true));
                assertThat(e.getMessage(), equalTo(errorMsg));
            }
        }));
    }

    public void testMarkDeleteNormal() {
        markDeleteTemplate(DetectorExecutionMode.MARK_NORMAL);
    }

    public void testMarkDeleteFailure() {
        markDeleteTemplate(DetectorExecutionMode.MARK_FAILURE);
    }
}
