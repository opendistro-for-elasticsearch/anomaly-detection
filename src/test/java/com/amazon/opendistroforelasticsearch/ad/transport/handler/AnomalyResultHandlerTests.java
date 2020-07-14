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

package com.amazon.opendistroforelasticsearch.ad.transport.handler;

import static com.amazon.opendistroforelasticsearch.ad.TestHelpers.createIndexBlockedState;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyResultTests;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.IndexUtils;
import com.amazon.opendistroforelasticsearch.ad.util.Throttler;
import com.amazon.opendistroforelasticsearch.ad.util.ThrowingConsumerWrapper;

public class AnomalyResultHandlerTests extends AbstractADTest {
    private static Settings settings;
    @Mock
    private ClusterService clusterService;

    @Mock
    private Client client;

    private ClientUtil clientUtil;

    @Mock
    private IndexNameExpressionResolver indexNameResolver;

    @Mock
    private AnomalyDetectionIndices anomalyDetectionIndices;

    private String detectorId = "123";

    @Mock
    private Throttler throttler;

    private ThreadPool context;

    private IndexUtils indexUtil;

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(AnomalyResultTests.class.getSimpleName());
        settings = Settings.EMPTY;
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
        settings = null;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        super.setUpLog4jForJUnit(AnomalyIndexHandler.class);
        MockitoAnnotations.initMocks(this);
        setWriteBlockAdResultIndex(false);
        context = TestHelpers.createThreadPool();
        clientUtil = new ClientUtil(settings, client, throttler, context);
        indexUtil = new IndexUtils(client, clientUtil, clusterService, indexNameResolver);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        super.tearDownLog4jForJUnit();
    }

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testSavingAdResult() throws IOException {
        setUpSavingAnomalyResultIndex(false);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length >= 2);
            IndexRequest request = invocation.getArgument(0);
            ActionListener<IndexResponse> listener = invocation.getArgument(1);
            assertTrue(request != null && listener != null);
            listener.onResponse(mock(IndexResponse.class));
            return null;
        }).when(client).index(any(IndexRequest.class), ArgumentMatchers.<ActionListener<IndexResponse>>any());
        AnomalyIndexHandler<AnomalyResult> handler = new AnomalyIndexHandler<AnomalyResult>(
            client,
            settings,
            threadPool,
            AnomalyResult.ANOMALY_RESULT_INDEX,
            ThrowingConsumerWrapper.throwingConsumerWrapper(anomalyDetectionIndices::initAnomalyResultIndexDirectly),
            anomalyDetectionIndices::doesAnomalyResultIndexExist,
            false,
            clientUtil,
            indexUtil,
            clusterService
        );
        handler.index(TestHelpers.randomAnomalyDetectResult(), detectorId);
        assertEquals(1, testAppender.countMessage(AnomalyIndexHandler.SUCCESS_SAVING_MSG, true));
    }

    @Test
    public void testSavingFailureNotRetry() throws InterruptedException, IOException {
        savingFailureTemplate(false, 1, true);

        assertEquals(1, testAppender.countMessage(AnomalyIndexHandler.FAIL_TO_SAVE_ERR_MSG, true));
        assertTrue(!testAppender.containsMessage(AnomalyIndexHandler.SUCCESS_SAVING_MSG, true));
        assertTrue(!testAppender.containsMessage(AnomalyIndexHandler.RETRY_SAVING_ERR_MSG, true));
    }

    @Test
    public void testSavingFailureRetry() throws InterruptedException, IOException {
        setWriteBlockAdResultIndex(false);
        savingFailureTemplate(true, 3, true);

        assertEquals(2, testAppender.countMessage(AnomalyIndexHandler.RETRY_SAVING_ERR_MSG, true));
        assertEquals(1, testAppender.countMessage(AnomalyIndexHandler.FAIL_TO_SAVE_ERR_MSG, true));
        assertTrue(!testAppender.containsMessage(AnomalyIndexHandler.SUCCESS_SAVING_MSG, true));
    }

    @Test
    public void testIndexWriteBlock() {
        setWriteBlockAdResultIndex(true);
        AnomalyIndexHandler<AnomalyResult> handler = new AnomalyIndexHandler<AnomalyResult>(
            client,
            settings,
            threadPool,
            AnomalyResult.ANOMALY_RESULT_INDEX,
            ThrowingConsumerWrapper.throwingConsumerWrapper(anomalyDetectionIndices::initAnomalyResultIndexDirectly),
            anomalyDetectionIndices::doesAnomalyResultIndexExist,
            false,
            clientUtil,
            indexUtil,
            clusterService
        );
        handler.index(TestHelpers.randomAnomalyDetectResult(), detectorId);

        assertTrue(testAppender.containsMessage(AnomalyIndexHandler.CANNOT_SAVE_ERR_MSG, true));
    }

    @Test
    public void testAdResultIndexExist() throws IOException {
        setInitAnomalyResultIndexException(true);
        AnomalyIndexHandler<AnomalyResult> handler = new AnomalyIndexHandler<AnomalyResult>(
            client,
            settings,
            threadPool,
            AnomalyResult.ANOMALY_RESULT_INDEX,
            ThrowingConsumerWrapper.throwingConsumerWrapper(anomalyDetectionIndices::initAnomalyResultIndexDirectly),
            anomalyDetectionIndices::doesAnomalyResultIndexExist,
            false,
            clientUtil,
            indexUtil,
            clusterService
        );
        handler.index(TestHelpers.randomAnomalyDetectResult(), detectorId);
        verify(client, times(1)).index(any(), any());
    }

    @Test
    public void testAdResultIndexOtherException() throws IOException {
        expectedEx.expect(AnomalyDetectionException.class);
        expectedEx.expectMessage("Error in saving .opendistro-anomaly-results for detector " + detectorId);

        setInitAnomalyResultIndexException(false);
        AnomalyIndexHandler<AnomalyResult> handler = new AnomalyIndexHandler<AnomalyResult>(
            client,
            settings,
            threadPool,
            AnomalyResult.ANOMALY_RESULT_INDEX,
            ThrowingConsumerWrapper.throwingConsumerWrapper(anomalyDetectionIndices::initAnomalyResultIndexDirectly),
            anomalyDetectionIndices::doesAnomalyResultIndexExist,
            false,
            clientUtil,
            indexUtil,
            clusterService
        );
        handler.index(TestHelpers.randomAnomalyDetectResult(), detectorId);
        verify(client, never()).index(any(), any());
    }

    private void setInitAnomalyResultIndexException(boolean indexExistException) throws IOException {
        Exception e = indexExistException ? mock(ResourceAlreadyExistsException.class) : mock(RuntimeException.class);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length >= 1);
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            assertTrue(listener != null);
            listener.onFailure(e);
            return null;
        }).when(anomalyDetectionIndices).initAnomalyResultIndexDirectly(any());
    }

    private void setWriteBlockAdResultIndex(boolean blocked) {
        String indexName = randomAlphaOfLength(10);
        Settings settings = blocked
            ? Settings.builder().put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true).build()
            : Settings.EMPTY;
        ClusterState blockedClusterState = createIndexBlockedState(indexName, settings, AnomalyResult.ANOMALY_RESULT_INDEX);
        when(clusterService.state()).thenReturn(blockedClusterState);
        when(indexNameResolver.concreteIndexNames(any(), any(), any())).thenReturn(new String[] { indexName });
    }

    /**
     * Template to test exponential backoff retry during saving anomaly result.
     *
     * @param throwEsRejectedExecutionException whether to throw
     *                                          EsRejectedExecutionException in the
     *                                          client::index mock or not
     * @param latchCount                        used for coordinating. Equal to
     *                                          number of expected retries plus 1.
     * @throws InterruptedException if thread execution is interrupted
     * @throws IOException          if IO failures
     */
    @SuppressWarnings("unchecked")
    private void savingFailureTemplate(boolean throwEsRejectedExecutionException, int latchCount, boolean adResultIndexExists)
        throws InterruptedException,
        IOException {
        setUpSavingAnomalyResultIndex(adResultIndexExists);

        final CountDownLatch backoffLatch = new CountDownLatch(latchCount);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length >= 2);
            IndexRequest request = invocation.getArgument(0);
            ActionListener<IndexResponse> listener = invocation.getArgument(1);
            assertTrue(request != null && listener != null);
            if (throwEsRejectedExecutionException) {
                listener.onFailure(new EsRejectedExecutionException(""));
            } else {
                listener.onFailure(new IllegalArgumentException());
            }

            backoffLatch.countDown();
            return null;
        }).when(client).index(any(IndexRequest.class), ArgumentMatchers.<ActionListener<IndexResponse>>any());

        Settings backoffSettings = Settings
            .builder()
            .put("opendistro.anomaly_detection.max_retry_for_backoff", 2)
            .put("opendistro.anomaly_detection.backoff_initial_delay", TimeValue.timeValueMillis(1))
            .build();

        AnomalyIndexHandler<AnomalyResult> handler = new AnomalyIndexHandler<AnomalyResult>(
            client,
            backoffSettings,
            threadPool,
            AnomalyResult.ANOMALY_RESULT_INDEX,
            ThrowingConsumerWrapper.throwingConsumerWrapper(anomalyDetectionIndices::initAnomalyResultIndexDirectly),
            anomalyDetectionIndices::doesAnomalyResultIndexExist,
            false,
            clientUtil,
            indexUtil,
            clusterService
        );

        handler.index(TestHelpers.randomAnomalyDetectResult(), detectorId);

        backoffLatch.await();
    }

    @SuppressWarnings("unchecked")
    private void setUpSavingAnomalyResultIndex(boolean anomalyResultIndexExists) throws IOException {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length >= 1);
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            assertTrue(listener != null);
            listener.onResponse(new CreateIndexResponse(true, true, AnomalyResult.ANOMALY_RESULT_INDEX) {
            });
            return null;
        }).when(anomalyDetectionIndices).initAnomalyResultIndexDirectly(any());
        when(anomalyDetectionIndices.doesAnomalyResultIndexExist()).thenReturn(anomalyResultIndexExists);
    }

}
