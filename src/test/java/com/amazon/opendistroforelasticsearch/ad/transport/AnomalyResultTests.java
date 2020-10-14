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

import static com.amazon.opendistroforelasticsearch.ad.TestHelpers.createIndexBlockedState;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyDouble;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.ArgumentCaptor;

import test.com.amazon.opendistroforelasticsearch.ad.util.JsonDeserializer;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.cluster.HashRing;
import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.InternalFailure;
import com.amazon.opendistroforelasticsearch.ad.common.exception.JsonPathNotFoundException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ResourceNotFoundException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonMessageAttributes;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.feature.FeatureManager;
import com.amazon.opendistroforelasticsearch.ad.feature.SinglePointFeatures;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.RcfResult;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingResult;
import com.amazon.opendistroforelasticsearch.ad.ml.rcf.CombinedRcfResult;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorInternalState;
import com.amazon.opendistroforelasticsearch.ad.model.FeatureData;
import com.amazon.opendistroforelasticsearch.ad.stats.ADStat;
import com.amazon.opendistroforelasticsearch.ad.stats.ADStats;
import com.amazon.opendistroforelasticsearch.ad.stats.StatNames;
import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.CounterSupplier;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.IndexUtils;
import com.amazon.opendistroforelasticsearch.ad.util.Throttler;
import com.google.gson.JsonElement;

public class AnomalyResultTests extends AbstractADTest {
    private static Settings settings = Settings.EMPTY;
    private TransportService transportService;
    private ClusterService clusterService;
    private TransportStateManager stateManager;
    private FeatureManager featureQuery;
    private ModelManager normalModelManager;
    private Client client;
    private AnomalyDetector detector;
    private HashRing hashRing;
    private IndexNameExpressionResolver indexNameResolver;
    private String rcfModelID;
    private String thresholdModelID;
    private String adID;
    private String featureId;
    private String featureName;
    private ADCircuitBreakerService adCircuitBreakerService;
    private ADStats adStats;
    private int partitionNum;

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(AnomalyResultTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
    }

    @SuppressWarnings("unchecked")
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        super.setUpLog4jForJUnit(AnomalyResultTransportAction.class);
        setupTestNodes(settings);

        transportService = testNodes[0].transportService;
        clusterService = testNodes[0].clusterService;
        stateManager = mock(TransportStateManager.class);
        // return 2 RCF partitions
        partitionNum = 2;
        when(stateManager.getPartitionNumber(any(String.class), any(AnomalyDetector.class))).thenReturn(partitionNum);
        when(stateManager.isMuted(any(String.class))).thenReturn(false);

        detector = mock(AnomalyDetector.class);
        featureId = "xyz";
        // we have one feature
        when(detector.getEnabledFeatureIds()).thenReturn(Collections.singletonList(featureId));
        featureName = "abc";
        when(detector.getEnabledFeatureNames()).thenReturn(Collections.singletonList(featureName));
        List<String> userIndex = new ArrayList<>();
        userIndex.add("test*");
        when(detector.getIndices()).thenReturn(userIndex);
        adID = "123";
        when(detector.getDetectorId()).thenReturn(adID);
        // when(detector.getDetectorId()).thenReturn("testDetectorId");
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(detector));
            return null;
        }).when(stateManager).getAnomalyDetector(any(String.class), any(ActionListener.class));

        hashRing = mock(HashRing.class);
        when(hashRing.getOwningNode(any(String.class))).thenReturn(Optional.of(clusterService.state().nodes().getLocalNode()));
        when(hashRing.build()).thenReturn(true);
        featureQuery = mock(FeatureManager.class);

        doAnswer(invocation -> {
            ActionListener<SinglePointFeatures> listener = invocation.getArgument(3);
            listener.onResponse(new SinglePointFeatures(Optional.of(new double[] { 0.0d }), Optional.of(new double[] { 0 })));
            return null;
        }).when(featureQuery).getCurrentFeatures(any(AnomalyDetector.class), anyLong(), anyLong(), any(ActionListener.class));

        normalModelManager = mock(ModelManager.class);
        doAnswer(invocation -> {
            ActionListener<ThresholdingResult> listener = invocation.getArgument(3);
            listener.onResponse(new ThresholdingResult(0, 1.0d));
            return null;
        }).when(normalModelManager).getThresholdingResult(any(String.class), any(String.class), anyDouble(), any(ActionListener.class));

        doAnswer(invocation -> {
            ActionListener<RcfResult> listener = invocation.getArgument(3);
            listener.onResponse(new RcfResult(0.2, 0, 100, new double[] { 1 }));
            return null;
        }).when(normalModelManager).getRcfResult(any(String.class), any(String.class), any(double[].class), any(ActionListener.class));
        when(normalModelManager.combineRcfResults(any(), anyInt())).thenReturn(new CombinedRcfResult(0, 1.0d, new double[] { 1 }));

        rcfModelID = "123-rcf-1";
        when(normalModelManager.getRcfModelId(any(String.class), anyInt())).thenReturn(rcfModelID);
        thresholdModelID = "123-threshold";
        when(normalModelManager.getThresholdModelId(any(String.class))).thenReturn(thresholdModelID);
        adCircuitBreakerService = mock(ADCircuitBreakerService.class);
        when(adCircuitBreakerService.isOpen()).thenReturn(false);

        client = mock(Client.class);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length >= 2);

            IndexRequest request = null;
            ActionListener<IndexResponse> listener = null;
            if (args[0] instanceof IndexRequest) {
                request = (IndexRequest) args[0];
            }
            if (args[1] instanceof ActionListener) {
                listener = (ActionListener<IndexResponse>) args[1];
            }

            assertTrue(request != null && listener != null);
            ShardId shardId = new ShardId(new Index(CommonName.ANOMALY_RESULT_INDEX_ALIAS, randomAlphaOfLength(10)), 0);
            listener.onResponse(new IndexResponse(shardId, randomAlphaOfLength(10), request.id(), 1, 1, 1, true));

            return null;
        }).when(client).index(any(), any());

        indexNameResolver = new IndexNameExpressionResolver();
        Clock clock = mock(Clock.class);
        Throttler throttler = new Throttler(clock);
        ThreadPool threadpool = mock(ThreadPool.class);
        ClientUtil clientUtil = new ClientUtil(Settings.EMPTY, client, throttler, threadpool);
        IndexUtils indexUtils = new IndexUtils(client, clientUtil, clusterService, indexNameResolver);

        Map<String, ADStat<?>> statsMap = new HashMap<String, ADStat<?>>() {
            {
                put(StatNames.AD_EXECUTE_REQUEST_COUNT.getName(), new ADStat<>(false, new CounterSupplier()));
                put(StatNames.AD_EXECUTE_FAIL_COUNT.getName(), new ADStat<>(false, new CounterSupplier()));
            }
        };

        adStats = new ADStats(indexUtils, normalModelManager, statsMap);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            GetRequest request = (GetRequest) args[0];
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            if (request.index().equals(DetectorInternalState.DETECTOR_STATE_INDEX)) {

                DetectorInternalState.Builder result = new DetectorInternalState.Builder().lastUpdateTime(Instant.now());

                listener
                    .onResponse(
                        TestHelpers.createGetResponse(result.build(), detector.getDetectorId(), DetectorInternalState.DETECTOR_STATE_INDEX)
                    );

            }

            return null;
        }).when(client).get(any(), any());
    }

    @Override
    @After
    public final void tearDown() throws Exception {
        tearDownTestNodes();
        client = null;
        super.tearDownLog4jForJUnit();
        super.tearDown();
    }

    private Throwable assertException(PlainActionFuture<AnomalyResultResponse> listener, Class<? extends Exception> exceptionType) {
        return expectThrows(exceptionType, () -> listener.actionGet());
    }

    public void testNormal() throws IOException {

        // These constructors register handler in transport service
        new RCFResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            normalModelManager,
            adCircuitBreakerService
        );
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertAnomalyResultResponse(response, 0, 1, 0d);
    }

    private void assertAnomalyResultResponse(AnomalyResultResponse response, double anomalyGrade, double confidence, double featureData) {
        assertEquals(anomalyGrade, response.getAnomalyGrade(), 0.001);
        assertEquals(confidence, response.getConfidence(), 0.001);
        assertEquals(1, response.getFeatures().size());
        FeatureData responseFeature = response.getFeatures().get(0);
        assertEquals(featureData, responseFeature.getData(), 0.001);
        assertEquals(featureId, responseFeature.getFeatureId());
        assertEquals(featureName, responseFeature.getFeatureName());
    }

    /**
     * Create handler that would return a failure
     * @param handler callback handler
     * @return handler that would return a failure
     */
    private <T extends TransportResponse> TransportResponseHandler<T> rcfFailureHandler(
        TransportResponseHandler<T> handler,
        Exception exception
    ) {
        return new TransportResponseHandler<T>() {
            @Override
            public T read(StreamInput in) throws IOException {
                return handler.read(in);
            }

            @Override
            public void handleResponse(T response) {
                handler.handleException(new RemoteTransportException("test", exception));
            }

            @Override
            public void handleException(TransportException exp) {
                handler.handleException(exp);
            }

            @Override
            public String executor() {
                return handler.executor();
            }
        };
    }

    public void noModelExceptionTemplate(
        Exception thrownException,
        String adID,
        Class<? extends Exception> expectedExceptionType,
        String error
    ) {

        TransportInterceptor failureTransportInterceptor = new TransportInterceptor() {
            @Override
            public AsyncSender interceptSender(AsyncSender sender) {
                return new AsyncSender() {
                    @Override
                    public <T extends TransportResponse> void sendRequest(
                        Transport.Connection connection,
                        String action,
                        TransportRequest request,
                        TransportRequestOptions options,
                        TransportResponseHandler<T> handler
                    ) {
                        if (RCFResultAction.NAME.equals(action)) {
                            sender.sendRequest(connection, action, request, options, rcfFailureHandler(handler, thrownException));
                        } else {
                            sender.sendRequest(connection, action, request, options, handler);
                        }
                    }
                };
            }
        };

        ModelManager rcfManager = mock(ModelManager.class);
        when(rcfManager.getRcfModelId(any(String.class), anyInt())).thenReturn(rcfModelID);

        // need to close nodes created in the setUp nodes and create new nodes
        // for the failure interceptor. Otherwise, we will get thread leak error.
        tearDownTestNodes();
        setupTestNodes(Settings.EMPTY, failureTransportInterceptor);

        // mock hashing ring response. This has to happen after setting up test nodes with the failure interceptor
        when(hashRing.getOwningNode(any(String.class))).thenReturn(Optional.of(testNodes[1].discoveryNode()));
        // register handler on testNodes[1]
        new RCFResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            testNodes[1].transportService,
            normalModelManager,
            adCircuitBreakerService
        );

        TransportService realTransportService = testNodes[0].transportService;
        ClusterService realClusterService = testNodes[0].clusterService;

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            realTransportService,
            settings,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            realClusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        Throwable exception = assertException(listener, expectedExceptionType);
        assertTrue("actual message: " + exception.getMessage(), exception.getMessage().contains(error));
    }

    public void noModelExceptionTemplate(Exception exception, String adID, String error) {
        noModelExceptionTemplate(exception, adID, exception.getClass(), error);
    }

    public void testNormalColdStart() {
        noModelExceptionTemplate(
            new ResourceNotFoundException(adID, ""),
            adID,
            InternalFailure.class,
            AnomalyResultTransportAction.NO_MODEL_ERR_MSG
        );
    }

    public void testNormalColdStartRemoteException() {
        noModelExceptionTemplate(
            new NotSerializableExceptionWrapper(new ResourceNotFoundException(adID, "")),
            adID,
            AnomalyDetectionException.class,
            AnomalyResultTransportAction.NO_MODEL_ERR_MSG
        );
    }

    public void testNullPointerExceptionWhenRCF() {
        noModelExceptionTemplate(new NullPointerException(), adID, EndRunException.class, AnomalyResultTransportAction.BUG_RESPONSE);
    }

    public void testADExceptionWhenColdStart() {
        String error = "blah";
        when(stateManager.fetchColdStartException(any(String.class))).thenReturn(Optional.of(new AnomalyDetectionException(adID, error)));

        noModelExceptionTemplate(new ResourceNotFoundException(adID, ""), adID, AnomalyDetectionException.class, error);
    }

    @SuppressWarnings("unchecked")
    public void testInsufficientCapacityExceptionDuringColdStart() {

        ModelManager rcfManager = mock(ModelManager.class);
        doThrow(ResourceNotFoundException.class)
            .when(rcfManager)
            .getRcfResult(any(String.class), any(String.class), any(double[].class), any(ActionListener.class));
        when(rcfManager.getRcfModelId(any(String.class), anyInt())).thenReturn(rcfModelID);

        when(stateManager.fetchColdStartException(any(String.class)))
            .thenReturn(Optional.of(new LimitExceededException(adID, CommonErrorMessages.MEMORY_LIMIT_EXCEEDED_ERR_MSG)));

        // These constructors register handler in transport service
        new RCFResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, rcfManager, adCircuitBreakerService);
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        assertException(listener, LimitExceededException.class);
    }

    @SuppressWarnings("unchecked")
    public void testInsufficientCapacityExceptionDuringRestoringModel() {

        ModelManager rcfManager = mock(ModelManager.class);
        doThrow(new NotSerializableExceptionWrapper(new LimitExceededException(adID, CommonErrorMessages.MEMORY_LIMIT_EXCEEDED_ERR_MSG)))
            .when(rcfManager)
            .getRcfResult(any(String.class), any(String.class), any(double[].class), any(ActionListener.class));

        // These constructors register handler in transport service
        new RCFResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, rcfManager, adCircuitBreakerService);
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        assertException(listener, LimitExceededException.class);
    }

    private <T extends TransportResponse> TransportResponseHandler<T> rcfResponseHandler(TransportResponseHandler<T> handler) {
        return new TransportResponseHandler<T>() {
            @Override
            public T read(StreamInput in) throws IOException {
                return handler.read(in);
            }

            @Override
            @SuppressWarnings("unchecked")
            public void handleResponse(T response) {
                handler.handleResponse((T) new RCFResultResponse(1, 1, 100, new double[0]));
            }

            @Override
            public void handleException(TransportException exp) {
                handler.handleException(exp);
            }

            @Override
            public String executor() {
                return handler.executor();
            }
        };
    }

    public void thresholdExceptionTestTemplate(
        Exception thrownException,
        String adID,
        Class<? extends Exception> expectedExceptionType,
        String error
    ) {

        TransportInterceptor failureTransportInterceptor = new TransportInterceptor() {
            @Override
            public AsyncSender interceptSender(AsyncSender sender) {
                return new AsyncSender() {
                    @Override
                    public <T extends TransportResponse> void sendRequest(
                        Transport.Connection connection,
                        String action,
                        TransportRequest request,
                        TransportRequestOptions options,
                        TransportResponseHandler<T> handler
                    ) {
                        if (ThresholdResultAction.NAME.equals(action)) {
                            sender.sendRequest(connection, action, request, options, rcfFailureHandler(handler, thrownException));
                        } else if (RCFResultAction.NAME.equals(action)) {
                            sender.sendRequest(connection, action, request, options, rcfResponseHandler(handler));
                        } else {
                            sender.sendRequest(connection, action, request, options, handler);
                        }
                    }
                };
            }
        };

        ModelManager rcfManager = mock(ModelManager.class);
        when(rcfManager.getRcfModelId(any(String.class), anyInt())).thenReturn(rcfModelID);

        // need to close nodes created in the setUp nodes and create new nodes
        // for the failure interceptor. Otherwise, we will get thread leak error.
        tearDownTestNodes();
        setupTestNodes(Settings.EMPTY, failureTransportInterceptor);

        // mock hashing ring response. This has to happen after setting up test nodes with the failure interceptor
        when(hashRing.getOwningNode(any(String.class))).thenReturn(Optional.of(testNodes[1].discoveryNode()));
        // register handlers on testNodes[1]
        ActionFilters actionFilters = new ActionFilters(Collections.emptySet());
        new RCFResultTransportAction(actionFilters, testNodes[1].transportService, normalModelManager, adCircuitBreakerService);
        new ThresholdResultTransportAction(actionFilters, testNodes[1].transportService, normalModelManager);

        TransportService realTransportService = testNodes[0].transportService;
        ClusterService realClusterService = testNodes[0].clusterService;

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            realTransportService,
            settings,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            realClusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        Throwable exception = assertException(listener, expectedExceptionType);
        assertTrue("actual message: " + exception.getMessage(), exception.getMessage().contains(error));
    }

    public void testThresholdException() {
        thresholdExceptionTestTemplate(new NullPointerException(), adID, EndRunException.class, AnomalyResultTransportAction.BUG_RESPONSE);
    }

    public void testCircuitBreaker() {

        ADCircuitBreakerService breakerService = mock(ADCircuitBreakerService.class);
        when(breakerService.isOpen()).thenReturn(true);

        // These constructors register handler in transport service
        new RCFResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager, breakerService);
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            breakerService,
            adStats,
            threadPool
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        assertException(listener, LimitExceededException.class);
    }

    /**
     * Test whether we can handle NodeNotConnectedException when sending requests to
     * remote nodes.
     *
     * @param isRCF             whether RCF model node throws node connection
     *                          exception or not
     * @param temporary         whether node has only temporary connection issue. If
     *                          yes, we should not trigger hash ring rebuilding.
     * @param numberOfBuildCall the number of expected hash ring build call
     */
    private void nodeNotConnectedExceptionTemplate(boolean isRCF, boolean temporary, int numberOfBuildCall) {
        ClusterService hackedClusterService = spy(clusterService);

        TransportService exceptionTransportService = spy(transportService);

        DiscoveryNode rcfNode = clusterService.state().nodes().getLocalNode();
        DiscoveryNode thresholdNode = testNodes[1].discoveryNode();

        if (isRCF) {
            doThrow(new NodeNotConnectedException(rcfNode, "rcf node not connected"))
                .when(exceptionTransportService)
                .getConnection(same(rcfNode));
        } else {
            when(hashRing.getOwningNode(eq(thresholdModelID))).thenReturn(Optional.of(thresholdNode));
            doThrow(new NodeNotConnectedException(rcfNode, "rcf node not connected"))
                .when(exceptionTransportService)
                .getConnection(same(thresholdNode));
        }

        if (!temporary) {
            when(hackedClusterService.state()).thenReturn(ClusterState.builder(new ClusterName("test")).build());
        }

        // These constructors register handler in transport service
        new RCFResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            exceptionTransportService,
            normalModelManager,
            adCircuitBreakerService
        );
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), exceptionTransportService, normalModelManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            exceptionTransportService,
            settings,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            hackedClusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        assertException(listener, AnomalyDetectionException.class);

        if (!temporary) {
            verify(hashRing, times(numberOfBuildCall)).build();
            verify(stateManager, never()).addPressure(any(String.class));
        } else {
            verify(hashRing, never()).build();
            // expect 2 times since we have 2 RCF model partitions
            verify(stateManager, times(numberOfBuildCall)).addPressure(any(String.class));
        }
    }

    public void testRCFNodeNotConnectedException() {
        // we expect two hashRing.build calls since we have two RCF model partitions and
        // both of them returns node not connected exception
        nodeNotConnectedExceptionTemplate(true, false, 2);
    }

    public void testTemporaryRCFNodeNotConnectedException() {
        // we expect two backpressure incrementBackpressureCounter calls since we have
        // two RCF model partitions and both of them returns node not connected
        // exception
        nodeNotConnectedExceptionTemplate(true, true, 2);
    }

    public void testThresholdNodeNotConnectedException() {
        // we expect one hashRing.build calls since we have one threshold model
        // partition
        nodeNotConnectedExceptionTemplate(false, false, 1);
    }

    public void testTemporaryThresholdNodeNotConnectedException() {
        // we expect one backpressure incrementBackpressureCounter call since we have
        // one threshold model partition
        nodeNotConnectedExceptionTemplate(false, true, 1);
    }

    @SuppressWarnings("unchecked")
    public void testMute() {
        TransportStateManager muteStateManager = mock(TransportStateManager.class);
        when(muteStateManager.isMuted(any(String.class))).thenReturn(true);
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(detector));
            return null;
        }).when(muteStateManager).getAnomalyDetector(any(String.class), any(ActionListener.class));
        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            muteStateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool
        );
        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        Throwable exception = assertException(listener, AnomalyDetectionException.class);
        assertThat(exception.getMessage(), containsString(AnomalyResultTransportAction.NODE_UNRESPONSIVE_ERR_MSG));
    }

    public void alertingRequestTemplate(boolean anomalyResultIndexExists) throws IOException {
        // These constructors register handler in transport service
        new RCFResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            normalModelManager,
            adCircuitBreakerService
        );
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool
        );

        TransportRequestOptions option = TransportRequestOptions
            .builder()
            .withType(TransportRequestOptions.Type.STATE)
            .withTimeout(6000)
            .build();

        transportService
            .sendRequest(
                clusterService.state().nodes().getLocalNode(),
                AnomalyResultAction.NAME,
                new AnomalyResultRequest(adID, 100, 200),
                option,
                new TransportResponseHandler<AnomalyResultResponse>() {

                    @Override
                    public AnomalyResultResponse read(StreamInput in) throws IOException {
                        return new AnomalyResultResponse(in);
                    }

                    @Override
                    public void handleResponse(AnomalyResultResponse response) {
                        assertAnomalyResultResponse(response, 0, 1, 0d);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        assertThat(exp, is(nullValue()));
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }
                }
            );
    }

    public void testAlertingRequestWithoutResultIndex() throws IOException {
        alertingRequestTemplate(false);
    }

    public void testAlertingRequestWithResultIndex() throws IOException {
        alertingRequestTemplate(true);
    }

    public void testSerialzationResponse() throws IOException {
        AnomalyResultResponse response = new AnomalyResultResponse(
            4,
            0.993,
            1.01,
            Collections.singletonList(new FeatureData(featureId, featureName, 0d))
        );
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        AnomalyResultResponse readResponse = AnomalyResultAction.INSTANCE.getResponseReader().read(streamInput);
        assertAnomalyResultResponse(readResponse, readResponse.getAnomalyGrade(), readResponse.getConfidence(), 0d);
    }

    public void testJsonResponse() throws IOException, JsonPathNotFoundException {
        AnomalyResultResponse response = new AnomalyResultResponse(
            4,
            0.993,
            1.01,
            Collections.singletonList(new FeatureData(featureId, featureName, 0d))
        );
        XContentBuilder builder = jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String json = Strings.toString(builder);
        Function<JsonElement, FeatureData> function = (s) -> {
            try {
                String featureId = JsonDeserializer.getTextValue(s, FeatureData.FEATURE_ID_FIELD);
                String featureName = JsonDeserializer.getTextValue(s, FeatureData.FEATURE_NAME_FIELD);
                double featureValue = JsonDeserializer.getDoubleValue(s, FeatureData.DATA_FIELD);
                return new FeatureData(featureId, featureName, featureValue);
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
            return null;
        };

        AnomalyResultResponse readResponse = new AnomalyResultResponse(
            JsonDeserializer.getDoubleValue(json, AnomalyResultResponse.ANOMALY_GRADE_JSON_KEY),
            JsonDeserializer.getDoubleValue(json, AnomalyResultResponse.CONFIDENCE_JSON_KEY),
            JsonDeserializer.getDoubleValue(json, AnomalyResultResponse.ANOMALY_SCORE_JSON_KEY),
            JsonDeserializer.getListValue(json, function, AnomalyResultResponse.FEATURES_JSON_KEY)
        );
        assertAnomalyResultResponse(readResponse, readResponse.getAnomalyGrade(), readResponse.getConfidence(), 0d);
    }

    public void testSerialzationRequest() throws IOException {
        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        AnomalyResultRequest readRequest = new AnomalyResultRequest(streamInput);
        assertThat(request.getAdID(), equalTo(readRequest.getAdID()));
        assertThat(request.getStart(), equalTo(readRequest.getStart()));
        assertThat(request.getEnd(), equalTo(readRequest.getEnd()));
    }

    public void testJsonRequest() throws IOException, JsonPathNotFoundException {
        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        XContentBuilder builder = jsonBuilder();
        request.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String json = Strings.toString(builder);
        assertEquals(JsonDeserializer.getTextValue(json, CommonMessageAttributes.ID_JSON_KEY), request.getAdID());
        assertEquals(JsonDeserializer.getLongValue(json, AnomalyResultRequest.START_JSON_KEY), request.getStart());
        assertEquals(JsonDeserializer.getLongValue(json, AnomalyResultRequest.END_JSON_KEY), request.getEnd());
    }

    public void testEmptyID() {
        ActionRequestValidationException e = new AnomalyResultRequest("", 100, 200).validate();
        assertThat(e.validationErrors(), hasItem(CommonErrorMessages.AD_ID_MISSING_MSG));
    }

    public void testZeroStartTime() {
        ActionRequestValidationException e = new AnomalyResultRequest(adID, 0, 200).validate();
        assertThat(e.validationErrors(), hasItem(startsWith(AnomalyResultRequest.INVALID_TIMESTAMP_ERR_MSG)));
    }

    public void testNegativeEndTime() {
        ActionRequestValidationException e = new AnomalyResultRequest(adID, 0, -200).validate();
        assertThat(e.validationErrors(), hasItem(startsWith(AnomalyResultRequest.INVALID_TIMESTAMP_ERR_MSG)));
    }

    public void testNegativeTime() {
        ActionRequestValidationException e = new AnomalyResultRequest(adID, 10, -200).validate();
        assertThat(e.validationErrors(), hasItem(startsWith(AnomalyResultRequest.INVALID_TIMESTAMP_ERR_MSG)));
    }

    // no exception should be thrown
    public void testOnFailureNull() throws IOException {
        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool
        );
        AnomalyResultTransportAction.RCFActionListener listener = action.new RCFActionListener(
            null, null, null, null, null, null, null, null, null, 0, new AtomicInteger(), null, 1
        );
        listener.onFailure(null);
    }

    @SuppressWarnings("unchecked")
    private void setUpColdStart(ThreadPool mockThreadPool, boolean coldStartRunning) {
        SinglePointFeatures mockSinglePoint = mock(SinglePointFeatures.class);

        when(mockSinglePoint.getProcessedFeatures()).thenReturn(Optional.empty());

        doAnswer(invocation -> {
            ActionListener<SinglePointFeatures> listener = invocation.getArgument(3);
            listener.onResponse(mockSinglePoint);
            return null;
        }).when(featureQuery).getCurrentFeatures(any(AnomalyDetector.class), anyLong(), anyLong(), any(ActionListener.class));

        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(Boolean.FALSE);
            return null;
        }).when(stateManager).getDetectorCheckpoint(any(String.class), any(ActionListener.class));

        when(stateManager.isColdStartRunning(any(String.class))).thenReturn(coldStartRunning);

        ExecutorService executorService = mock(ExecutorService.class);

        when(mockThreadPool.executor(AnomalyDetectorPlugin.AD_THREAD_POOL_NAME)).thenReturn(executorService);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
    }

    @SuppressWarnings("unchecked")
    public void testColdStartNoTrainingData() throws Exception {
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        setUpColdStart(mockThreadPool, false);

        doAnswer(invocation -> {
            ActionListener<Optional<double[][]>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.empty());
            return null;
        }).when(featureQuery).getColdStartData(any(AnomalyDetector.class), any(ActionListener.class));

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            mockThreadPool
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        verify(stateManager, times(1)).setLastColdStartException(eq(adID), any(EndRunException.class));
        verify(stateManager, times(2)).setColdStartRunning(eq(adID), anyBoolean());
    }

    @SuppressWarnings("unchecked")
    public void testConcurrentColdStart() throws Exception {
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        setUpColdStart(mockThreadPool, true);

        doAnswer(invocation -> {
            ActionListener<Optional<double[][]>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.empty());
            return null;
        }).when(featureQuery).getColdStartData(any(AnomalyDetector.class), any(ActionListener.class));

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            mockThreadPool
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        verify(stateManager, never()).setLastColdStartException(eq(adID), any(EndRunException.class));
        verify(stateManager, never()).setColdStartRunning(eq(adID), anyBoolean());
    }

    @SuppressWarnings("unchecked")
    public void testColdStartTimeoutPutCheckpoint() throws Exception {
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        setUpColdStart(mockThreadPool, false);

        doAnswer(invocation -> {
            ActionListener<Optional<double[][]>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(new double[][] { { 1.0 } }));
            return null;
        }).when(featureQuery).getColdStartData(any(AnomalyDetector.class), any(ActionListener.class));

        doAnswer(invocation -> {
            ActionListener<Optional<double[][]>> listener = invocation.getArgument(2);
            listener.onFailure(new ElasticsearchTimeoutException(""));
            return null;
        }).when(normalModelManager).trainModel(any(AnomalyDetector.class), any(double[][].class), any(ActionListener.class));

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            mockThreadPool
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        verify(stateManager, times(1)).setLastColdStartException(eq(adID), any(InternalFailure.class));
        verify(stateManager, times(2)).setColdStartRunning(eq(adID), anyBoolean());
    }

    @SuppressWarnings("unchecked")
    public void testColdStartIllegalArgumentException() throws Exception {
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        setUpColdStart(mockThreadPool, false);

        doAnswer(invocation -> {
            ActionListener<Optional<double[][]>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(new double[][] { { 1.0 } }));
            return null;
        }).when(featureQuery).getColdStartData(any(AnomalyDetector.class), any(ActionListener.class));

        doAnswer(invocation -> {
            ActionListener<Optional<double[][]>> listener = invocation.getArgument(2);
            listener.onFailure(new IllegalArgumentException(""));
            return null;
        }).when(normalModelManager).trainModel(any(AnomalyDetector.class), any(double[][].class), any(ActionListener.class));

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            mockThreadPool
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        verify(stateManager, times(1)).setLastColdStartException(eq(adID), any(EndRunException.class));
        verify(stateManager, times(2)).setColdStartRunning(eq(adID), anyBoolean());
    }

    enum FeatureTestMode {
        FEATURE_NOT_AVAILABLE,
        ILLEGAL_STATE,
        AD_EXCEPTION
    }

    @SuppressWarnings("unchecked")
    public void featureTestTemplate(FeatureTestMode mode) throws IOException {
        if (mode == FeatureTestMode.FEATURE_NOT_AVAILABLE) {
            doAnswer(invocation -> {
                ActionListener<SinglePointFeatures> listener = invocation.getArgument(3);
                listener.onResponse(new SinglePointFeatures(Optional.empty(), Optional.empty()));
                return null;
            }).when(featureQuery).getCurrentFeatures(any(AnomalyDetector.class), anyLong(), anyLong(), any(ActionListener.class));
        } else if (mode == FeatureTestMode.ILLEGAL_STATE) {
            doThrow(IllegalArgumentException.class)
                .when(featureQuery)
                .getCurrentFeatures(any(AnomalyDetector.class), anyLong(), anyLong(), any(ActionListener.class));
        } else if (mode == FeatureTestMode.AD_EXCEPTION) {
            doThrow(AnomalyDetectionException.class)
                .when(featureQuery)
                .getCurrentFeatures(any(AnomalyDetector.class), anyLong(), anyLong(), any(ActionListener.class));
        }

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        if (mode == FeatureTestMode.FEATURE_NOT_AVAILABLE) {
            AnomalyResultResponse response = listener.actionGet();
            assertEquals(Double.NaN, response.getAnomalyGrade(), 0.001);
            assertEquals(Double.NaN, response.getConfidence(), 0.001);
            assertEquals(Double.NaN, response.getAnomalyScore(), 0.001);
            assertThat(response.getFeatures(), is(empty()));
        } else if (mode == FeatureTestMode.ILLEGAL_STATE || mode == FeatureTestMode.AD_EXCEPTION) {
            assertException(listener, InternalFailure.class);
        }
    }

    public void testFeatureNotAvailable() throws IOException {
        featureTestTemplate(FeatureTestMode.FEATURE_NOT_AVAILABLE);
    }

    public void testFeatureIllegalState() throws IOException {
        featureTestTemplate(FeatureTestMode.ILLEGAL_STATE);
    }

    public void testFeatureAnomalyException() throws IOException {
        featureTestTemplate(FeatureTestMode.AD_EXCEPTION);
    }

    enum BlockType {
        INDEX_BLOCK,
        GLOBAL_BLOCK_WRITE,
        GLOBAL_BLOCK_READ
    }

    private void globalBlockTemplate(BlockType type, String errLogMsg, Settings indexSettings, String indexName) {
        ClusterState blockedClusterState = null;

        switch (type) {
            case GLOBAL_BLOCK_WRITE:
                blockedClusterState = ClusterState
                    .builder(new ClusterName("test cluster"))
                    .blocks(ClusterBlocks.builder().addGlobalBlock(IndexMetadata.INDEX_WRITE_BLOCK))
                    .build();
                break;
            case GLOBAL_BLOCK_READ:
                blockedClusterState = ClusterState
                    .builder(new ClusterName("test cluster"))
                    .blocks(ClusterBlocks.builder().addGlobalBlock(IndexMetadata.INDEX_READ_BLOCK))
                    .build();
                break;
            case INDEX_BLOCK:
                blockedClusterState = createIndexBlockedState(indexName, indexSettings, null);
                break;
            default:
                break;
        }

        ClusterService hackedClusterService = spy(clusterService);
        when(hackedClusterService.state()).thenReturn(blockedClusterState);

        // These constructors register handler in transport service
        new RCFResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            normalModelManager,
            adCircuitBreakerService
        );
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            hackedClusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        assertException(listener, AnomalyDetectionException.class, errLogMsg);
    }

    private void globalBlockTemplate(BlockType type, String errLogMsg) {
        globalBlockTemplate(type, errLogMsg, null, null);
    }

    public void testReadBlock() {
        globalBlockTemplate(BlockType.GLOBAL_BLOCK_READ, AnomalyResultTransportAction.READ_WRITE_BLOCKED);
    }

    public void testWriteBlock() {
        globalBlockTemplate(BlockType.GLOBAL_BLOCK_WRITE, AnomalyResultTransportAction.READ_WRITE_BLOCKED);
    }

    public void testIndexReadBlock() {
        globalBlockTemplate(
            BlockType.INDEX_BLOCK,
            AnomalyResultTransportAction.INDEX_READ_BLOCKED,
            Settings.builder().put(IndexMetadata.INDEX_BLOCKS_READ_SETTING.getKey(), true).build(),
            "test1"
        );
    }

    public void testNullRCFResult() {
        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool
        );
        AnomalyResultTransportAction.RCFActionListener listener = action.new RCFActionListener(
            null, "123-rcf-0", null, "123", null, null, null, null, null, 0, new AtomicInteger(), null, 1
        );
        listener.onResponse(null);
        assertTrue(testAppender.containsMessage(AnomalyResultTransportAction.NULL_RESPONSE));
    }

    @SuppressWarnings("unchecked")
    public void testAllFeaturesDisabled() throws IOException {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<SinglePointFeatures> listener = (ActionListener<SinglePointFeatures>) args[3];
            listener.onFailure(new IllegalArgumentException());
            return null;
        }).when(featureQuery).getCurrentFeatures(any(AnomalyDetector.class), anyLong(), anyLong(), any(ActionListener.class));
        when(detector.getEnabledFeatureIds()).thenReturn(Collections.emptyList());

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        assertException(listener, EndRunException.class, AnomalyResultTransportAction.ALL_FEATURES_DISABLED_ERR_MSG);
    }

    @SuppressWarnings("unchecked")
    public void testEndRunDueToNoTrainingData() {
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        setUpColdStart(mockThreadPool, false);

        ModelManager rcfManager = mock(ModelManager.class);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<RcfResult> listener = (ActionListener<RcfResult>) args[3];
            listener.onFailure(new IndexNotFoundException(CommonName.CHECKPOINT_INDEX_NAME));
            return null;
        }).when(rcfManager).getRcfResult(any(String.class), any(String.class), any(double[].class), any(ActionListener.class));
        when(rcfManager.getRcfModelId(any(String.class), anyInt())).thenReturn(rcfModelID);

        when(stateManager.fetchColdStartException(any(String.class)))
            .thenReturn(Optional.of(new EndRunException(adID, "Cannot get training data", false)));

        doAnswer(invocation -> {
            ActionListener<Optional<double[][]>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(new double[][] { { 1.0 } }));
            return null;
        }).when(featureQuery).getColdStartData(any(AnomalyDetector.class), any(ActionListener.class));

        doAnswer(invocation -> {
            ActionListener<Optional<Void>> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(normalModelManager).trainModel(any(AnomalyDetector.class), any(double[][].class), any(ActionListener.class));

        // These constructors register handler in transport service
        new RCFResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, rcfManager, adCircuitBreakerService);
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            mockThreadPool
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        assertException(listener, EndRunException.class);
        ArgumentCaptor<Boolean> booleanCaptor = ArgumentCaptor.forClass(Boolean.class);
        verify(stateManager, times(2)).setColdStartRunning(eq(adID), booleanCaptor.capture());
        List<Boolean> capturedBoolean = booleanCaptor.getAllValues();
        // first, we set cold start running to true; then false
        assertTrue(capturedBoolean.get(0));
        assertTrue(!capturedBoolean.get(1));
    }
}
