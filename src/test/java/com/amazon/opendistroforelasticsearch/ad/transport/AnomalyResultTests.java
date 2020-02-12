/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyDouble;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.cluster.HashRing;
import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ClientException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.InternalFailure;
import com.amazon.opendistroforelasticsearch.ad.common.exception.JsonPathNotFoundException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ResourceNotFoundException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonMessageAttributes;
import com.amazon.opendistroforelasticsearch.ad.feature.FeatureManager;
import com.amazon.opendistroforelasticsearch.ad.feature.SinglePointFeatures;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.RcfResult;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingResult;
import com.amazon.opendistroforelasticsearch.ad.ml.rcf.CombinedRcfResult;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.FeatureData;
import com.amazon.opendistroforelasticsearch.ad.stats.ADStat;
import com.amazon.opendistroforelasticsearch.ad.stats.ADStats;
import com.amazon.opendistroforelasticsearch.ad.stats.StatNames;
import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.CounterSupplier;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.ColdStartRunner;
import com.amazon.opendistroforelasticsearch.ad.util.IndexUtils;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import com.google.gson.JsonElement;

import test.com.amazon.opendistroforelasticsearch.ad.util.FakeNode;
import test.com.amazon.opendistroforelasticsearch.ad.util.JsonDeserializer;

public class AnomalyResultTests extends AbstractADTest {
    private static Settings settings = Settings.EMPTY;
    private FakeNode[] testNodes;
    private int nodesCount;
    private TransportService transportService;
    private ClusterService clusterService;
    private ADStateManager stateManager;
    private ColdStartRunner runner;
    private FeatureManager featureQuery;
    private ModelManager normalModelManager;
    private Client client;
    private AnomalyDetectionIndices anomalyDetectionIndices;
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

    @SuppressWarnings("unchecked")
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        super.setUpLog4jForJUnit(AnomalyResultTransportAction.class);
        setupTestNodes(Settings.EMPTY);
        FakeNode.connectNodes(testNodes);
        transportService = testNodes[0].transportService;
        clusterService = testNodes[0].clusterService;
        stateManager = mock(ADStateManager.class);
        // return 2 RCF partitions
        when(stateManager.getPartitionNumber(any(String.class))).thenReturn(2);
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
        when(stateManager.getAnomalyDetector(any(String.class))).thenReturn(Optional.of(detector));

        hashRing = mock(HashRing.class);
        when(hashRing.getOwningNode(any(String.class))).thenReturn(Optional.of(clusterService.state().nodes().getLocalNode()));
        when(hashRing.build()).thenReturn(true);
        featureQuery = mock(FeatureManager.class);
        when(featureQuery.getCurrentFeatures(any(AnomalyDetector.class), anyLong(), anyLong()))
            .thenReturn(new SinglePointFeatures(Optional.of(new double[] { 0.0d }), Optional.of(new double[] { 0 })));
        normalModelManager = mock(ModelManager.class);
        when(normalModelManager.getThresholdingResult(any(String.class), any(String.class), anyDouble()))
            .thenReturn(new ThresholdingResult(0, 1.0d));
        when(normalModelManager.getRcfResult(any(String.class), any(String.class), any(double[].class)))
            .thenReturn(new RcfResult(0.2, 0, 100));
        when(normalModelManager.combineRcfResults(any())).thenReturn(new CombinedRcfResult(0, 1.0d));
        adID = "123";
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
            ShardId shardId = new ShardId(new Index(AnomalyResult.ANOMALY_RESULT_INDEX, randomAlphaOfLength(10)), 0);
            listener.onResponse(new IndexResponse(shardId, randomAlphaOfLength(10), request.id(), 1, 1, 1, true));

            return null;
        }).when(client).index(any(), any());

        indexNameResolver = new IndexNameExpressionResolver();

        ClientUtil clientUtil = new ClientUtil(Settings.EMPTY, client);
        IndexUtils indexUtils = new IndexUtils(client, clientUtil, clusterService);

        Map<String, ADStat<?>> statsMap = new HashMap<String, ADStat<?>>() {
            {
                put(StatNames.AD_EXECUTE_REQUEST_COUNT.getName(), new ADStat<>(false, new CounterSupplier()));
                put(StatNames.AD_EXECUTE_FAIL_COUNT.getName(), new ADStat<>(false, new CounterSupplier()));
            }
        };

        adStats = new ADStats(indexUtils, normalModelManager, statsMap);
    }

    public void setupTestNodes(Settings settings) {
        nodesCount = randomIntBetween(2, 10);
        testNodes = new FakeNode[nodesCount];
        for (int i = 0; i < testNodes.length; i++) {
            testNodes[i] = new FakeNode("node" + i, threadPool, settings);
        }
        runner = new ColdStartRunner();
    }

    @SuppressWarnings("unchecked")
    public void setUpSavingAnomalyResultIndex(boolean anomalyResultIndexExists) throws IOException {
        anomalyDetectionIndices = mock(AnomalyDetectionIndices.class);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length >= 1);

            ActionListener<CreateIndexResponse> listener = null;

            if (args[0] instanceof ActionListener) {
                listener = (ActionListener<CreateIndexResponse>) args[0];
            }

            assertTrue(listener != null);

            listener.onResponse(new CreateIndexResponse(true, true, AnomalyResult.ANOMALY_RESULT_INDEX) {
            });

            return null;
        }).when(anomalyDetectionIndices).initAnomalyResultIndex(any());

        when(anomalyDetectionIndices.doesAnomalyResultIndexExist()).thenReturn(anomalyResultIndexExists);
    }

    public void setupInitResultIndexException(Class<? extends Throwable> exceptionType) throws IOException {
        anomalyDetectionIndices = mock(AnomalyDetectionIndices.class);
        doThrow(exceptionType).when(anomalyDetectionIndices).initAnomalyResultIndex(any());

        when(anomalyDetectionIndices.doesAnomalyResultIndexExist()).thenReturn(false);
    }

    @Override
    @After
    public final void tearDown() throws Exception {
        for (FakeNode testNode : testNodes) {
            testNode.close();
        }
        runner.shutDown();
        runner = null;
        client = null;
        anomalyDetectionIndices = null;
        super.tearDownLog4jForJUnit();
        super.tearDown();
    }

    private Throwable assertException(PlainActionFuture<AnomalyResultResponse> listener, Class<? extends Exception> exceptionType) {
        return expectThrows(exceptionType, () -> listener.actionGet());
    }

    private void assertException(PlainActionFuture<AnomalyResultResponse> listener, Class<? extends Exception> exceptionType, String msg) {
        Exception e = expectThrows(exceptionType, () -> listener.actionGet());
        assertThat(e.getMessage(), containsString(msg));
    }

    public void testNormal() throws IOException {

        setUpSavingAnomalyResultIndex(false);
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
            client,
            settings,
            stateManager,
            runner,
            anomalyDetectionIndices,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            threadPool,
            adCircuitBreakerService,
            adStats
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet();
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

    public Throwable noModelExceptionTemplate(
        Exception thrownException,
        ColdStartRunner globalRunner,
        String adID,
        Class<? extends Exception> expectedExceptionType,
        String error
    ) {

        ModelManager rcfManager = mock(ModelManager.class);
        doThrow(thrownException).when(rcfManager).getRcfResult(any(String.class), any(String.class), any(double[].class));
        when(rcfManager.getRcfModelId(any(String.class), anyInt())).thenReturn(rcfModelID);

        doNothing().when(normalModelManager).trainModel(any(AnomalyDetector.class), any(double[][].class));
        when(featureQuery.getColdStartData(any(AnomalyDetector.class))).thenReturn(Optional.of(new double[][] { { 0 } }));

        // These constructors register handler in transport service
        new RCFResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, rcfManager, adCircuitBreakerService);
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            client,
            settings,
            stateManager,
            globalRunner,
            anomalyDetectionIndices,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            threadPool,
            adCircuitBreakerService,
            adStats
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        return assertException(listener, expectedExceptionType);
    }

    public void noModelExceptionTemplate(Exception exception, ColdStartRunner globalRunner, String adID, String error) {
        noModelExceptionTemplate(exception, globalRunner, adID, exception.getClass(), error);
    }

    public void testNormalColdStart() {
        noModelExceptionTemplate(
            new ResourceNotFoundException(adID, ""),
            runner,
            adID,
            AnomalyDetectionException.class,
            AnomalyResultTransportAction.NO_MODEL_ERR_MSG
        );
    }

    public void testNormalColdStartRemoteException() {
        noModelExceptionTemplate(
            new NotSerializableExceptionWrapper(new ResourceNotFoundException(adID, "")),
            runner,
            adID,
            AnomalyDetectionException.class,
            AnomalyResultTransportAction.NO_MODEL_ERR_MSG
        );
    }

    public void testNullPointerExceptionWhenRCF() {
        noModelExceptionTemplate(
            new NullPointerException(),
            runner,
            adID,
            AnomalyDetectionException.class,
            AnomalyResultTransportAction.NO_MODEL_ERR_MSG
        );
    }

    public void testADExceptionWhenColdStart() {
        String error = "blah";
        ColdStartRunner mockRunner = mock(ColdStartRunner.class);
        when(mockRunner.fetchException(any(String.class))).thenReturn(Optional.of(new AnomalyDetectionException(adID, error)));

        noModelExceptionTemplate(new AnomalyDetectionException(adID, ""), mockRunner, adID, error);
    }

    public void testInsufficientCapacityExceptionDuringColdStart() {

        ModelManager rcfManager = mock(ModelManager.class);
        doThrow(ResourceNotFoundException.class).when(rcfManager).getRcfResult(any(String.class), any(String.class), any(double[].class));
        when(rcfManager.getRcfModelId(any(String.class), anyInt())).thenReturn(rcfModelID);

        ColdStartRunner mockRunner = mock(ColdStartRunner.class);
        when(mockRunner.fetchException(any(String.class)))
            .thenReturn(Optional.of(new LimitExceededException(adID, CommonErrorMessages.MEMORY_LIMIT_EXCEEDED_ERR_MSG)));

        // These constructors register handler in transport service
        new RCFResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, rcfManager, adCircuitBreakerService);
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            client,
            settings,
            stateManager,
            mockRunner,
            anomalyDetectionIndices,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            threadPool,
            adCircuitBreakerService,
            adStats
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        assertException(listener, LimitExceededException.class);
    }

    public void testInsufficientCapacityExceptionDuringRestoringModel() {

        ModelManager rcfManager = mock(ModelManager.class);
        doThrow(new NotSerializableExceptionWrapper(new LimitExceededException(adID, CommonErrorMessages.MEMORY_LIMIT_EXCEEDED_ERR_MSG)))
            .when(rcfManager)
            .getRcfResult(any(String.class), any(String.class), any(double[].class));

        // These constructors register handler in transport service
        new RCFResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, rcfManager, adCircuitBreakerService);
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            client,
            settings,
            stateManager,
            runner,
            anomalyDetectionIndices,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            threadPool,
            adCircuitBreakerService,
            adStats
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        assertException(listener, LimitExceededException.class);
    }

    public void testThresholdException() {

        ModelManager exceptionThreadholdfManager = mock(ModelManager.class);
        doThrow(NullPointerException.class)
            .when(exceptionThreadholdfManager)
            .getThresholdingResult(any(String.class), any(String.class), anyDouble());

        // These constructors register handler in transport service
        new RCFResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            normalModelManager,
            adCircuitBreakerService
        );
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, exceptionThreadholdfManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            client,
            settings,
            stateManager,
            runner,
            anomalyDetectionIndices,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            threadPool,
            adCircuitBreakerService,
            adStats
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        assertException(listener, AnomalyDetectionException.class);
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
            client,
            settings,
            stateManager,
            runner,
            anomalyDetectionIndices,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            threadPool,
            breakerService,
            adStats
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
            client,
            settings,
            stateManager,
            runner,
            anomalyDetectionIndices,
            featureQuery,
            normalModelManager,
            hashRing,
            hackedClusterService,
            indexNameResolver,
            threadPool,
            adCircuitBreakerService,
            adStats
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

    public void testMute() {
        ADStateManager muteStateManager = mock(ADStateManager.class);
        when(muteStateManager.isMuted(any(String.class))).thenReturn(true);
        when(muteStateManager.getAnomalyDetector(any(String.class))).thenReturn(Optional.of(detector));
        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            client,
            settings,
            muteStateManager,
            runner,
            anomalyDetectionIndices,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            threadPool,
            adCircuitBreakerService,
            adStats
        );
        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        Throwable exception = assertException(listener, AnomalyDetectionException.class);
        assertThat(exception.getMessage(), containsString(AnomalyResultTransportAction.NODE_UNRESPONSIVE_ERR_MSG));
    }

    public void testRCFLatchAwaitException() throws InterruptedException {

        // These constructors register handler in transport service
        new RCFResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            normalModelManager,
            adCircuitBreakerService
        );
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        AnomalyResultTransportAction action = spy(
            new AnomalyResultTransportAction(
                new ActionFilters(Collections.emptySet()),
                transportService,
                client,
                settings,
                stateManager,
                runner,
                anomalyDetectionIndices,
                featureQuery,
                normalModelManager,
                hashRing,
                clusterService,
                indexNameResolver,
                threadPool,
                adCircuitBreakerService,
                adStats
            )
        );

        CountDownLatch latch = mock(CountDownLatch.class);
        doThrow(InterruptedException.class).when(latch).await(anyLong(), any(TimeUnit.class));
        when(action.createCountDownLatch(anyInt())).thenReturn(latch);

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        Throwable nestedCause = assertException(listener, AnomalyDetectionException.class);
        assertEquals(CommonErrorMessages.WAIT_ERR_MSG, nestedCause.getMessage());
    }

    public void testThresholdLatchAwaitException() throws InterruptedException {
        // These constructors register handler in transport service
        new RCFResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            normalModelManager,
            adCircuitBreakerService
        );
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        AnomalyResultTransportAction action = spy(
            new AnomalyResultTransportAction(
                new ActionFilters(Collections.emptySet()),
                transportService,
                client,
                settings,
                stateManager,
                new ColdStartRunner(),
                anomalyDetectionIndices,
                featureQuery,
                normalModelManager,
                hashRing,
                clusterService,
                indexNameResolver,
                threadPool,
                adCircuitBreakerService,
                adStats
            )
        );

        CountDownLatch latch = mock(CountDownLatch.class);
        doThrow(InterruptedException.class).when(latch).await(anyLong(), any(TimeUnit.class));
        when(action.createCountDownLatch(eq(1))).thenReturn(latch);

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        Throwable nestedCause = assertException(listener, AnomalyDetectionException.class);
        assertEquals(AnomalyResultTransportAction.WAIT_FOR_THRESHOLD_ERR_MSG, nestedCause.getMessage());
    }

    public void alertingRequestTemplate(boolean anomalyResultIndexExists) throws IOException {
        setUpSavingAnomalyResultIndex(anomalyResultIndexExists);
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
            client,
            settings,
            stateManager,
            runner,
            anomalyDetectionIndices,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            threadPool,
            adCircuitBreakerService,
            adStats
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
            client,
            settings,
            stateManager,
            new ColdStartRunner(),
            anomalyDetectionIndices,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            threadPool,
            adCircuitBreakerService,
            adStats
        );
        AnomalyResultTransportAction.RCFActionListener listener = action.new RCFActionListener(null, null, null, null);
        listener.onFailure(null);
    }

    public void testColdStartNoTrainingData() throws Exception {
        when(featureQuery.getColdStartData(any(AnomalyDetector.class))).thenReturn(Optional.empty());

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            client,
            settings,
            stateManager,
            runner,
            anomalyDetectionIndices,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            threadPool,
            adCircuitBreakerService,
            adStats
        );

        AnomalyResultTransportAction.ColdStartJob job = action.new ColdStartJob(detector);
        expectThrows(AnomalyDetectionException.class, () -> job.call());
    }

    public void testColdStartTimeoutPutCheckpoint() throws Exception {
        when(featureQuery.getColdStartData(any(AnomalyDetector.class))).thenReturn(Optional.of(new double[][] { { 1.0 } }));
        doThrow(new ElasticsearchTimeoutException(""))
            .when(normalModelManager)
            .trainModel(any(AnomalyDetector.class), any(double[][].class));

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            client,
            settings,
            stateManager,
            runner,
            anomalyDetectionIndices,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            threadPool,
            adCircuitBreakerService,
            adStats
        );

        AnomalyResultTransportAction.ColdStartJob job = action.new ColdStartJob(detector);
        expectThrows(ClientException.class, () -> job.call());
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
    public void savingFailureTemplate(boolean throwEsRejectedExecutionException, int latchCount) throws InterruptedException, IOException {
        setUpSavingAnomalyResultIndex(false);

        final CountDownLatch backoffLatch = new CountDownLatch(latchCount);

        Client badClient = mock(Client.class);
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
            if (throwEsRejectedExecutionException) {
                listener.onFailure(new EsRejectedExecutionException(""));
            } else {
                listener.onFailure(new IllegalArgumentException());
            }

            backoffLatch.countDown();
            return null;
        }).when(badClient).index(any(), any());

        Settings backoffSettings = Settings
            .builder()
            .put("opendistro.anomaly_detection.max_retry_for_backoff", 2)
            .put("opendistro.anomaly_detection.backoff_initial_delay", TimeValue.timeValueMillis(1))
            .build();

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
            badClient,
            backoffSettings,
            stateManager,
            runner,
            anomalyDetectionIndices,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            threadPool,
            adCircuitBreakerService,
            adStats
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        backoffLatch.await();
    }

    public void testSavingFailureNotRetry() throws InterruptedException, IOException {
        savingFailureTemplate(false, 1);

        assertEquals(1, testAppender.countMessage((AnomalyResultTransportAction.FAIL_TO_SAVE_ERR_MSG)));
        assertTrue(!testAppender.containsMessage(AnomalyResultTransportAction.SUCCESS_SAVING_MSG));
        assertTrue(!testAppender.containsMessage(AnomalyResultTransportAction.RETRY_SAVING_ERR_MSG));
    }

    public void testSavingFailureRetry() throws InterruptedException, IOException {
        savingFailureTemplate(true, 3);

        assertEquals(2, testAppender.countMessage((AnomalyResultTransportAction.RETRY_SAVING_ERR_MSG)));
        assertEquals(1, testAppender.countMessage((AnomalyResultTransportAction.FAIL_TO_SAVE_ERR_MSG)));
        assertTrue(!testAppender.containsMessage(AnomalyResultTransportAction.SUCCESS_SAVING_MSG));
    }

    enum FeatureTestMode {
        FEATURE_NOT_AVAILABLE,
        ILLEGAL_STATE,
        AD_EXCEPTION
    }

    public void featureTestTemplate(FeatureTestMode mode) {
        if (mode == FeatureTestMode.FEATURE_NOT_AVAILABLE) {
            when(featureQuery.getCurrentFeatures(any(AnomalyDetector.class), anyLong(), anyLong()))
                .thenReturn(new SinglePointFeatures(Optional.empty(), Optional.empty()));
        } else if (mode == FeatureTestMode.ILLEGAL_STATE) {
            doThrow(IllegalArgumentException.class).when(featureQuery).getCurrentFeatures(any(AnomalyDetector.class), anyLong(), anyLong());
        } else if (mode == FeatureTestMode.AD_EXCEPTION) {
            doThrow(AnomalyDetectionException.class)
                .when(featureQuery)
                .getCurrentFeatures(any(AnomalyDetector.class), anyLong(), anyLong());
        }

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            client,
            settings,
            stateManager,
            runner,
            anomalyDetectionIndices,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            threadPool,
            adCircuitBreakerService,
            adStats
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        if (mode == FeatureTestMode.FEATURE_NOT_AVAILABLE) {
            AnomalyResultResponse response = listener.actionGet();
            assertEquals(Double.NaN, response.getAnomalyGrade(), 0.001);
            assertEquals(Double.NaN, response.getConfidence(), 0.001);
            assertThat(response.getFeatures(), is(empty()));
        } else if (mode == FeatureTestMode.ILLEGAL_STATE || mode == FeatureTestMode.AD_EXCEPTION) {
            assertException(listener, InternalFailure.class);
        }
    }

    public void testFeatureNotAvailable() {
        featureTestTemplate(FeatureTestMode.FEATURE_NOT_AVAILABLE);
    }

    public void testFeatureIllegalState() {
        featureTestTemplate(FeatureTestMode.ILLEGAL_STATE);
    }

    public void testFeatureAnomalyException() {
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
                    .blocks(ClusterBlocks.builder().addGlobalBlock(IndexMetaData.INDEX_WRITE_BLOCK))
                    .build();
                break;
            case GLOBAL_BLOCK_READ:
                blockedClusterState = ClusterState
                    .builder(new ClusterName("test cluster"))
                    .blocks(ClusterBlocks.builder().addGlobalBlock(IndexMetaData.INDEX_READ_BLOCK))
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
            client,
            settings,
            stateManager,
            runner,
            anomalyDetectionIndices,
            featureQuery,
            normalModelManager,
            hashRing,
            hackedClusterService,
            indexNameResolver,
            threadPool,
            adCircuitBreakerService,
            adStats
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        assertException(listener, AnomalyDetectionException.class, errLogMsg);
    }

    private ClusterState createIndexBlockedState(String indexName, Settings hackedSettings, String alias) {
        ClusterState blockedClusterState = null;
        IndexMetaData.Builder builder = IndexMetaData.builder(indexName);
        if (alias != null) {
            builder.putAlias(AliasMetaData.builder(alias));
        }
        IndexMetaData indexMetaData = builder
            .settings(
                Settings
                    .builder()
                    .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                    .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(hackedSettings)
            )
            .build();
        MetaData metaData = MetaData.builder().put(indexMetaData, false).build();
        blockedClusterState = ClusterState
            .builder(new ClusterName("test cluster"))
            .metaData(metaData)
            .blocks(ClusterBlocks.builder().addBlocks(indexMetaData))
            .build();
        return blockedClusterState;
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
            Settings.builder().put(IndexMetaData.INDEX_BLOCKS_READ_SETTING.getKey(), true).build(),
            "test1"
        );
    }

    public void testIndexWriteBlock() {

        ClusterState blockedClusterState = createIndexBlockedState(
            UUIDs.randomBase64UUID(),
            Settings.builder().put(IndexMetaData.INDEX_BLOCKS_WRITE_SETTING.getKey(), true).build(),
            AnomalyResult.ANOMALY_RESULT_INDEX
        );
        ClusterService hackedClusterService = spy(clusterService);
        when(hackedClusterService.state()).thenReturn(blockedClusterState);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            client,
            settings,
            stateManager,
            runner,
            anomalyDetectionIndices,
            featureQuery,
            normalModelManager,
            hashRing,
            hackedClusterService,
            indexNameResolver,
            threadPool,
            adCircuitBreakerService,
            adStats
        );
        action.indexAnomalyResult(TestHelpers.randomAnomalyDetectResult());

        assertTrue(testAppender.containsMessage(AnomalyResultTransportAction.CANNOT_SAVE_ERR_MSG));
    }

    public void testNullRCFResult() {
        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            client,
            settings,
            stateManager,
            runner,
            anomalyDetectionIndices,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            threadPool,
            adCircuitBreakerService,
            adStats
        );
        AnomalyResultTransportAction.RCFActionListener listener = action.new RCFActionListener(null, "123-rcf-0", null, "123");
        listener.onResponse(null);
        assertTrue(testAppender.containsMessage(AnomalyResultTransportAction.NULL_RESPONSE));
    }
}
