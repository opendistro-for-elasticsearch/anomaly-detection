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

package com.amazon.opendistroforelasticsearch.ad.cluster;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Matchers.any;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNode.Role;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.Version;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class ADClusterEventListenerTests extends AbstractADTest {
    private final String masterNodeId = "masterNode";
    private final String dataNode1Id = "dataNode1";
    private final String clusterName = "multi-node-cluster";

    private ClusterService clusterService;
    private ADClusterEventListener listener;
    private HashRing hashRing;
    private ModelManager modelManager;
    private ClusterState oldClusterState;
    private ClusterState newClusterState;
    private DiscoveryNode masterNode;
    private DiscoveryNode dataNode1;

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(ADClusterEventListenerTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        super.setUpLog4jForJUnit(ADClusterEventListener.class);
        clusterService = createClusterService(threadPool);
        hashRing = mock(HashRing.class);
        when(hashRing.build()).thenReturn(true);
        modelManager = mock(ModelManager.class);
        masterNode = new DiscoveryNode(masterNodeId, buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        dataNode1 = new DiscoveryNode(dataNode1Id, buildNewFakeTransportAddress(), emptyMap(), EnumSet.allOf(Role.class), Version.CURRENT);
        oldClusterState = ClusterState
            .builder(new ClusterName(clusterName))
            .nodes(new DiscoveryNodes.Builder().masterNodeId(masterNodeId).localNodeId(masterNodeId).add(masterNode))
            .build();
        newClusterState = ClusterState
            .builder(new ClusterName(clusterName))
            .nodes(new DiscoveryNodes.Builder().masterNodeId(masterNodeId).localNodeId(dataNode1Id).add(masterNode).add(dataNode1))
            .build();

        listener = new ADClusterEventListener(clusterService, hashRing, modelManager);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        super.tearDownLog4jForJUnit();
        clusterService = null;
        hashRing = null;
        modelManager = null;
        oldClusterState = null;
        listener = null;
    }

    public void testIsMasterNode() {
        listener.clusterChanged(new ClusterChangedEvent("foo", oldClusterState, oldClusterState));
        assertTrue(testAppender.containsMessage(ADClusterEventListener.MASTER_NOT_APPLIED_MSG));
    }

    public void testNotRecovered() {
        ClusterState blockedClusterState = ClusterState
            .builder(new ClusterName(clusterName))
            .nodes(new DiscoveryNodes.Builder().masterNodeId(masterNodeId).localNodeId(dataNode1Id).add(masterNode).add(dataNode1))
            .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK))
            .build();
        listener.clusterChanged(new ClusterChangedEvent("foo", blockedClusterState, oldClusterState));
        assertTrue(testAppender.containsMessage(ADClusterEventListener.NOT_RECOVERED_MSG));
    }

    class ListenerRunnable implements Runnable {

        @Override
        public void run() {
            listener.clusterChanged(new ClusterChangedEvent("foo", newClusterState, oldClusterState));
        }
    }

    public void testInprogress() throws InterruptedException {
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        final CountDownLatch executionLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            executionLatch.countDown();
            inProgressLatch.await();
            return emptySet();
        }).when(modelManager).getAllModelIds();
        new Thread(new ListenerRunnable()).start();
        executionLatch.await();
        listener.clusterChanged(new ClusterChangedEvent("bar", newClusterState, oldClusterState));
        assertTrue(testAppender.containsMessage(ADClusterEventListener.IN_PROGRESS_MSG));
        inProgressLatch.countDown();
    }

    public void testNodeAdded() {
        String modelId = "123-threshold";
        doAnswer(invocation -> {
            Set<String> res = new HashSet<>();
            res.add(modelId);
            return res;
        }).when(modelManager).getAllModelIds();

        doAnswer(invocation -> { return Optional.<DiscoveryNode>of(masterNode); }).when(hashRing).getOwningNode(any(String.class));

        listener.clusterChanged(new ClusterChangedEvent("foo", newClusterState, oldClusterState));
        assertTrue(testAppender.containsMessage(ADClusterEventListener.NODE_ADDED_MSG));
        assertTrue(testAppender.containsMessage(ADClusterEventListener.REMOVE_MODEL_MSG + " " + modelId));
    }

    public void testNodeRemoved() {
        ClusterState twoDataNodeClusterState = ClusterState
            .builder(new ClusterName(clusterName))
            .nodes(
                new DiscoveryNodes.Builder()
                    .masterNodeId(masterNodeId)
                    .localNodeId(dataNode1Id)
                    .add(new DiscoveryNode(masterNodeId, buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT))
                    .add(dataNode1)
                    .add(
                        new DiscoveryNode(
                            "dataNode2",
                            buildNewFakeTransportAddress(),
                            emptyMap(),
                            EnumSet.allOf(Role.class),
                            Version.CURRENT
                        )
                    )
            )
            .build();

        listener.clusterChanged(new ClusterChangedEvent("foo", newClusterState, twoDataNodeClusterState));
        assertTrue(!testAppender.containsMessage(ADClusterEventListener.NODE_ADDED_MSG));
        assertTrue(testAppender.containsMessage(ADClusterEventListener.NODE_REMOVED_MSG));
    }
}
