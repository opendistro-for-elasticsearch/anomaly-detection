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

import static java.util.Collections.emptyMap;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.BUILT_IN_ROLES;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.time.Clock;
import java.util.ArrayList;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class HashRingTests extends AbstractADTest {

    private ClusterService clusterService;
    private Settings settings;
    private Clock clock;

    private DiscoveryNode createNode(String nodeId) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), emptyMap(), BUILT_IN_ROLES, Version.CURRENT);
    }

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(HashRingTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        super.setUpLog4jForJUnit(HashRing.class);
        clusterService = createClusterService(threadPool);
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        List<DiscoveryNode> discoveryNodes = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            final DiscoveryNode node = createNode(Integer.toString(i));
            discoBuilder = discoBuilder.add(node);
            discoveryNodes.add(node);
        }
        discoBuilder.localNodeId(randomFrom(discoveryNodes).getId());
        discoBuilder.masterNodeId(randomFrom(discoveryNodes).getId());
        ClusterState.Builder stateBuilder = ClusterState.builder(clusterService.getClusterName());
        stateBuilder.nodes(discoBuilder);
        ClusterState clusterState = stateBuilder.build();
        setState(clusterService.getClusterApplierService(), clusterState);

        settings = Settings
            .builder()
            .put("opendistro.anomaly_detection.cluster_state_change_cooldown_minutes", TimeValue.timeValueMinutes(5))
            .build();
        clock = mock(Clock.class);
        when(clock.millis()).thenReturn(700000L);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        super.tearDownLog4jForJUnit();
        clusterService.close();
    }

    public void testGetOwningNode() {
        HashRing ring = new HashRing(clusterService, clock, settings);
        Optional<DiscoveryNode> node = ring.getOwningNode("http-latency-rcf-1");
        assertTrue(node.isPresent());
        String id = node.get().getId();
        assertTrue(id.equals("1") || id.equals("2"));

        when(clock.millis()).thenReturn(700001L);
        ring.recordMembershipChange();
        Optional<DiscoveryNode> node2 = ring.getOwningNode("http-latency-rcf-1");
        assertEquals(node, node2);
        assertTrue(testAppender.containsMessage(HashRing.COOLDOWN_MSG));
    }
}
