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

package test.com.amazon.opendistroforelasticsearch.ad.util;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.tasks.MockTaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.nio.MockNioTransport;

public class FakeNode implements Releasable {
    public FakeNode(String name, ThreadPool threadPool, Settings settings, TransportInterceptor transportInterceptor) {
        final Function<BoundTransportAddress, DiscoveryNode> boundTransportAddressDiscoveryNodeFunction = address -> {
            discoveryNode.set(new DiscoveryNode(name, address.publishAddress(), emptyMap(), emptySet(), Version.CURRENT));
            return discoveryNode.get();
        };
        transportService = new TransportService(
            settings,
            new MockNioTransport(
                settings,
                Version.CURRENT,
                threadPool,
                new NetworkService(Collections.emptyList()),
                PageCacheRecycler.NON_RECYCLING_INSTANCE,
                new NamedWriteableRegistry(ClusterModule.getNamedWriteables()),
                new NoneCircuitBreakerService()
            ) {
                @Override
                public TransportAddress[] addressesFromString(String address) {
                    return new TransportAddress[] { dns.getOrDefault(address, ESTestCase.buildNewFakeTransportAddress()) };
                }
            },
            threadPool,
            transportInterceptor,
            boundTransportAddressDiscoveryNodeFunction,
            null,
            Collections.emptySet()
        ) {
            @Override
            protected TaskManager createTaskManager(Settings settings, ThreadPool threadPool, Set<String> taskHeaders) {
                if (MockTaskManager.USE_MOCK_TASK_MANAGER_SETTING.get(settings)) {
                    return new MockTaskManager(settings, threadPool, taskHeaders);
                } else {
                    return super.createTaskManager(settings, threadPool, taskHeaders);
                }
            }
        };

        transportService.start();
        clusterService = createClusterService(threadPool, discoveryNode.get());
        clusterService.addStateApplier(transportService.getTaskManager());
        ActionFilters actionFilters = new ActionFilters(emptySet());
        transportListTasksAction = new TransportListTasksAction(clusterService, transportService, actionFilters);
        transportCancelTasksAction = new TransportCancelTasksAction(clusterService, transportService, actionFilters);
        transportService.acceptIncomingRequests();
    }

    public FakeNode(String name, ThreadPool threadPool, Settings settings) {
        this(name, threadPool, settings, TransportService.NOOP_TRANSPORT_INTERCEPTOR);
    }

    public final ClusterService clusterService;
    public final TransportService transportService;
    private final SetOnce<DiscoveryNode> discoveryNode = new SetOnce<>();
    public final TransportListTasksAction transportListTasksAction;
    public final TransportCancelTasksAction transportCancelTasksAction;
    private final Map<String, TransportAddress> dns = new ConcurrentHashMap<>();

    @Override
    public void close() {
        clusterService.close();
        transportService.close();
    }

    public String getNodeId() {
        return discoveryNode().getId();
    }

    public DiscoveryNode discoveryNode() {
        return discoveryNode.get();
    }

    public static void connectNodes(FakeNode... nodes) {
        List<DiscoveryNode> discoveryNodes = new ArrayList<DiscoveryNode>(nodes.length);
        DiscoveryNode master = nodes[0].discoveryNode();
        for (int i = 0; i < nodes.length; i++) {
            discoveryNodes.add(nodes[i].discoveryNode());
        }

        for (FakeNode node : nodes) {
            setState(node.clusterService, ClusterCreation.state(new ClusterName("test"), node.discoveryNode(), master, discoveryNodes));
        }
        for (FakeNode nodeA : nodes) {
            for (FakeNode nodeB : nodes) {
                nodeA.transportService.connectToNode(nodeB.discoveryNode());
            }
        }
    }
}
