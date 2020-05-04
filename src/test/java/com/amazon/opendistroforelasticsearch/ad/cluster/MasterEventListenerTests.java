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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;

import java.time.Clock;
import java.util.Arrays;
import java.util.HashMap;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

public class MasterEventListenerTests extends AbstractADTest {
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private DeleteDetector deleteUtil;
    private Client client;
    private Clock clock;
    private Cancellable hourlyCancellable;
    private Cancellable dailyCancellable;
    private MasterEventListener masterService;
    private ClientUtil clientUtil;
    private DiscoveryNodeFilterer nodeFilter;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        threadPool = mock(ThreadPool.class);
        hourlyCancellable = mock(Cancellable.class);
        dailyCancellable = mock(Cancellable.class);
        when(threadPool.scheduleWithFixedDelay(any(HourlyCron.class), any(TimeValue.class), any(String.class)))
            .thenReturn(hourlyCancellable);
        when(threadPool.scheduleWithFixedDelay(any(DailyCron.class), any(TimeValue.class), any(String.class))).thenReturn(dailyCancellable);
        deleteUtil = mock(DeleteDetector.class);
        client = mock(Client.class);
        clock = mock(Clock.class);
        clientUtil = mock(ClientUtil.class);
        HashMap<String, String> ignoredAttributes = new HashMap<String, String>();
        ignoredAttributes.put(CommonName.BOX_TYPE_KEY, CommonName.WARM_BOX_TYPE);
        nodeFilter = new DiscoveryNodeFilterer(clusterService);

        masterService = new MasterEventListener(clusterService, threadPool, deleteUtil, client, clock, clientUtil, nodeFilter);
    }

    public void testOnOffMaster() {
        masterService.onMaster();
        assertThat(hourlyCancellable, is(notNullValue()));
        assertThat(dailyCancellable, is(notNullValue()));
        assertTrue(!masterService.getHourlyCron().isCancelled());
        assertTrue(!masterService.getDailyCron().isCancelled());
        masterService.offMaster();
        assertThat(masterService.getDailyCron(), is(nullValue()));
        assertThat(masterService.getHourlyCron(), is(nullValue()));
    }

    public void testBeforeStop() {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length == 1);

            LifecycleListener listener = null;
            if (args[0] instanceof LifecycleListener) {
                listener = (LifecycleListener) args[0];
            }

            assertTrue(listener != null);
            listener.beforeStop();

            return null;
        }).when(clusterService).addLifecycleListener(any());

        masterService.onMaster();
        assertThat(masterService.getDailyCron(), is(nullValue()));
        assertThat(masterService.getHourlyCron(), is(nullValue()));
        masterService.offMaster();
        assertThat(masterService.getDailyCron(), is(nullValue()));
        assertThat(masterService.getHourlyCron(), is(nullValue()));
    }
}
