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

import static org.mockito.ArgumentMatchers.any;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.transport.CronAction;
import com.amazon.opendistroforelasticsearch.ad.transport.CronNodeResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.CronResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;

import test.com.amazon.opendistroforelasticsearch.ad.util.ClusterCreation;

public class HourlyCronTests extends AbstractADTest {

    enum HourlyCronTestExecutionMode {
        NORMAL,
        NODE_FAIL,
        ALL_FAIL
    }

    @SuppressWarnings("unchecked")
    public void templateHourlyCron(HourlyCronTestExecutionMode mode) {
        super.setUpLog4jForJUnit(HourlyCron.class);

        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = ClusterCreation.state(1);
        when(clusterService.state()).thenReturn(state);

        Client client = mock(Client.class);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length == 3);
            assertTrue(args[2] instanceof ActionListener);

            ActionListener<CronResponse> listener = (ActionListener<CronResponse>) args[2];

            if (mode == HourlyCronTestExecutionMode.NODE_FAIL) {
                listener
                    .onResponse(
                        new CronResponse(
                            new ClusterName("test"),
                            Collections.singletonList(new CronNodeResponse()),
                            Collections.singletonList(new FailedNodeException("foo0", "blah", new ElasticsearchException("bar")))
                        )
                    );
            } else if (mode == HourlyCronTestExecutionMode.ALL_FAIL) {
                listener.onFailure(new ElasticsearchException("bar"));
            } else {
                CronNodeResponse nodeResponse = new CronNodeResponse(state.nodes().getLocalNode());
                BytesStreamOutput nodeResponseOut = new BytesStreamOutput();
                nodeResponseOut.setVersion(Version.V_7_1_1);
                nodeResponse.writeTo(nodeResponseOut);
                StreamInput siNode = nodeResponseOut.bytes().streamInput();

                CronNodeResponse nodeResponseRead = new CronNodeResponse();
                nodeResponseRead.readFrom(siNode);

                CronResponse response = new CronResponse(
                    new ClusterName("test"),
                    Collections.singletonList(nodeResponseRead),
                    Collections.EMPTY_LIST
                );
                BytesStreamOutput out = new BytesStreamOutput();
                out.setVersion(Version.V_7_1_1);
                response.writeTo(out);
                StreamInput si = out.bytes().streamInput();
                CronResponse responseRead = CronAction.INSTANCE.newResponse();
                responseRead.readFrom(si);
                listener.onResponse(responseRead);
            }

            return null;
        }).when(client).execute(eq(CronAction.INSTANCE), any(), any());

        HourlyCron cron = new HourlyCron(clusterService, client);
        cron.run();

        Logger LOG = LogManager.getLogger(HourlyCron.class);
        LOG.info(testAppender.messages);
        if (mode == HourlyCronTestExecutionMode.NODE_FAIL) {
            assertTrue(testAppender.containsMessage(HourlyCron.NODE_EXCEPTION_LOG_MSG));
        } else if (mode == HourlyCronTestExecutionMode.ALL_FAIL) {
            assertTrue(testAppender.containsMessage(HourlyCron.EXCEPTION_LOG_MSG));
        } else {
            assertTrue(testAppender.containsMessage(HourlyCron.SUCCEEDS_LOG_MSG));
        }

        super.tearDownLog4jForJUnit();
    }

    public void testNormal() {
        templateHourlyCron(HourlyCronTestExecutionMode.NORMAL);
    }

    public void testAllFail() {
        templateHourlyCron(HourlyCronTestExecutionMode.ALL_FAIL);
    }

    public void testNodeFail() {
        templateHourlyCron(HourlyCronTestExecutionMode.NODE_FAIL);
    }
}
