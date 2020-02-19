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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.reindex.BulkByScrollResponse;

public class DailyCronTests extends AbstractADTest {

    enum DailyCronTestExecutionMode {
        NORMAL,
        INDEX_NOT_EXIST,
        FAIL
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        super.setUpLog4jForJUnit(DailyCron.class);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        super.tearDownLog4jForJUnit();
    }

    @SuppressWarnings("unchecked")
    private void templateDailyCron(DailyCronTestExecutionMode mode) {
        DeleteDetector deleteUtil = mock(DeleteDetector.class);
        Clock clock = mock(Clock.class);
        Client client = mock(Client.class);
        ClientUtil clientUtil = mock(ClientUtil.class);
        CancelQueryUtil cancelQueryUtil = mock(CancelQueryUtil.class);
        DailyCron cron = new DailyCron(deleteUtil, clock, client, Duration.ofHours(24), clientUtil, cancelQueryUtil);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length == 3);
            assertTrue(args[2] instanceof ActionListener);

            ActionListener<BulkByScrollResponse> listener = (ActionListener<BulkByScrollResponse>) args[2];

            if (mode == DailyCronTestExecutionMode.INDEX_NOT_EXIST) {
                listener.onFailure(new IndexNotFoundException("foo", "bar"));
            } else if (mode == DailyCronTestExecutionMode.FAIL) {
                listener.onFailure(new ElasticsearchException("bar"));
            } else {
                BulkByScrollResponse deleteByQueryResponse = mock(BulkByScrollResponse.class);
                when(deleteByQueryResponse.getDeleted()).thenReturn(10L);
                listener.onResponse(deleteByQueryResponse);
            }

            return null;
        }).when(clientUtil).execute(eq(DeleteByQueryAction.INSTANCE), any(), any());

        // those tests are covered by each util class
        doNothing().when(deleteUtil).deleteDetectorResult(eq(client));
        doNothing().when(cancelQueryUtil).cancelQuery(eq(client));

        cron.run();
    }

    public void testNormal() {
        templateDailyCron(DailyCronTestExecutionMode.NORMAL);
        assertTrue(testAppender.containsMessage(DailyCron.CHECKPOINT_DELETED_MSG));
    }

    public void testCheckpointNotExist() {
        templateDailyCron(DailyCronTestExecutionMode.INDEX_NOT_EXIST);
        assertTrue(testAppender.containsMessage(DailyCron.CHECKPOINT_NOT_EXIST_MSG));
    }

    public void testFail() {
        templateDailyCron(DailyCronTestExecutionMode.FAIL);
        assertTrue(testAppender.containsMessage(DailyCron.CANNOT_DELETE_OLD_CHECKPOINT_MSG));
    }
}
