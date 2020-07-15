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

package com.amazon.opendistroforelasticsearch.ad.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;

import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfile;

public class MultiResponsesDelegateActionListenerTests extends ESTestCase {

    public void testEmptyResponse() throws InterruptedException {
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        ActionListener<DetectorProfile> actualListener = ActionListener.wrap(response -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }, exception -> {
            String exceptionMsg = exception.getMessage();
            assertTrue(exceptionMsg, exceptionMsg.contains(MultiResponsesDelegateActionListener.NO_RESPONSE));
            inProgressLatch.countDown();
        });

        MultiResponsesDelegateActionListener<DetectorProfile> multiListener = new MultiResponsesDelegateActionListener<DetectorProfile>(
            actualListener,
            2,
            "blah"
        );
        multiListener.onResponse(null);
        multiListener.onResponse(null);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }
}
