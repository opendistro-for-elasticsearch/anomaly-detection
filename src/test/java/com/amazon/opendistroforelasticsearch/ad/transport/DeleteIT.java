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

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;

@ESIntegTestCase.ClusterScope(transportClientRatio = 0.9)
public class DeleteIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(AnomalyDetectorPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singletonList(AnomalyDetectorPlugin.class);
    }

    public void testNormalDeleteDetector() throws ExecutionException, InterruptedException {
        DeleteDetectorRequest request = new DeleteDetectorRequest().adID("123");

        ActionFuture<AcknowledgedResponse> future = client().execute(DeleteDetectorAction.INSTANCE, request);

        AcknowledgedResponse response = future.get();
        assertTrue(response.isAcknowledged());
    }

    public void testNormalDeleteModel() throws ExecutionException, InterruptedException {
        DeleteModelRequest request = new DeleteModelRequest("123");

        ActionFuture<DeleteModelResponse> future = client().execute(DeleteModelAction.INSTANCE, request);

        DeleteModelResponse response = future.get();
        assertTrue(!response.hasFailures());
    }

    public void testEmptyIDDeleteModel() throws ExecutionException, InterruptedException {
        DeleteModelRequest request = new DeleteModelRequest("");

        ActionFuture<DeleteModelResponse> future = client().execute(DeleteModelAction.INSTANCE, request);

        expectThrows(ActionRequestValidationException.class, () -> future.actionGet());
    }

    public void testEmptyIDDeleteDetector() throws ExecutionException, InterruptedException {
        DeleteDetectorRequest request = new DeleteDetectorRequest();

        ActionFuture<AcknowledgedResponse> future = client().execute(DeleteDetectorAction.INSTANCE, request);

        expectThrows(ActionRequestValidationException.class, () -> future.actionGet());
    }
}
