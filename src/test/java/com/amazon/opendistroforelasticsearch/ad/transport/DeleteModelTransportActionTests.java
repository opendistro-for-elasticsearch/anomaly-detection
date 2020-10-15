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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;

import test.com.amazon.opendistroforelasticsearch.ad.util.JsonDeserializer;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.NodeStateManager;
import com.amazon.opendistroforelasticsearch.ad.caching.CacheProvider;
import com.amazon.opendistroforelasticsearch.ad.common.exception.JsonPathNotFoundException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.feature.FeatureManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.google.gson.JsonElement;

public class DeleteModelTransportActionTests extends AbstractADTest {
    private DeleteModelTransportAction action;
    private String localNodeID;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        ThreadPool threadPool = mock(ThreadPool.class);

        ClusterService clusterService = mock(ClusterService.class);
        localNodeID = "foo";
        when(clusterService.localNode()).thenReturn(new DiscoveryNode(localNodeID, buildNewFakeTransportAddress(), Version.CURRENT));
        when(clusterService.getClusterName()).thenReturn(new ClusterName("test"));

        TransportService transportService = mock(TransportService.class);
        ActionFilters actionFilters = mock(ActionFilters.class);
        NodeStateManager tarnsportStatemanager = mock(NodeStateManager.class);
        ModelManager modelManager = mock(ModelManager.class);
        FeatureManager featureManager = mock(FeatureManager.class);
        CacheProvider cacheProvider = mock(CacheProvider.class);

        action = new DeleteModelTransportAction(
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            tarnsportStatemanager,
            modelManager,
            featureManager,
            cacheProvider
        );
    }

    public void testNormal() throws IOException, JsonPathNotFoundException {
        DeleteModelRequest request = new DeleteModelRequest("123");
        assertThat(request.validate(), is(nullValue()));

        DeleteModelNodeRequest nodeRequest = new DeleteModelNodeRequest(request);
        BytesStreamOutput nodeRequestOut = new BytesStreamOutput();
        nodeRequestOut.setVersion(Version.CURRENT);
        nodeRequest.writeTo(nodeRequestOut);
        StreamInput siNode = nodeRequestOut.bytes().streamInput();

        DeleteModelNodeRequest nodeResponseRead = new DeleteModelNodeRequest(siNode);

        DeleteModelNodeResponse nodeResponse1 = action.nodeOperation(nodeResponseRead);
        DeleteModelNodeResponse nodeResponse2 = action.nodeOperation(new DeleteModelNodeRequest(request));

        DeleteModelResponse response = action.newResponse(request, Arrays.asList(nodeResponse1, nodeResponse2), Collections.emptyList());

        assertEquals(2, response.getNodes().size());
        assertTrue(!response.hasFailures());

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String json = Strings.toString(builder);
        Function<JsonElement, String> function = (s) -> {
            try {
                return JsonDeserializer.getTextValue(s, CronNodeResponse.NODE_ID);
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
            return null;
        };
        assertArrayEquals(
            JsonDeserializer.getArrayValue(json, function, CronResponse.NODES_JSON_KEY),
            new String[] { localNodeID, localNodeID }
        );
    }

    public void testEmptyDetectorID() {
        ActionRequestValidationException e = new DeleteModelRequest().validate();
        assertThat(e.validationErrors(), Matchers.hasItem(CommonErrorMessages.AD_ID_MISSING_MSG));
    }
}
