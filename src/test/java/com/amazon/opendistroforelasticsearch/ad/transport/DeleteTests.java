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

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.cluster.DeleteDetector;
import com.amazon.opendistroforelasticsearch.ad.common.exception.JsonPathNotFoundException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonMessageAttributes;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;
import org.junit.Before;

import test.com.amazon.opendistroforelasticsearch.ad.util.ClusterCreation;
import test.com.amazon.opendistroforelasticsearch.ad.util.JsonDeserializer;

public class DeleteTests extends AbstractADTest {
    private DeleteModelResponse response;
    private List<FailedNodeException> failures;
    private List<DeleteModelNodeResponse> deleteModelResponse;
    private String node1, node2, nodename1, nodename2;
    private Client client;
    private ClusterService clusterService;
    private TransportService transportService;
    private ThreadPool threadPool;
    private IndexNameExpressionResolver indexNameResolver;
    private ActionFilters actionFilters;
    private Task task;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        node1 = "node1";
        node2 = "node2";
        nodename1 = "nodename1";
        nodename2 = "nodename2";
        DiscoveryNode discoveryNode1 = new DiscoveryNode(
            nodename1,
            node1,
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );
        DiscoveryNode discoveryNode2 = new DiscoveryNode(
            nodename2,
            node2,
            new TransportAddress(TransportAddress.META_ADDRESS, 9301),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );
        List<DiscoveryNode> discoveryNodes = new ArrayList<DiscoveryNode>(2);
        discoveryNodes.add(discoveryNode1);
        discoveryNodes.add(discoveryNode2);

        DeleteModelNodeResponse nodeResponse1 = new DeleteModelNodeResponse(discoveryNode1);
        DeleteModelNodeResponse nodeResponse2 = new DeleteModelNodeResponse(discoveryNode2);

        deleteModelResponse = new ArrayList<>();

        deleteModelResponse.add(nodeResponse1);
        deleteModelResponse.add(nodeResponse2);

        failures = new ArrayList<>();
        failures.add(new FailedNodeException("node3", "blah", new ElasticsearchException("foo")));

        response = new DeleteModelResponse(new ClusterName("Cluster"), deleteModelResponse, failures);

        clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(discoveryNode1);
        when(clusterService.state())
            .thenReturn(ClusterCreation.state(new ClusterName("test"), discoveryNode2, discoveryNode1, discoveryNodes));

        transportService = mock(TransportService.class);
        threadPool = mock(ThreadPool.class);
        indexNameResolver = mock(IndexNameExpressionResolver.class);
        actionFilters = mock(ActionFilters.class);
        Settings settings = Settings.builder().put("opendistro.anomaly_detection.request_timeout", TimeValue.timeValueSeconds(10)).build();
        task = mock(Task.class);
        when(task.getId()).thenReturn(1000L);
        client = mock(Client.class);
        when(client.settings()).thenReturn(settings);
        when(client.threadPool()).thenReturn(threadPool);
    }

    public void testSerialzationResponse() throws IOException {

        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        DeleteModelResponse readResponse = DeleteModelAction.INSTANCE.getResponseReader().read(streamInput);
        assertTrue(readResponse.hasFailures());

        assertEquals(failures.size(), readResponse.failures().size());
        assertEquals(deleteModelResponse.size(), readResponse.getNodes().size());
    }

    public void testEmptyIDDeleteModel() {
        ActionRequestValidationException e = new DeleteModelRequest("").validate();
        assertThat(e.validationErrors(), Matchers.hasItem(CommonErrorMessages.AD_ID_MISSING_MSG));
    }

    public void testEmptyIDDeleteDetector() {
        ActionRequestValidationException e = new DeleteDetectorRequest().validate();
        assertThat(e.validationErrors(), hasItem(CommonErrorMessages.AD_ID_MISSING_MSG));
    }

    public void testValidIDDeleteDetector() {
        ActionRequestValidationException e = new DeleteDetectorRequest().adID("foo").validate();
        assertThat(e, is(nullValue()));
    }

    public void testSerialzationRequestDeleteModel() throws IOException {
        DeleteModelRequest request = new DeleteModelRequest("123");
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        StreamInput streamInput = output.bytes().streamInput();
        DeleteModelRequest readRequest = new DeleteModelRequest(streamInput);
        assertThat(request.getAdID(), equalTo(readRequest.getAdID()));
    }

    public void testSerialzationRequestDeleteDetector() throws IOException {
        DeleteDetectorRequest request = new DeleteDetectorRequest().adID("123");
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        StreamInput streamInput = output.bytes().streamInput();
        DeleteDetectorRequest readRequest = new DeleteDetectorRequest(streamInput);
        assertThat(request.getAdID(), equalTo(readRequest.getAdID()));
    }

    public <R extends ToXContent> void testJsonRequestTemplate(R request, Supplier<String> requestSupplier) throws IOException,
        JsonPathNotFoundException {
        XContentBuilder builder = jsonBuilder();
        request.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String json = Strings.toString(builder);
        assertEquals(JsonDeserializer.getTextValue(json, CommonMessageAttributes.ID_JSON_KEY), requestSupplier.get());
    }

    public void testJsonRequestDeleteDetector() throws IOException, JsonPathNotFoundException {
        DeleteDetectorRequest request = new DeleteDetectorRequest().adID("123");
        testJsonRequestTemplate(request, request::getAdID);
    }

    public void testJsonRequestDeleteModel() throws IOException, JsonPathNotFoundException {
        DeleteModelRequest request = new DeleteModelRequest("123");
        testJsonRequestTemplate(request, request::getAdID);
    }

    public void testNewResponse() throws IOException {
        StreamInput input = mock(StreamInput.class);
        when(input.readByte()).thenReturn((byte) 0x01);
        AcknowledgedResponse response = new AcknowledgedResponse(input);

        assertTrue(response.isAcknowledged());
    }

    private enum DetectorExecutionMode {
        DELETE_MODEL_NORMAL,
        DELETE_MODEL_FAILURE
    }

    @SuppressWarnings("unchecked")
    public void deleteDetectorResponseTemplate(DetectorExecutionMode mode) throws Exception {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length >= 3);
            assertTrue(args[2] instanceof ActionListener);

            ActionListener<DeleteModelResponse> listener = (ActionListener<DeleteModelResponse>) args[2];

            assertTrue(listener != null);
            if (mode == DetectorExecutionMode.DELETE_MODEL_FAILURE) {
                listener.onFailure(new ElasticsearchException(""));
            } else {
                listener.onResponse(response);
            }

            return null;
        }).when(client).execute(eq(DeleteModelAction.INSTANCE), any(), any());

        BulkByScrollResponse deleteByQueryResponse = mock(BulkByScrollResponse.class);
        when(deleteByQueryResponse.getDeleted()).thenReturn(10L);

        String detectorID = "123";

        DeleteDetector deleteDetector = mock(DeleteDetector.class);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length == 2);
            assertTrue(args[1] instanceof ActionListener);

            ActionListener<Void> listener = (ActionListener<Void>) args[1];

            listener.onResponse(null);

            return null;
        }).when(deleteDetector).markAnomalyResultDeleted(any(String.class), any());

        DeleteDetectorTransportAction action = new DeleteDetectorTransportAction(
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameResolver,
            client,
            deleteDetector
        );

        DeleteDetectorRequest request = new DeleteDetectorRequest().adID(detectorID);
        PlainActionFuture<AcknowledgedResponse> listener = new PlainActionFuture<>();
        action.masterOperation(task, request, null, listener);

        AcknowledgedResponse response = listener.actionGet();
        assertTrue(!response.isAcknowledged());

    }

    public void testNormalResponse() throws Exception {
        deleteDetectorResponseTemplate(DetectorExecutionMode.DELETE_MODEL_NORMAL);
    }

    public void testFailureResponse() throws Exception {
        deleteDetectorResponseTemplate(DetectorExecutionMode.DELETE_MODEL_FAILURE);
    }

}
