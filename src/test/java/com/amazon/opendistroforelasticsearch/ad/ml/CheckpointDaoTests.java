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

package com.amazon.opendistroforelasticsearch.ad.ml;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;

import org.junit.Before;
import org.junit.Test;

import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CheckpointDaoTests {

    private CheckpointDao checkpointDao;

    // dependencies
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Client client;

    @Mock
    private ClientUtil clientUtil;

    @Mock
    private GetResponse getResponse;

    // configuration
    private String indexName;

    // test data
    private String modelId;
    private String model;
    private Map<String, Object> docSource;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        indexName = "testIndexName";

        checkpointDao = new CheckpointDao(client, clientUtil, indexName);

        modelId = "testModelId";
        model = "testModel";
        docSource = new HashMap<>();
        docSource.put(CheckpointDao.FIELD_MODEL, model);
    }

    @Test
    public void putModelCheckpoint_getIndexRequest() {
        checkpointDao.putModelCheckpoint(modelId, model);

        ArgumentCaptor<IndexRequest> indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(clientUtil).timedRequest(indexRequestCaptor.capture(), anyObject(),
            Matchers.<BiConsumer<IndexRequest, ActionListener<IndexResponse>>>anyObject());
        IndexRequest indexRequest = indexRequestCaptor.getValue();
        assertEquals(indexName, indexRequest.index());
        assertEquals(CheckpointDao.DOC_TYPE, indexRequest.type());
        assertEquals(modelId, indexRequest.id());
        assertEquals(model, indexRequest.sourceAsMap().get(CheckpointDao.FIELD_MODEL));
    }

    @Test
    public void getModelCheckpoint_returnExpected() {
        ArgumentCaptor<GetRequest> getRequestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        doReturn(Optional.of(getResponse)).when(clientUtil).timedRequest(
            getRequestCaptor.capture(), anyObject(), Matchers.<BiConsumer<GetRequest, ActionListener<GetResponse>>>anyObject());
        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getSource()).thenReturn(docSource);

        Optional<String> result = checkpointDao.getModelCheckpoint(modelId);

        assertTrue(result.isPresent());
        assertEquals(model, result.get());
        GetRequest getRequest = getRequestCaptor.getValue();
        assertEquals(indexName, getRequest.index());
        assertEquals(CheckpointDao.DOC_TYPE, getRequest.type());
        assertEquals(modelId, getRequest.id());
    }

    @Test
    public void getModelCheckpoint_returnEmpty_whenDocNotFound() {
        doReturn(Optional.of(getResponse)).when(clientUtil).timedRequest(
            anyObject(), anyObject(), Matchers.<BiConsumer<GetRequest, ActionListener<GetResponse>>>anyObject());
        when(getResponse.isExists()).thenReturn(false);

        Optional<String> result = checkpointDao.getModelCheckpoint(modelId);

        assertFalse(result.isPresent());
    }

    @Test
    public void deleteModelCheckpoint_getDeleteRequest() {
        checkpointDao.deleteModelCheckpoint(modelId);

        ArgumentCaptor<DeleteRequest> deleteRequestCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        verify(clientUtil).timedRequest(deleteRequestCaptor.capture(), anyObject(),
            Matchers.<BiConsumer<DeleteRequest, ActionListener<DeleteResponse>>>anyObject());
        DeleteRequest deleteRequest = deleteRequestCaptor.getValue();
        assertEquals(indexName, deleteRequest.index());
        assertEquals(CheckpointDao.DOC_TYPE, deleteRequest.type());
        assertEquals(modelId, deleteRequest.id());
    }
}
