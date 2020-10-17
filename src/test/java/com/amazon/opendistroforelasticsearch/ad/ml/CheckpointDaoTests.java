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

package com.amazon.opendistroforelasticsearch.ad.ml;

import static com.amazon.opendistroforelasticsearch.ad.ml.CheckpointDao.FIELD_MODEL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import test.com.amazon.opendistroforelasticsearch.ad.util.MLUtil;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.randomcutforest.serialize.RandomCutForestSerDe;
import com.google.gson.Gson;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ Gson.class })
public class CheckpointDaoTests {
    private static final Logger logger = LogManager.getLogger(CheckpointDaoTests.class);

    private CheckpointDao checkpointDao;

    // dependencies
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Client client;

    @Mock
    private ClientUtil clientUtil;

    @Mock
    private GetResponse getResponse;

    @Mock
    private RandomCutForestSerDe rcfSerde;

    @Mock
    private Clock clock;

    @Mock
    private AnomalyDetectionIndices indexUtil;

    // configuration
    private String indexName;

    // test data
    private String modelId;
    private String model;
    private Map<String, Object> docSource;

    private Gson gson;
    private Class<? extends ThresholdingModel> thresholdingModelClass;
    private int maxBulkSize;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        indexName = "testIndexName";

        gson = PowerMockito.mock(Gson.class);

        thresholdingModelClass = HybridThresholdingModel.class;

        when(clock.instant()).thenReturn(Instant.now());

        maxBulkSize = 10;

        checkpointDao = new CheckpointDao(
            client,
            clientUtil,
            indexName,
            gson,
            rcfSerde,
            thresholdingModelClass,
            clock,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            indexUtil,
            maxBulkSize,
            200.0
        );

        when(indexUtil.doesCheckpointIndexExist()).thenReturn(true);

        modelId = "testModelId";
        model = "testModel";
        docSource = new HashMap<>();
        docSource.put(FIELD_MODEL, model);
    }

    private void verifySuccessfulPutModelCheckpointSync() {
        ArgumentCaptor<IndexRequest> indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(clientUtil)
            .timedRequest(
                indexRequestCaptor.capture(),
                anyObject(),
                Matchers.<BiConsumer<IndexRequest, ActionListener<IndexResponse>>>anyObject()
            );
        IndexRequest indexRequest = indexRequestCaptor.getValue();
        assertEquals(indexName, indexRequest.index());
        assertEquals(modelId, indexRequest.id());
        Set<String> expectedSourceKeys = new HashSet<String>(Arrays.asList(FIELD_MODEL, CheckpointDao.TIMESTAMP));
        assertEquals(expectedSourceKeys, indexRequest.sourceAsMap().keySet());
        assertEquals(model, indexRequest.sourceAsMap().get(FIELD_MODEL));
        assertNotNull(indexRequest.sourceAsMap().get(CheckpointDao.TIMESTAMP));
    }

    @Test
    public void putModelCheckpoint_getIndexRequest() {
        checkpointDao.putModelCheckpoint(modelId, model);

        verifySuccessfulPutModelCheckpointSync();
    }

    @Test
    public void putModelCheckpoint_no_checkpoint_index() {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onResponse(new CreateIndexResponse(true, true, CommonName.CHECKPOINT_INDEX_NAME));
            return null;
        }).when(indexUtil).initCheckpointIndex(any());

        checkpointDao.putModelCheckpoint(modelId, model);

        verifySuccessfulPutModelCheckpointSync();
    }

    @Test
    public void putModelCheckpoint_index_race_condition() {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new ResourceAlreadyExistsException(CommonName.CHECKPOINT_INDEX_NAME));
            return null;
        }).when(indexUtil).initCheckpointIndex(any());

        checkpointDao.putModelCheckpoint(modelId, model);

        verifySuccessfulPutModelCheckpointSync();
    }

    @Test
    public void putModelCheckpoint_unexpected_exception() {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new RuntimeException(""));
            return null;
        }).when(indexUtil).initCheckpointIndex(any());

        checkpointDao.putModelCheckpoint(modelId, model);

        verify(clientUtil, never()).timedRequest(any(), any(), any());
    }

    @Test
    public void getModelCheckpoint_returnExpected() {
        ArgumentCaptor<GetRequest> getRequestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        doReturn(Optional.of(getResponse))
            .when(clientUtil)
            .timedRequest(
                getRequestCaptor.capture(),
                anyObject(),
                Matchers.<BiConsumer<GetRequest, ActionListener<GetResponse>>>anyObject()
            );
        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getSource()).thenReturn(docSource);

        Optional<String> result = checkpointDao.getModelCheckpoint(modelId);

        assertTrue(result.isPresent());
        assertEquals(model, result.get());
        GetRequest getRequest = getRequestCaptor.getValue();
        assertEquals(indexName, getRequest.index());
        assertEquals(modelId, getRequest.id());
    }

    @Test
    public void getModelCheckpoint_returnEmpty_whenDocNotFound() {
        doReturn(Optional.of(getResponse))
            .when(clientUtil)
            .timedRequest(anyObject(), anyObject(), Matchers.<BiConsumer<GetRequest, ActionListener<GetResponse>>>anyObject());
        when(getResponse.isExists()).thenReturn(false);

        Optional<String> result = checkpointDao.getModelCheckpoint(modelId);

        assertFalse(result.isPresent());
    }

    @Test
    public void deleteModelCheckpoint_getDeleteRequest() {
        checkpointDao.deleteModelCheckpoint(modelId);

        ArgumentCaptor<DeleteRequest> deleteRequestCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        verify(clientUtil)
            .timedRequest(
                deleteRequestCaptor.capture(),
                anyObject(),
                Matchers.<BiConsumer<DeleteRequest, ActionListener<DeleteResponse>>>anyObject()
            );
        DeleteRequest deleteRequest = deleteRequestCaptor.getValue();
        assertEquals(indexName, deleteRequest.index());
        assertEquals(modelId, deleteRequest.id());
    }

    @SuppressWarnings("unchecked")
    private void verifyPutModelCheckpointAsync() {
        ArgumentCaptor<IndexRequest> requestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        doAnswer(invocation -> {
            ActionListener<IndexResponse> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(clientUtil).asyncRequest(requestCaptor.capture(), any(BiConsumer.class), any(ActionListener.class));

        ActionListener<Void> listener = mock(ActionListener.class);
        checkpointDao.putModelCheckpoint(modelId, model, listener);

        IndexRequest indexRequest = requestCaptor.getValue();
        assertEquals(indexName, indexRequest.index());
        assertEquals(modelId, indexRequest.id());
        Set<String> expectedSourceKeys = new HashSet<String>(Arrays.asList(FIELD_MODEL, CheckpointDao.TIMESTAMP));
        assertEquals(expectedSourceKeys, indexRequest.sourceAsMap().keySet());
        assertEquals(model, indexRequest.sourceAsMap().get(FIELD_MODEL));
        assertNotNull(indexRequest.sourceAsMap().get(CheckpointDao.TIMESTAMP));

        ArgumentCaptor<Void> responseCaptor = ArgumentCaptor.forClass(Void.class);
        verify(listener).onResponse(responseCaptor.capture());
        Void response = responseCaptor.getValue();
        assertEquals(null, response);
    }

    @Test
    public void putModelCheckpoint_callListener_whenCompleted() {
        verifyPutModelCheckpointAsync();
    }

    @Test
    public void putModelCheckpoint_callListener_no_checkpoint_index() {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onResponse(new CreateIndexResponse(true, true, CommonName.CHECKPOINT_INDEX_NAME));
            return null;
        }).when(indexUtil).initCheckpointIndex(any());

        verifyPutModelCheckpointAsync();
    }

    @Test
    public void putModelCheckpoint_callListener_race_condition() {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new ResourceAlreadyExistsException(CommonName.CHECKPOINT_INDEX_NAME));
            return null;
        }).when(indexUtil).initCheckpointIndex(any());

        verifyPutModelCheckpointAsync();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void putModelCheckpoint_callListener_unexpected_exception() {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new RuntimeException(""));
            return null;
        }).when(indexUtil).initCheckpointIndex(any());

        ActionListener<Void> listener = mock(ActionListener.class);
        checkpointDao.putModelCheckpoint(modelId, model, listener);

        verify(clientUtil, never()).asyncRequest(any(), any(), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getModelCheckpoint_returnExpectedToListener() {
        ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(2);
            listener.onResponse(getResponse);
            return null;
        }).when(clientUtil).asyncRequest(requestCaptor.capture(), any(BiConsumer.class), any(ActionListener.class));
        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getSource()).thenReturn(docSource);

        ActionListener<Optional<String>> listener = mock(ActionListener.class);
        checkpointDao.getModelCheckpoint(modelId, listener);

        GetRequest getRequest = requestCaptor.getValue();
        assertEquals(indexName, getRequest.index());
        assertEquals(modelId, getRequest.id());
        ArgumentCaptor<Optional<String>> responseCaptor = ArgumentCaptor.forClass(Optional.class);
        verify(listener).onResponse(responseCaptor.capture());
        Optional<String> result = responseCaptor.getValue();
        assertTrue(result.isPresent());
        assertEquals(model, result.get());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getModelCheckpoint_returnEmptyToListener_whenModelNotFound() {
        ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(2);
            listener.onResponse(getResponse);
            return null;
        }).when(clientUtil).asyncRequest(requestCaptor.capture(), any(BiConsumer.class), any(ActionListener.class));
        when(getResponse.isExists()).thenReturn(false);

        ActionListener<Optional<String>> listener = mock(ActionListener.class);
        checkpointDao.getModelCheckpoint(modelId, listener);

        GetRequest getRequest = requestCaptor.getValue();
        assertEquals(indexName, getRequest.index());
        assertEquals(modelId, getRequest.id());
        ArgumentCaptor<Optional<String>> responseCaptor = ArgumentCaptor.forClass(Optional.class);
        verify(listener).onResponse(responseCaptor.capture());
        Optional<String> result = responseCaptor.getValue();
        assertFalse(result.isPresent());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void deleteModelCheckpoint_callListener_whenCompleted() {
        ArgumentCaptor<DeleteRequest> requestCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        doAnswer(invocation -> {
            ActionListener<DeleteResponse> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(clientUtil).asyncRequest(requestCaptor.capture(), any(BiConsumer.class), any(ActionListener.class));

        ActionListener<Void> listener = mock(ActionListener.class);
        checkpointDao.deleteModelCheckpoint(modelId, listener);

        DeleteRequest deleteRequest = requestCaptor.getValue();
        assertEquals(indexName, deleteRequest.index());
        assertEquals(modelId, deleteRequest.id());

        ArgumentCaptor<Void> responseCaptor = ArgumentCaptor.forClass(Void.class);
        verify(listener).onResponse(responseCaptor.capture());
        Void response = responseCaptor.getValue();
        assertEquals(null, response);
    }

    private BulkResponse createBulkResponse(int succeeded, int failed, String[] failedId) {
        BulkItemResponse[] bulkItemResponses = new BulkItemResponse[succeeded + failed];

        ShardId shardId = new ShardId(CommonName.CHECKPOINT_INDEX_NAME, "", 1);
        int i = 0;
        for (; i < failed; i++) {
            bulkItemResponses[i] = new BulkItemResponse(
                i,
                DocWriteRequest.OpType.UPDATE,
                new BulkItemResponse.Failure(
                    CommonName.CHECKPOINT_INDEX_NAME,
                    CommonName.MAPPING_TYPE,
                    failedId[i],
                    new VersionConflictEngineException(shardId, "id", "test")
                )
            );
        }

        for (; i < failed + succeeded; i++) {
            bulkItemResponses[i] = new BulkItemResponse(
                i,
                DocWriteRequest.OpType.UPDATE,
                new UpdateResponse(shardId, CommonName.MAPPING_TYPE, "1", 0L, 1L, 1L, DocWriteResponse.Result.CREATED)
            );
        }

        return new BulkResponse(bulkItemResponses, 507);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void flush_less_than_1k() {
        int writeRequests = maxBulkSize - 1;
        for (int i = 0; i < writeRequests; i++) {
            ModelState<EntityModel> state = MLUtil.randomModelState();
            checkpointDao.write(state, state.getModelId(), true);
        }

        doAnswer(invocation -> {
            BulkRequest request = invocation.getArgument(1);
            assertEquals(writeRequests, request.numberOfActions());
            ActionListener<BulkResponse> listener = invocation.getArgument(2);

            listener.onResponse(createBulkResponse(request.numberOfActions(), 0, null));
            return null;
        }).when(clientUtil).execute(eq(BulkAction.INSTANCE), any(BulkRequest.class), any(ActionListener.class));

        checkpointDao.flush();

        verify(clientUtil, times(1)).execute(eq(BulkAction.INSTANCE), any(BulkRequest.class), any(ActionListener.class));
    }

    @SuppressWarnings("unchecked")
    public void flush_more_than_1k() {
        int writeRequests = maxBulkSize + 1;

        doAnswer(invocation -> {
            BulkRequest request = invocation.getArgument(1);
            assertEquals(maxBulkSize, request.numberOfActions());
            ActionListener<BulkResponse> listener = invocation.getArgument(2);

            listener.onResponse(createBulkResponse(request.numberOfActions(), 0, null));
            return null;
        }).when(clientUtil).execute(eq(BulkAction.INSTANCE), any(BulkRequest.class), any(ActionListener.class));

        for (int i = 0; i < writeRequests; i++) {
            ModelState<EntityModel> state = MLUtil.randomModelState();
            // should trigger auto flush
            checkpointDao.write(state, state.getModelId(), true);
        }

        verify(clientUtil, times(1)).execute(eq(BulkAction.INSTANCE), any(BulkRequest.class), any(ActionListener.class));
    }

    @Test
    public void flush_more_than_1k_has_index() {
        flush_more_than_1k();
    }

    @Test
    public void flush_more_than_1k_no_index() {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onResponse(new CreateIndexResponse(true, true, CommonName.CHECKPOINT_INDEX_NAME));
            return null;
        }).when(indexUtil).initCheckpointIndex(any());

        flush_more_than_1k();
    }

    @Test
    public void flush_more_than_1k_race_condition() {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new ResourceAlreadyExistsException(CommonName.CHECKPOINT_INDEX_NAME));
            return null;
        }).when(indexUtil).initCheckpointIndex(any());

        flush_more_than_1k();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void flush_more_than_1k_unexpected_exception() {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new RuntimeException(""));
            return null;
        }).when(indexUtil).initCheckpointIndex(any());

        verify(clientUtil, never()).execute(eq(BulkAction.INSTANCE), any(BulkRequest.class), any(ActionListener.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void bulk_has_failure() throws InterruptedException {
        int writeRequests = maxBulkSize - 1;
        int failureCount = 1;
        String[] failedId = new String[failureCount];
        for (int i = 0; i < writeRequests; i++) {
            ModelState<EntityModel> state = MLUtil.randomModelState();
            checkpointDao.write(state, state.getModelId(), true);
            if (i < failureCount) {
                failedId[i] = state.getModelId();
            }
        }

        doAnswer(invocation -> {
            BulkRequest request = invocation.getArgument(1);
            assertEquals(writeRequests, request.numberOfActions());
            ActionListener<BulkResponse> listener = invocation.getArgument(2);

            listener.onResponse(createBulkResponse(request.numberOfActions(), failureCount, failedId));
            return null;
        }).when(clientUtil).execute(eq(BulkAction.INSTANCE), any(BulkRequest.class), any(ActionListener.class));

        checkpointDao.flush();

        doAnswer(invocation -> {
            BulkRequest request = invocation.getArgument(1);
            assertEquals(failureCount, request.numberOfActions());
            ActionListener<BulkResponse> listener = invocation.getArgument(2);

            listener.onResponse(createBulkResponse(request.numberOfActions(), 0, null));
            return null;
        }).when(clientUtil).execute(eq(BulkAction.INSTANCE), any(BulkRequest.class), any(ActionListener.class));

        checkpointDao.flush();

        verify(clientUtil, times(2)).execute(eq(BulkAction.INSTANCE), any(BulkRequest.class), any(ActionListener.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void bulk_all_failure() throws InterruptedException {
        int writeRequests = maxBulkSize - 1;
        for (int i = 0; i < writeRequests; i++) {
            ModelState<EntityModel> state = MLUtil.randomModelState();
            checkpointDao.write(state, state.getModelId(), true);
        }

        doAnswer(invocation -> {
            BulkRequest request = invocation.getArgument(1);
            assertEquals(writeRequests, request.numberOfActions());
            ActionListener<BulkResponse> listener = invocation.getArgument(2);

            listener.onFailure(new RuntimeException(""));
            return null;
        }).when(clientUtil).execute(eq(BulkAction.INSTANCE), any(BulkRequest.class), any(ActionListener.class));

        checkpointDao.flush();

        doAnswer(invocation -> {
            BulkRequest request = invocation.getArgument(1);
            assertEquals(writeRequests, request.numberOfActions());
            ActionListener<BulkResponse> listener = invocation.getArgument(2);

            listener.onResponse(createBulkResponse(request.numberOfActions(), 0, null));
            return null;
        }).when(clientUtil).execute(eq(BulkAction.INSTANCE), any(BulkRequest.class), any(ActionListener.class));

        checkpointDao.flush();

        verify(clientUtil, times(2)).execute(eq(BulkAction.INSTANCE), any(BulkRequest.class), any(ActionListener.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void checkpoint_saved_less_than_1_hr() {
        ModelState<EntityModel> state = MLUtil.randomModelState();
        state.setLastCheckpointTime(Instant.now());
        checkpointDao.write(state, state.getModelId());

        checkpointDao.flush();

        verify(clientUtil, never()).execute(eq(BulkAction.INSTANCE), any(BulkRequest.class), any(ActionListener.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void checkpoint_coldstart_checkpoint() {
        ModelState<EntityModel> state = MLUtil.randomModelState();
        state.setLastCheckpointTime(Instant.now());
        // cold start checkpoint will save whatever
        checkpointDao.write(state, state.getModelId(), true);

        checkpointDao.flush();

        verify(clientUtil, times(1)).execute(eq(BulkAction.INSTANCE), any(BulkRequest.class), any(ActionListener.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void restore() throws IOException {
        ModelState<EntityModel> state = MLUtil.randomNonEmptyModelState();
        EntityModel modelToSave = state.getModel();

        checkpointDao = new CheckpointDao(
            client,
            clientUtil,
            indexName,
            new Gson(),
            new RandomCutForestSerDe(),
            thresholdingModelClass,
            clock,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            indexUtil,
            maxBulkSize,
            2
        );

        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(true);
        Map<String, Object> source = new HashMap<>();
        source.put(CheckpointDao.DETECTOR_ID, state.getDetectorId());
        source.put(CheckpointDao.FIELD_MODEL, checkpointDao.toCheckpoint(modelToSave));
        source.put(CheckpointDao.TIMESTAMP, "2020-10-11T22:58:23.610392Z");
        when(getResponse.getSource()).thenReturn(source);

        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(2);

            listener.onResponse(getResponse);
            return null;
        }).when(clientUtil).asyncRequest(any(GetRequest.class), any(BiConsumer.class), any(ActionListener.class));

        ActionListener<Optional<Entry<EntityModel, Instant>>> listener = mock(ActionListener.class);
        checkpointDao.restoreModelCheckpoint(modelId, listener);

        ArgumentCaptor<Optional<Entry<EntityModel, Instant>>> responseCaptor = ArgumentCaptor.forClass(Optional.class);
        verify(listener).onResponse(responseCaptor.capture());
        Optional<Entry<EntityModel, Instant>> response = responseCaptor.getValue();
        assertTrue(response.isPresent());
        Entry<EntityModel, Instant> entry = response.get();
        OffsetDateTime utcTime = entry.getValue().atOffset(ZoneOffset.UTC);
        assertEquals(2020, utcTime.getYear());
        assertEquals(Month.OCTOBER, utcTime.getMonth());
        assertEquals(11, utcTime.getDayOfMonth());
        assertEquals(22, utcTime.getHour());
        assertEquals(58, utcTime.getMinute());
        assertEquals(23, utcTime.getSecond());

        EntityModel model = entry.getKey();
        Queue<double[]> queue = model.getSamples();
        Queue<double[]> samplesToSave = modelToSave.getSamples();
        assertEquals(samplesToSave.size(), queue.size());
        assertTrue(Arrays.equals(samplesToSave.peek(), queue.peek()));
        logger.info(modelToSave.getRcf());
        logger.info(model.getRcf());
        assertEquals(modelToSave.getRcf().getTotalUpdates(), model.getRcf().getTotalUpdates());
        assertTrue(model.getThreshold() != null);
    }
}
