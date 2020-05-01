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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.google.common.io.Resources;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.XContentType;

import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;

/**
 * DAO for model checkpoints.
 */
public class CheckpointDao {

    protected static final String DOC_TYPE = "_doc";
    protected static final String FIELD_MODEL = "model";
    protected static final String MAPPING_PATH = "mappings/checkpoint.json";
    public static final String TIMESTAMP = "timestamp";

    private static final Logger logger = LogManager.getLogger(CheckpointDao.class);

    // dependencies
    private final Client client;
    private final ClientUtil clientUtil;
    private final ClusterService clusterService;

    // configuration
    private final String indexName;

    /**
     * Constructor with dependencies and configuration.
     *
     * @param client ES search client
     * @param clientUtil utility with ES client
     * @param clusterService provides cluster info
     * @param indexName name of the index for model checkpoints
     */
    public CheckpointDao(Client client, ClientUtil clientUtil, ClusterService clusterService, String indexName) {
        this.client = client;
        this.clientUtil = clientUtil;
        this.clusterService = clusterService;
        this.indexName = indexName;
    }

    /**
     * Puts a model checkpoint in the storage.
     *
     * @deprecated use putModelCheckpoint with listener instead
     *
     * @param modelId Id of the model
     * @param modelCheckpoint Checkpoint data of the model
     */
    @Deprecated
    public void putModelCheckpoint(String modelId, byte[] modelCheckpoint) {
        Map<String, Object> source = new HashMap<>();
        source.put(FIELD_MODEL, modelCheckpoint);
        source.put(TIMESTAMP, ZonedDateTime.now(ZoneOffset.UTC));

        createCheckpointIndexIfAbsent(
            ActionListener
                .wrap(
                    r -> clientUtil
                        .<IndexRequest, IndexResponse>timedRequest(
                            new IndexRequest(indexName, DOC_TYPE, modelId).source(source),
                            logger,
                            client::index
                        ),
                    e -> logger.warn("Failed to put checkpoint for " + modelId, e)
                )
        );
    }

    /**
     * Puts a model checkpoint in the storage.
     *
     * @param modelId id of the model
     * @param modelCheckpoint checkpoint of the model
     * @param listener onResponse is called with null when the operation is completed
     */
    public void putModelCheckpoint(String modelId, byte[] modelCheckpoint, ActionListener<Void> listener) {
        Map<String, Object> source = new HashMap<>();
        source.put(FIELD_MODEL, modelCheckpoint);
        source.put(TIMESTAMP, ZonedDateTime.now(ZoneOffset.UTC));

        createCheckpointIndexIfAbsent(
            ActionListener
                .wrap(
                    s -> clientUtil
                        .<IndexRequest, IndexResponse>asyncRequest(
                            new IndexRequest(indexName, DOC_TYPE, modelId).source(source),
                            client::index,
                            ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure)
                        ),
                    listener::onFailure
                )
        );
    }

    /**
     * Returns the checkpoint for the model.
     *
     * @deprecated use getModelCheckpoint with listener instead
     *
     * @param modelId ID of the model
     * @return model checkpoint, or empty if not found
     */
    @Deprecated
    public Optional<byte[]> getModelCheckpoint(String modelId) {
        return clientUtil
            .<GetRequest, GetResponse>timedRequest(new GetRequest(indexName, DOC_TYPE, modelId), logger, client::get)
            .filter(GetResponse::isExists)
            .map(GetResponse::getSource)
            .map(source -> (byte[]) source.get(FIELD_MODEL));
    }

    /**
     * Returns to listener the checkpoint for the model.
     *
     * @param modelId id of the model
     * @param listener onResponse is called with the model checkpoint, or empty for no such model
     */
    public void getModelCheckpoint(String modelId, ActionListener<Optional<byte[]>> listener) {
        clientUtil
            .<GetRequest, GetResponse>asyncRequest(
                new GetRequest(indexName, DOC_TYPE, modelId),
                client::get,
                ActionListener.wrap(response -> listener.onResponse(processModelCheckpoint(response)), listener::onFailure)
            );
    }

    private Optional<byte[]> processModelCheckpoint(GetResponse response) {
        return Optional
            .ofNullable(response)
            .filter(GetResponse::isExists)
            .map(GetResponse::getSource)
            .map(source -> Base64.getDecoder().decode((String) source.get(FIELD_MODEL)));
    }

    /**
     * Deletes the model checkpoint for the id.
     *
     * @deprecated use deleteModelCheckpoint with listener instead
     *
     * @param modelId ID of the model checkpoint
     */
    @Deprecated
    public void deleteModelCheckpoint(String modelId) {
        clientUtil.<DeleteRequest, DeleteResponse>timedRequest(new DeleteRequest(indexName, DOC_TYPE, modelId), logger, client::delete);
    }

    /**
     * Deletes the model checkpoint for the model.
     *
     * @param modelId id of the model
     * @param listener onReponse is called with null when the operation is completed
     */
    public void deleteModelCheckpoint(String modelId, ActionListener<Void> listener) {
        clientUtil
            .<DeleteRequest, DeleteResponse>asyncRequest(
                new DeleteRequest(indexName, DOC_TYPE, modelId),
                client::delete,
                ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure)
            );
    }

    private void createCheckpointIndexIfAbsent(ActionListener<CreateIndexResponse> listener) {
        if (clusterService.state().getRoutingTable().hasIndex(indexName)) {
            listener.onResponse(null);
        } else {
            try {
                CreateIndexRequest request = new CreateIndexRequest(indexName)
                    .mapping(
                        DOC_TYPE,
                        Resources.toString(getClass().getClassLoader().getResource(MAPPING_PATH), StandardCharsets.UTF_8),
                        XContentType.JSON
                    );
                client.admin().indices().create(request, listener);
            } catch (IOException e) {
                listener.onFailure(e);
            }
        }
    }
}
