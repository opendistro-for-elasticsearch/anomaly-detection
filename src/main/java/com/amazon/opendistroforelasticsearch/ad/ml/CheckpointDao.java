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

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;

import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;

/**
 * DAO for model checkpoints.
 */
public class CheckpointDao {

    protected static final String DOC_TYPE = "_doc";
    protected static final String FIELD_MODEL = "model";
    public static final String TIMESTAMP = "timestamp";

    private static final Logger logger = LogManager.getLogger(CheckpointDao.class);

    // dependencies
    private final Client client;
    private final ClientUtil clientUtil;

    // configuration
    private final String indexName;

    /**
     * Constructor with dependencies and configuration.
     *
     * @param client ES search client
     * @param clientUtil utility with ES client
     * @param indexName name of the index for model checkpoints
     */
    public CheckpointDao(Client client, ClientUtil clientUtil, String indexName) {
        this.client = client;
        this.clientUtil = clientUtil;
        this.indexName = indexName;
    }

    /**
     * Puts a model checkpoint in the storage.
     *
     * @param modelId Id of the model
     * @param modelCheckpoint Checkpoint data of the model
     */
    public void putModelCheckpoint(String modelId, String modelCheckpoint) {
        Map<String, Object> source = new HashMap<>();
        source.put(FIELD_MODEL, modelCheckpoint);
        source.put(TIMESTAMP, ZonedDateTime.now(ZoneOffset.UTC));

        clientUtil
            .<IndexRequest, IndexResponse>timedRequest(
                new IndexRequest(indexName, DOC_TYPE, modelId).source(source),
                logger,
                client::index
            );
    }

    /**
     * Returns the checkpoint for the model.
     *
     * @param modelId ID of the model
     * @return model checkpoint, or empty if not found
     */
    public Optional<String> getModelCheckpoint(String modelId) {
        return clientUtil
            .<GetRequest, GetResponse>timedRequest(new GetRequest(indexName, DOC_TYPE, modelId), logger, client::get)
            .filter(GetResponse::isExists)
            .map(GetResponse::getSource)
            .map(source -> (String) source.get(FIELD_MODEL));
    }

    /**
     * Deletes the model checkpoint for the id.
     *
     * @param modelId ID of the model checkpoint
     */
    public void deleteModelCheckpoint(String modelId) {
        clientUtil.<DeleteRequest, DeleteResponse>timedRequest(new DeleteRequest(indexName, DOC_TYPE, modelId), logger, client::delete);
    }
}
