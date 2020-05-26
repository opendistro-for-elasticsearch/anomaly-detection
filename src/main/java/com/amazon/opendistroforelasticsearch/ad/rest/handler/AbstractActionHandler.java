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

package com.amazon.opendistroforelasticsearch.ad.rest.handler;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;

/**
 * Action handler to process REST request and handle failures.
 */
public abstract class AbstractActionHandler {

    protected final NodeClient client;
    protected final RestChannel channel;

    private final Logger logger = LogManager.getLogger(AbstractActionHandler.class);

    /**
     * Constructor function.
     *
     * @param client  ES node client that executes actions on the local node
     * @param channel ES channel used to construct bytes / builder based outputs, and send responses
     */
    public AbstractActionHandler(NodeClient client, RestChannel channel) {
        this.client = client;
        this.channel = channel;
    }

    /**
     * Send failure message via channel.
     *
     * @param e exception
     */
    public void onFailure(Exception e) {
        if (e != null) {
            try {
                channel.sendResponse(new BytesRestResponse(channel, e));
            } catch (IOException e1) {
                logger.warn("Fail to send out failure message of exception", e);
            }
        }
    }
}
