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

import java.time.Clock;
import java.time.Duration;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.ml.CheckpointDao;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;

public class DailyCron implements Runnable {
    private static final Logger LOG = LogManager.getLogger(DailyCron.class);
    protected static final String FIELD_MODEL = "queue";
    static final String CANNOT_DELETE_OLD_CHECKPOINT_MSG = "Cannot delete old checkpoint.";
    static final String CHECKPOINT_NOT_EXIST_MSG = "Checkpoint index does not exist.";
    static final String CHECKPOINT_DELETED_MSG = "checkpoint docs get deleted";

    private final DeleteDetector deleteUtil;
    private final Clock clock;
    private final Client client;
    private final Duration checkpointTtl;
    private final ClientUtil clientUtil;

    public DailyCron(DeleteDetector deleteUtil, Clock clock, Client client, Duration checkpointTtl, ClientUtil clientUtil) {
        this.deleteUtil = deleteUtil;
        this.clock = clock;
        this.client = client;
        this.clientUtil = clientUtil;
        this.checkpointTtl = checkpointTtl;
    }

    @Override
    public void run() {
        DeleteByQueryRequest deleteRequest = new DeleteByQueryRequest(CommonName.CHECKPOINT_INDEX_NAME)
            .setQuery(
                QueryBuilders
                    .boolQuery()
                    .filter(
                        QueryBuilders
                            .rangeQuery(CheckpointDao.TIMESTAMP)
                            .lte(clock.millis() - checkpointTtl.toMillis())
                            .format(CommonName.EPOCH_MILLIS_FORMAT)
                    )
            )
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        clientUtil
            .execute(
                DeleteByQueryAction.INSTANCE,
                deleteRequest,
                ActionListener
                    .wrap(
                        response -> {
                            // if 0 docs get deleted, it means our query cannot find any matching doc
                            LOG.info("{} " + CHECKPOINT_DELETED_MSG, response.getDeleted());
                        },
                        exception -> {
                            if (exception instanceof IndexNotFoundException) {
                                LOG.info(CHECKPOINT_NOT_EXIST_MSG);
                            } else {
                                // Gonna eventually delete in maintenance window.
                                LOG.error(CANNOT_DELETE_OLD_CHECKPOINT_MSG, exception);
                            }
                        }
                    )
            );
        deleteUtil.deleteDetectorResult(client);
    }

}
