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

import com.amazon.opendistroforelasticsearch.ad.cluster.DeleteDetector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class DeleteDetectorTransportAction extends TransportMasterNodeAction<DeleteDetectorRequest, AcknowledgedResponse> {
    private static final Logger LOG = LogManager.getLogger(DeleteDetectorTransportAction.class);

    private final Client client;
    private final ClusterService clusterService;
    private final DeleteDetector deleteUtil;

    @Inject
    public DeleteDetectorTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client,
        DeleteDetector deleteUtil
    ) {
        super(
            DeleteDetectorAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteDetectorRequest::new,
            indexNameExpressionResolver
        );
        this.client = client;
        this.clusterService = clusterService;
        this.deleteUtil = deleteUtil;
    }

    @Override
    protected String executor() {
        // the transport action handler will run on the same thread that trigger the
        // action. For example, on the coordinating node, since RESTful Delete API
        // trigger delete detector transport action, the thread running RESTful Delete
        // API would run DeleteDetectorTransportAction. Since this is a master node
        // action, the coordinating node would forward this request to master node.
        // Whatever thread on master node that gets the request would execute
        // DeleteDetectorTransportAction.
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(DeleteDetectorRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener)
        throws Exception {
        throw new UnsupportedOperationException("Need a task ID");
    }

    @Override
    protected void masterOperation(
        Task task,
        DeleteDetectorRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {

        String adID = request.getAdID();
        deleteUtil.markAnomalyResultDeleted(adID, ActionListener.wrap(success -> {
            DiscoveryNode[] dataNodes = clusterService.state().nodes().getDataNodes().values().toArray(DiscoveryNode.class);

            DeleteModelRequest modelDeleteRequest = new DeleteModelRequest(adID, dataNodes);
            client.execute(DeleteModelAction.INSTANCE, modelDeleteRequest, ActionListener.wrap(response -> {
                if (response.hasFailures()) {
                    LOG.warn("Cannot delete all models of detector {}", adID);
                    for (FailedNodeException failedNodeException : response.failures()) {
                        LOG.warn("Deleting models of node has exception", failedNodeException);
                    }
                    // if customers are using an updated detector and we haven't deleted old
                    // checkpoints, customer would have trouble
                    listener.onResponse(new AcknowledgedResponse(false));
                } else {
                    LOG.info("models of detector {} get deleted", adID);
                    listener.onResponse(new AcknowledgedResponse(true));
                }
            }, exception -> {
                LOG.error(new ParameterizedMessage("Deletion of detector [{}] has exception.", adID), exception);
                listener.onResponse(new AcknowledgedResponse(false));
            }));
        }, listener::onFailure));

    }

    @Override
    protected ClusterBlockException checkBlock(DeleteDetectorRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

}
