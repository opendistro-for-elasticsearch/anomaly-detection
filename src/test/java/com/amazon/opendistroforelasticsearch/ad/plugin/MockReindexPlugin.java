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

package com.amazon.opendistroforelasticsearch.ad.plugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.google.common.collect.ImmutableList;

public class MockReindexPlugin extends Plugin implements ActionPlugin {

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays
            .asList(
                new ActionHandler<>(UpdateByQueryAction.INSTANCE, MockTransportUpdateByQueryAction.class),
                new ActionHandler<>(DeleteByQueryAction.INSTANCE, MockTransportDeleteByQueryAction.class)
            );
    }

    public static class MockTransportUpdateByQueryAction extends HandledTransportAction<UpdateByQueryRequest, BulkByScrollResponse> {

        @Inject
        public MockTransportUpdateByQueryAction(ActionFilters actionFilters, TransportService transportService) {
            super(UpdateByQueryAction.NAME, transportService, actionFilters, UpdateByQueryRequest::new);
        }

        @Override
        protected void doExecute(Task task, UpdateByQueryRequest request, ActionListener<BulkByScrollResponse> listener) {
            BulkByScrollResponse response = null;
            try {
                XContentParser parser = TestHelpers
                    .parser(
                        "{\"slice_id\":1,\"total\":2,\"updated\":3,\"created\":0,\"deleted\":0,\"batches\":6,"
                            + "\"version_conflicts\":0,\"noops\":0,\"retries\":{\"bulk\":0,\"search\":10},"
                            + "\"throttled_millis\":0,\"requests_per_second\":13.0,\"canceled\":\"reasonCancelled\","
                            + "\"throttled_until_millis\":14}"
                    );
                parser.nextToken();
                response = new BulkByScrollResponse(
                    TimeValue.timeValueMillis(10),
                    BulkByScrollTask.Status.innerFromXContent(parser),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    false
                );
            } catch (IOException exception) {
                exception.printStackTrace();
            }
            listener.onResponse(response);
        }
    }

    public static class MockTransportDeleteByQueryAction extends HandledTransportAction<DeleteByQueryRequest, BulkByScrollResponse> {

        private Client client;

        @Inject
        public MockTransportDeleteByQueryAction(ActionFilters actionFilters, TransportService transportService, Client client) {
            super(DeleteByQueryAction.NAME, transportService, actionFilters, DeleteByQueryRequest::new);
            this.client = client;
        }

        private class MultiResponsesActionListener implements ActionListener<DeleteResponse> {
            private final ActionListener<BulkByScrollResponse> delegate;
            private final AtomicInteger collectedResponseCount;
            private final AtomicLong maxResponseCount;
            private final AtomicBoolean hasFailure;

            MultiResponsesActionListener(ActionListener<BulkByScrollResponse> delegate, long maxResponseCount) {
                this.delegate = delegate;
                this.collectedResponseCount = new AtomicInteger(0);
                this.maxResponseCount = new AtomicLong(maxResponseCount);
                this.hasFailure = new AtomicBoolean(false);
            }

            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                if (collectedResponseCount.incrementAndGet() >= maxResponseCount.get()) {
                    finish();
                }
            }

            @Override
            public void onFailure(Exception e) {
                this.hasFailure.set(true);
                if (collectedResponseCount.incrementAndGet() >= maxResponseCount.get()) {
                    finish();
                }
            }

            private void finish() {
                if (this.hasFailure.get()) {
                    this.delegate.onFailure(new RuntimeException("failed to delete old AD tasks"));
                } else {
                    try {
                        XContentParser parser = TestHelpers
                            .parser(
                                "{\"slice_id\":1,\"total\":2,\"updated\":0,\"created\":0,\"deleted\":"
                                    + maxResponseCount
                                    + ",\"batches\":6,\"version_conflicts\":0,\"noops\":0,\"retries\":{\"bulk\":0,"
                                    + "\"search\":10},\"throttled_millis\":0,\"requests_per_second\":13.0,\"canceled\":"
                                    + "\"reasonCancelled\",\"throttled_until_millis\":14}"
                            );
                        parser.nextToken();
                        BulkByScrollResponse response = new BulkByScrollResponse(
                            TimeValue.timeValueMillis(10),
                            BulkByScrollTask.Status.innerFromXContent(parser),
                            ImmutableList.of(),
                            ImmutableList.of(),
                            false
                        );
                        this.delegate.onResponse(response);
                    } catch (IOException exception) {
                        this.delegate.onFailure(new RuntimeException("failed to parse BulkByScrollResponse"));
                    }
                }
            }
        }

        @Override
        protected void doExecute(Task task, DeleteByQueryRequest request, ActionListener<BulkByScrollResponse> listener) {
            SearchRequest searchRequest = request.getSearchRequest();
            client.search(searchRequest, ActionListener.wrap(r -> {
                long totalHits = r.getHits().getTotalHits().value;
                MultiResponsesActionListener delegateListener = new MultiResponsesActionListener(listener, totalHits);
                Iterator<SearchHit> iterator = r.getHits().iterator();
                while (iterator.hasNext()) {
                    String id = iterator.next().getId();
                    DeleteRequest deleteRequest = new DeleteRequest(ADTask.DETECTION_STATE_INDEX, id)
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                    client.delete(deleteRequest, delegateListener);
                }
            }, e -> listener.onFailure(e)));
        }
    }
}
