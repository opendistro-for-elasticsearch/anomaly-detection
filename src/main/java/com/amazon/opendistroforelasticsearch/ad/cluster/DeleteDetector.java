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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ScrollableHitSource;

public class DeleteDetector {
    private static final Logger LOG = LogManager.getLogger(DeleteDetector.class);
    private static final String UPDATE_TASK_NAME = "update-detector-graveyard";
    static final String INDEX_DELETED_LOG_MSG = "Anomaly result index has been deleted.  Has nothing to do:";
    static final String NOT_ABLE_TO_DELETE_LOG_MSG = "Cannot delete all anomaly result of detector";
    static final String DOC_GOT_DELETED_LOG_MSG = "Anomaly result docs get deleted";
    static final String TIMEOUT_LOG_MSG = "Timeout while deleting anomaly results of";
    static final String BULK_FAILURE_LOG_MSG = "Bulk failure while deleting anomaly results of";
    static final String SEARCH_FAILURE_LOG_MSG = "Search failure while deleting anomaly results of";

    private ClusterService clusterService;
    private Clock clock;

    public DeleteDetector(ClusterService cluserService, Clock clock) {
        this.clusterService = cluserService;
        this.clock = clock;
    }

    private void logFailure(BulkByScrollResponse response, String detectorID) {
        if (response.isTimedOut()) {
            LOG.warn(TIMEOUT_LOG_MSG + " {}", detectorID);
        } else if (!response.getBulkFailures().isEmpty()) {
            LOG.warn(BULK_FAILURE_LOG_MSG + " {}", detectorID);
            for (BulkItemResponse.Failure bulkFailure : response.getBulkFailures()) {
                LOG.warn(bulkFailure);
            }
        } else {
            LOG.warn(SEARCH_FAILURE_LOG_MSG + " {}", detectorID);
            for (ScrollableHitSource.SearchFailure searchFailure : response.getSearchFailures()) {
                LOG.warn(searchFailure);
            }
        }
    }

    public void deleteDetectorResult(Client client) {
        ADMetaData metaData = ADMetaData.getADMetaData(clusterService.state());
        Set<AnomalyDetectorGraveyard> failToDelete = Collections.synchronizedSet(new HashSet<>());
        AtomicInteger count = new AtomicInteger(0);
        Set<AnomalyDetectorGraveyard> deadDetectors = metaData.getAnomalyDetectorGraveyard();
        for (AnomalyDetectorGraveyard detectorToDelete : deadDetectors) {
            deleteDetectorResult(detectorToDelete, count, deadDetectors.size(), client, failToDelete);
        }
    }

    private void deleteDetectorResult(
        AnomalyDetectorGraveyard detectorToDelete,
        AtomicInteger count,
        int total,
        Client client,
        Set<AnomalyDetectorGraveyard> failToDelete
    ) {
        // A bulk delete request is performed for each batch of matching documents. If a
        // search or bulk request is rejected, the requests are retried up to 10 times,
        // with exponential back off. If the maximum retry limit is reached, processing
        // halts and all failed requests are returned in the response. Any delete
        // requests that completed successfully still stick, they are not rolled back.
        String detectorID = detectorToDelete.getDetectorID();
        long deleteBeforeEpochMillis = detectorToDelete.getDeleteEpochMillis();
        DeleteByQueryRequest deleteRequest = new DeleteByQueryRequest(AnomalyDetectionIndices.ALL_AD_RESULTS_INDEX_PATTERN)
            .setQuery(
                new BoolQueryBuilder()
                    .filter(QueryBuilders.termsQuery(AnomalyResult.DETECTOR_ID_FIELD, detectorID))
                    .filter(
                        QueryBuilders
                            .rangeQuery(AnomalyResult.DATA_END_TIME_FIELD)
                            .lte(deleteBeforeEpochMillis)
                            .format(CommonName.EPOCH_MILLIS_FORMAT)
                    )
            )
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .setAbortOnVersionConflict(false) // when current delete happens, previous might not finish.
                                              // Retry in this case
            .setRequestsPerSecond(500); // throttle delete requests
        LOG.info("Delete anomaly results of detector {}", detectorID);
        client.execute(DeleteByQueryAction.INSTANCE, deleteRequest, ActionListener.wrap(response -> {
            if (response.isTimedOut() || !response.getBulkFailures().isEmpty() || !response.getSearchFailures().isEmpty()) {
                logFailure(response, detectorID);
                failToDelete.add(detectorToDelete);
            }
            // if 0 docs get deleted, it means we cannot find matching docs
            LOG.info("{} " + DOC_GOT_DELETED_LOG_MSG, response.getDeleted());
            if (count.incrementAndGet() == total) {
                wrapUp(failToDelete);
            }
        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                LOG.info(INDEX_DELETED_LOG_MSG + " {}", detectorID);
            } else {
                // Gonna eventually delete in maintenance window.
                LOG.error(NOT_ABLE_TO_DELETE_LOG_MSG, exception);
                failToDelete.add(detectorToDelete);
            }
            if (count.incrementAndGet() == total) {
                wrapUp(failToDelete);
            }
        }));
    }

    private void wrapUp(Set<AnomalyDetectorGraveyard> failToDelete) {
        clusterService.submitStateUpdateTask(UPDATE_TASK_NAME, new ClusterStateUpdateTask(Priority.LOW) {

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                ClusterState.Builder newState = ClusterState.builder(currentState);
                MetaData.Builder metaDataBuilder = MetaData.builder(currentState.getMetaData());
                if (failToDelete.isEmpty()) {
                    metaDataBuilder.putCustom(ADMetaData.TYPE, ADMetaData.EMPTY_METADATA);
                } else {
                    metaDataBuilder.putCustom(ADMetaData.TYPE, new ADMetaData(failToDelete));
                }

                newState.metaData(metaDataBuilder.build());
                return newState.build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                LOG.error("Fail to submit undeleted detector metadata", e);
            }
        });
    }

    public void markAnomalyResultDeleted(String adID, ActionListener<Void> listener) {
        clusterService.submitStateUpdateTask(UPDATE_TASK_NAME, new ClusterStateUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                ClusterState.Builder newState = ClusterState.builder(currentState);
                MetaData.Builder metaDataBuilder = MetaData.builder(currentState.getMetaData());

                ADMetaData adMeta = ADMetaData.getADMetaData(currentState);
                Set<AnomalyDetectorGraveyard> newDeletedDetectors = new HashSet<>(adMeta.getAnomalyDetectorGraveyard());
                newDeletedDetectors.add(new AnomalyDetectorGraveyard(adID, clock.millis()));
                metaDataBuilder.putCustom(adMeta.getWriteableName(), new ADMetaData(newDeletedDetectors));

                newState.metaData(metaDataBuilder.build());
                return newState.build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                LOG.error("Fail to mark detector deleted", e);
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) {
                listener.onResponse(null);
            }
        });

    }
}
