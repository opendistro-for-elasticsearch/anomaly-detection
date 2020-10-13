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

package com.amazon.opendistroforelasticsearch.ad.indices;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.ANOMALY_DETECTION_STATE_INDEX_MAPPING_FILE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.ANOMALY_DETECTORS_INDEX_MAPPING_FILE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.ANOMALY_DETECTOR_JOBS_INDEX_MAPPING_FILE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.ANOMALY_RESULTS_INDEX_MAPPING_FILE;

import java.io.IOException;
import java.net.URL;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorInternalState;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

/**
 * This class provides utility methods for various anomaly detection indices.
 */
public class AnomalyDetectionIndices implements LocalNodeMasterListener {

    // The index name pattern to query all the AD result history indices
    public static final String AD_RESULT_HISTORY_INDEX_PATTERN = "<.opendistro-anomaly-results-history-{now/d}-1>";

    // The index name pattern to query all AD result, history and current AD result
    public static final String ALL_AD_RESULTS_INDEX_PATTERN = ".opendistro-anomaly-results*";

    // Elastic mapping type
    static final String MAPPING_TYPE = "_doc";

    private ClusterService clusterService;
    private final AdminClient adminClient;
    private final ThreadPool threadPool;

    private volatile TimeValue historyRolloverPeriod;
    private volatile Long historyMaxDocs;
    private volatile TimeValue historyRetentionPeriod;

    private Scheduler.Cancellable scheduledRollover = null;

    private static final Logger logger = LogManager.getLogger(AnomalyDetectionIndices.class);

    /**
     * Constructor function
     *
     * @param client         ES client supports administrative actions
     * @param clusterService ES cluster service
     * @param threadPool     ES thread pool
     * @param settings       ES cluster setting
     */
    public AnomalyDetectionIndices(Client client, ClusterService clusterService, ThreadPool threadPool, Settings settings) {
        this.adminClient = client.admin();
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.clusterService.addLocalNodeMasterListener(this);
        this.historyRolloverPeriod = AD_RESULT_HISTORY_ROLLOVER_PERIOD.get(settings);
        this.historyMaxDocs = AD_RESULT_HISTORY_MAX_DOCS.get(settings);
        this.historyRetentionPeriod = AD_RESULT_HISTORY_RETENTION_PERIOD.get(settings);
        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_RESULT_HISTORY_MAX_DOCS, it -> historyMaxDocs = it);
        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_RESULT_HISTORY_ROLLOVER_PERIOD, it -> {
            historyRolloverPeriod = it;
            rescheduleRollover();
        });
        this.clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(AD_RESULT_HISTORY_RETENTION_PERIOD, it -> { historyRetentionPeriod = it; });
    }

    /**
     * Get anomaly detector index mapping json content.
     *
     * @return anomaly detector index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    private String getAnomalyDetectorMappings() throws IOException {
        URL url = AnomalyDetectionIndices.class.getClassLoader().getResource(ANOMALY_DETECTORS_INDEX_MAPPING_FILE);
        return Resources.toString(url, Charsets.UTF_8);
    }

    /**
     * Get anomaly result index mapping json content.
     *
     * @return anomaly result index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    private String getAnomalyResultMappings() throws IOException {
        URL url = AnomalyDetectionIndices.class.getClassLoader().getResource(ANOMALY_RESULTS_INDEX_MAPPING_FILE);
        return Resources.toString(url, Charsets.UTF_8);
    }

    /**
     * Get anomaly detector job index mapping json content.
     *
     * @return anomaly detector job index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    private String getAnomalyDetectorJobMappings() throws IOException {
        URL url = AnomalyDetectionIndices.class.getClassLoader().getResource(ANOMALY_DETECTOR_JOBS_INDEX_MAPPING_FILE);
        return Resources.toString(url, Charsets.UTF_8);
    }

    /**
     * Get anomaly detector state index mapping json content.
     *
     * @return anomaly detector state index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    private String getDetectorStateMappings() throws IOException {
        URL url = AnomalyDetectionIndices.class.getClassLoader().getResource(ANOMALY_DETECTION_STATE_INDEX_MAPPING_FILE);
        return Resources.toString(url, Charsets.UTF_8);
    }

    /**
     * Anomaly detector index exist or not.
     *
     * @return true if anomaly detector index exists
     */
    public boolean doesAnomalyDetectorIndexExist() {
        return clusterService.state().getRoutingTable().hasIndex(AnomalyDetector.ANOMALY_DETECTORS_INDEX);
    }

    /**
     * Anomaly detector job index exist or not.
     *
     * @return true if anomaly detector job index exists
     */
    public boolean doesAnomalyDetectorJobIndexExist() {
        return clusterService.state().getRoutingTable().hasIndex(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX);
    }

    /**
     * Anomaly result index exist or not.
     *
     * @return true if anomaly detector index exists
     */
    public boolean doesAnomalyResultIndexExist() {
        return clusterService.state().metadata().hasAlias(CommonName.ANOMALY_RESULT_INDEX_ALIAS);
    }

    /**
     * Anomaly result index exist or not.
     *
     * @return true if anomaly detector index exists
     */
    public boolean doesDetectorStateIndexExist() {
        return clusterService.state().getRoutingTable().hasIndex(DetectorInternalState.DETECTOR_STATE_INDEX);
    }

    /**
     * Create anomaly detector index if not exist.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link AnomalyDetectionIndices#getAnomalyDetectorMappings}
     */
    public void initAnomalyDetectorIndexIfAbsent(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        if (!doesAnomalyDetectorIndexExist()) {
            initAnomalyDetectorIndex(actionListener);
        }
    }

    /**
     * Create anomaly detector index directly.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link AnomalyDetectionIndices#getAnomalyDetectorMappings}
     */
    public void initAnomalyDetectorIndex(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX)
            .mapping(AnomalyDetector.TYPE, getAnomalyDetectorMappings(), XContentType.JSON);
        adminClient.indices().create(request, actionListener);
    }

    /**
     * Create anomaly detector index if not exist.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link AnomalyDetectionIndices#getAnomalyDetectorMappings}
     */
    public void initAnomalyResultIndexIfAbsent(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        if (!doesAnomalyResultIndexExist()) {
            initAnomalyResultIndexDirectly(actionListener);
        }
    }

    /**
     * Create anomaly detector index without checking exist or not.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link AnomalyDetectionIndices#getAnomalyDetectorMappings}
     */
    public void initAnomalyResultIndexDirectly(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        String mapping = getAnomalyResultMappings();
        CreateIndexRequest request = new CreateIndexRequest(AD_RESULT_HISTORY_INDEX_PATTERN)
            .mapping(MAPPING_TYPE, mapping, XContentType.JSON)
            .alias(new Alias(CommonName.ANOMALY_RESULT_INDEX_ALIAS));
        adminClient.indices().create(request, actionListener);
    }

    /**
     * Create anomaly detector job index.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link AnomalyDetectionIndices#getAnomalyDetectorJobMappings}
     */
    public void initAnomalyDetectorJobIndex(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        // TODO: specify replica setting
        CreateIndexRequest request = new CreateIndexRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX)
            .mapping(AnomalyDetector.TYPE, getAnomalyDetectorJobMappings(), XContentType.JSON);
        adminClient.indices().create(request, actionListener);
    }

    /**
     * Create an index.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link AnomalyDetectionIndices#getAnomalyDetectorJobMappings}
     */
    public void initDetectorStateIndex(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(DetectorInternalState.DETECTOR_STATE_INDEX)
            .mapping(AnomalyDetector.TYPE, getDetectorStateMappings(), XContentType.JSON);
        adminClient.indices().create(request, actionListener);
    }

    @Override
    public void onMaster() {
        try {
            // try to rollover immediately as we might be restarting the cluster
            rolloverAndDeleteHistoryIndex();
            // schedule the next rollover for approx MAX_AGE later
            scheduledRollover = threadPool
                .scheduleWithFixedDelay(() -> rolloverAndDeleteHistoryIndex(), historyRolloverPeriod, executorName());
        } catch (Exception e) {
            // This should be run on cluster startup
            logger.error("Error rollover AD result indices. " + "Can't rollover AD result until master node is restarted.", e);
        }
    }

    @Override
    public void offMaster() {
        if (scheduledRollover != null) {
            scheduledRollover.cancel();
        }
    }

    @Override
    public String executorName() {
        return ThreadPool.Names.MANAGEMENT;
    }

    private void rescheduleRollover() {
        if (clusterService.state().getNodes().isLocalNodeElectedMaster()) {
            if (scheduledRollover != null) {
                scheduledRollover.cancel();
            }
            scheduledRollover = threadPool
                .scheduleWithFixedDelay(() -> rolloverAndDeleteHistoryIndex(), historyRolloverPeriod, executorName());
        }
    }

    void rolloverAndDeleteHistoryIndex() {
        if (!doesAnomalyResultIndexExist()) {
            return;
        }

        // We have to pass null for newIndexName in order to get Elastic to increment the index count.
        RolloverRequest request = new RolloverRequest(CommonName.ANOMALY_RESULT_INDEX_ALIAS, null);
        String adResultMapping = null;
        try {
            adResultMapping = getAnomalyResultMappings();
        } catch (IOException e) {
            logger.error("Fail to roll over AD result index, as can't get AD result index mapping");
            return;
        }
        request.getCreateIndexRequest().index(AD_RESULT_HISTORY_INDEX_PATTERN).mapping(MAPPING_TYPE, adResultMapping, XContentType.JSON);
        request.addMaxIndexDocsCondition(historyMaxDocs);
        adminClient.indices().rolloverIndex(request, ActionListener.wrap(response -> {
            if (!response.isRolledOver()) {
                logger
                    .warn("{} not rolled over. Conditions were: {}", CommonName.ANOMALY_RESULT_INDEX_ALIAS, response.getConditionStatus());
            } else {
                logger.info("{} rolled over. Conditions were: {}", CommonName.ANOMALY_RESULT_INDEX_ALIAS, response.getConditionStatus());
                deleteOldHistoryIndices();
            }
        }, exception -> { logger.error("Fail to roll over result index", exception); }));
    }

    void deleteOldHistoryIndices() {
        Set<String> candidates = new HashSet<String>();

        ClusterStateRequest clusterStateRequest = new ClusterStateRequest()
            .clear()
            .indices(AnomalyDetectionIndices.ALL_AD_RESULTS_INDEX_PATTERN)
            .metadata(true)
            .local(true)
            .indicesOptions(IndicesOptions.strictExpand());

        adminClient.cluster().state(clusterStateRequest, ActionListener.wrap(clusterStateResponse -> {
            String latestToDelete = null;
            long latest = Long.MIN_VALUE;
            for (ObjectCursor<IndexMetadata> cursor : clusterStateResponse.getState().metadata().indices().values()) {
                IndexMetadata indexMetaData = cursor.value;
                long creationTime = indexMetaData.getCreationDate();

                if ((Instant.now().toEpochMilli() - creationTime) > historyRetentionPeriod.millis()) {
                    String indexName = indexMetaData.getIndex().getName();
                    candidates.add(indexName);
                    if (latest < creationTime) {
                        latest = creationTime;
                        latestToDelete = indexName;
                    }
                }
            }

            if (candidates.size() > 1) {
                // delete all indices except the last one because the last one may contain docs newer than the retention period
                candidates.remove(latestToDelete);
                String[] toDelete = candidates.toArray(Strings.EMPTY_ARRAY);
                DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(toDelete);
                adminClient.indices().delete(deleteIndexRequest, ActionListener.wrap(deleteIndexResponse -> {
                    if (!deleteIndexResponse.isAcknowledged()) {
                        logger
                            .error(
                                "Could not delete one or more Anomaly result indices: {}. Retrying one by one.",
                                Arrays.toString(toDelete)
                            );
                        deleteIndexIteration(toDelete);
                    } else {
                        logger.info("Succeeded in deleting expired anomaly result indices: {}.", Arrays.toString(toDelete));
                    }
                }, exception -> {
                    logger.error("Failed to delete expired anomaly result indices: {}.", Arrays.toString(toDelete));
                    deleteIndexIteration(toDelete);
                }));
            }
        }, exception -> { logger.error("Fail to delete result indices", exception); }));
    }

    private void deleteIndexIteration(String[] toDelete) {
        for (String index : toDelete) {
            DeleteIndexRequest singleDeleteRequest = new DeleteIndexRequest(index);
            adminClient.indices().delete(singleDeleteRequest, ActionListener.wrap(singleDeleteResponse -> {
                if (!singleDeleteResponse.isAcknowledged()) {
                    logger.error("Retrying deleting {} does not succeed.", index);
                }
            }, exception -> {
                if (exception instanceof IndexNotFoundException) {
                    logger.info("{} was already deleted.", index);
                } else {
                    logger.error(new ParameterizedMessage("Retrying deleting {} does not succeed.", index), exception);
                }
            }));
        }
    }
}
