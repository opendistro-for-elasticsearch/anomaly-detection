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
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.CHECKPOINT_INDEX_MAPPING_FILE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_PRIMARY_SHARDS;

import java.io.IOException;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonValue;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorInternalState;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

/**
 * This class provides utility methods for various anomaly detection indices.
 */
public class AnomalyDetectionIndices implements LocalNodeMasterListener {
    private static final Logger logger = LogManager.getLogger(AnomalyDetectionIndices.class);

    // The index name pattern to query all the AD result history indices
    public static final String AD_RESULT_HISTORY_INDEX_PATTERN = "<.opendistro-anomaly-results-history-{now/d}-1>";

    // The index name pattern to query all AD result, history and current AD result
    public static final String ALL_AD_RESULTS_INDEX_PATTERN = ".opendistro-anomaly-results*";

    private static final String META = "_meta";
    private static final String SCHEMA_VERSION = "schema_version";

    private ClusterService clusterService;
    private final AdminClient adminClient;
    private final ThreadPool threadPool;

    private volatile TimeValue historyRolloverPeriod;
    private volatile Long historyMaxDocs;
    private volatile TimeValue historyRetentionPeriod;

    private Scheduler.Cancellable scheduledRollover = null;

    private DiscoveryNodeFilterer nodeFilter;
    private int maxPrimaryShards;
    // keep track of whether the mapping version is up-to-date
    private EnumMap<ADIndex, IndexState> indexStates;
    // whether all index have the correct mappings
    private boolean allUpdated;
    // we only want one update at a time
    private final AtomicBoolean updateRunning;

    class IndexState {
        // keep track of whether the mapping version is up-to-date
        private Boolean updated;
        // record schema version reading from the mapping file
        private Integer schemaVersion;

        IndexState(ADIndex index) {
            this.updated = false;
            this.schemaVersion = parseSchemaVersion(index.getMapping());
        }
    }

    /**
     * Constructor function
     *
     * @param client         ES client supports administrative actions
     * @param clusterService ES cluster service
     * @param threadPool     ES thread pool
     * @param settings       ES cluster setting
     * @param nodeFilter     Used to filter eligible nodes to host AD indices
     */
    public AnomalyDetectionIndices(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        Settings settings,
        DiscoveryNodeFilterer nodeFilter
    ) {
        this.adminClient = client.admin();
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.clusterService.addLocalNodeMasterListener(this);
        this.historyRolloverPeriod = AD_RESULT_HISTORY_ROLLOVER_PERIOD.get(settings);
        this.historyMaxDocs = AD_RESULT_HISTORY_MAX_DOCS.get(settings);
        this.historyRetentionPeriod = AD_RESULT_HISTORY_RETENTION_PERIOD.get(settings);
        this.maxPrimaryShards = MAX_PRIMARY_SHARDS.get(settings);

        this.nodeFilter = nodeFilter;

        this.indexStates = new EnumMap<ADIndex, IndexState>(ADIndex.class);

        this.allUpdated = false;
        this.updateRunning = new AtomicBoolean(false);

        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_RESULT_HISTORY_MAX_DOCS, it -> historyMaxDocs = it);

        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_RESULT_HISTORY_ROLLOVER_PERIOD, it -> {
            historyRolloverPeriod = it;
            rescheduleRollover();
        });
        this.clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(AD_RESULT_HISTORY_RETENTION_PERIOD, it -> { historyRetentionPeriod = it; });

        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_PRIMARY_SHARDS, it -> maxPrimaryShards = it);
    }

    /**
     * Get anomaly detector index mapping json content.
     *
     * @return anomaly detector index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getAnomalyDetectorMappings() throws IOException {
        URL url = AnomalyDetectionIndices.class.getClassLoader().getResource(ANOMALY_DETECTORS_INDEX_MAPPING_FILE);
        return Resources.toString(url, Charsets.UTF_8);
    }

    /**
     * Get anomaly result index mapping json content.
     *
     * @return anomaly result index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getAnomalyResultMappings() throws IOException {
        URL url = AnomalyDetectionIndices.class.getClassLoader().getResource(ANOMALY_RESULTS_INDEX_MAPPING_FILE);
        return Resources.toString(url, Charsets.UTF_8);
    }

    /**
     * Get anomaly detector job index mapping json content.
     *
     * @return anomaly detector job index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getAnomalyDetectorJobMappings() throws IOException {
        URL url = AnomalyDetectionIndices.class.getClassLoader().getResource(ANOMALY_DETECTOR_JOBS_INDEX_MAPPING_FILE);
        return Resources.toString(url, Charsets.UTF_8);
    }

    /**
     * Get anomaly detector state index mapping json content.
     *
     * @return anomaly detector state index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getDetectorStateMappings() throws IOException {
        URL url = AnomalyDetectionIndices.class.getClassLoader().getResource(ANOMALY_DETECTION_STATE_INDEX_MAPPING_FILE);
        return Resources.toString(url, Charsets.UTF_8);
    }

    /**
     * Get checkpoint index mapping json content.
     *
     * @return checkpoint index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getCheckpointMappings() throws IOException {
        URL url = AnomalyDetectionIndices.class.getClassLoader().getResource(CHECKPOINT_INDEX_MAPPING_FILE);
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
     * anomaly result index exist or not.
     *
     * @return true if anomaly result index exists
     */
    public boolean doesAnomalyResultIndexExist() {
        return clusterService.state().metadata().hasAlias(CommonName.ANOMALY_RESULT_INDEX_ALIAS);
    }

    /**
     * Anomaly state index exist or not.
     *
     * @return true if anomaly state index exists
     */
    public boolean doesDetectorStateIndexExist() {
        return clusterService.state().getRoutingTable().hasIndex(DetectorInternalState.DETECTOR_STATE_INDEX);
    }

    /**
     * Checkpoint index exist or not.
     *
     * @return true if checkpoint index exists
     */
    public boolean doesCheckpointIndexExist() {
        return clusterService.state().getRoutingTable().hasIndex(CommonName.CHECKPOINT_INDEX_NAME);
    }

    /**
     * Index exists or not
     * @param clusterServiceAccessor Cluster service
     * @param name Index name
     * @return true if the index exists
     */
    public static boolean doesIndexExists(ClusterService clusterServiceAccessor, String name) {
        return clusterServiceAccessor.state().getRoutingTable().hasIndex(name);
    }

    /**
     * Alias exists or not
     * @param clusterServiceAccessor Cluster service
     * @param alias Alias name
     * @return true if the alias exists
     */
    public static boolean doesAliasExists(ClusterService clusterServiceAccessor, String alias) {
        return clusterServiceAccessor.state().metadata().hasAlias(alias);
    }

    private ActionListener<CreateIndexResponse> markMappingUpToDate(ADIndex index, ActionListener<CreateIndexResponse> followingListener) {
        return ActionListener.wrap(createdResponse -> {
            if (createdResponse.isAcknowledged()) {
                IndexState indexStatetate = indexStates.computeIfAbsent(index, IndexState::new);
                if (Boolean.FALSE.equals(indexStatetate.updated)) {
                    indexStatetate.updated = Boolean.TRUE;
                    logger.info(new ParameterizedMessage("Mark [{}]'s mapping up-to-date", index.getIndexName()));
                }
            }
            followingListener.onResponse(createdResponse);
        }, exception -> followingListener.onFailure(exception));
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
        adminClient.indices().create(request, markMappingUpToDate(ADIndex.CONFIG, actionListener));
    }

    /**
     * Create anomaly result index if not exist.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link AnomalyDetectionIndices#getAnomalyResultMappings}
     */
    public void initAnomalyResultIndexIfAbsent(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        if (!doesAnomalyResultIndexExist()) {
            initAnomalyResultIndexDirectly(actionListener);
        }
    }

    /**
     * choose the number of primary shards for checkpoint, multientity result, and job scheduler based on the number of hot nodes. Max 10.
     * @param request The request to add the setting
     */
    private void choosePrimaryShards(CreateIndexRequest request) {
        request
            .settings(
                Settings
                    .builder()
                    // put 1 primary shards per hot node if possible
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, Math.min(nodeFilter.getNumberOfEligibleDataNodes(), maxPrimaryShards))
                    // 1 replica for better search performance and fail-over
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            );
    }

    /**
     * Create anomaly result index without checking exist or not.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link AnomalyDetectionIndices#getAnomalyResultMappings}
     */
    public void initAnomalyResultIndexDirectly(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        String mapping = getAnomalyResultMappings();
        CreateIndexRequest request = new CreateIndexRequest(AD_RESULT_HISTORY_INDEX_PATTERN)
            .mapping(CommonName.MAPPING_TYPE, mapping, XContentType.JSON)
            .alias(new Alias(CommonName.ANOMALY_RESULT_INDEX_ALIAS));
        choosePrimaryShards(request);
        adminClient.indices().create(request, markMappingUpToDate(ADIndex.RESULT, actionListener));
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
        choosePrimaryShards(request);
        adminClient.indices().create(request, markMappingUpToDate(ADIndex.JOB, actionListener));
    }

    /**
     * Create the state index.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link AnomalyDetectionIndices#getDetectorStateMappings}
     */
    public void initDetectorStateIndex(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(DetectorInternalState.DETECTOR_STATE_INDEX)
            .mapping(AnomalyDetector.TYPE, getDetectorStateMappings(), XContentType.JSON);
        adminClient.indices().create(request, markMappingUpToDate(ADIndex.STATE, actionListener));
    }

    /**
     * Create the checkpoint index.
     *
     * @param actionListener action called after create index
     * @throws EndRunException EndRunException due to failure to get mapping
     */
    public void initCheckpointIndex(ActionListener<CreateIndexResponse> actionListener) {
        String mapping;
        try {
            mapping = getCheckpointMappings();
        } catch (IOException e) {
            throw new EndRunException("", "Cannot find checkpoint mapping file", true);
        }
        CreateIndexRequest request = new CreateIndexRequest(CommonName.CHECKPOINT_INDEX_NAME)
            .mapping(CommonName.MAPPING_TYPE, mapping, XContentType.JSON);
        choosePrimaryShards(request);
        adminClient.indices().create(request, markMappingUpToDate(ADIndex.CHECKPOINT, actionListener));
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

    private String executorName() {
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
        request
            .getCreateIndexRequest()
            .index(AD_RESULT_HISTORY_INDEX_PATTERN)
            .mapping(CommonName.MAPPING_TYPE, adResultMapping, XContentType.JSON);
        request.addMaxIndexDocsCondition(historyMaxDocs);
        adminClient.indices().rolloverIndex(request, ActionListener.wrap(response -> {
            if (!response.isRolledOver()) {
                logger
                    .warn("{} not rolled over. Conditions were: {}", CommonName.ANOMALY_RESULT_INDEX_ALIAS, response.getConditionStatus());
            } else {
                IndexState indexStatetate = indexStates.computeIfAbsent(ADIndex.RESULT, IndexState::new);
                indexStatetate.updated = true;
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

    /**
     * Update mapping if schema version changes.
     */
    public void updateMappingIfNecessary() {
        if (allUpdated || updateRunning.get()) {
            return;
        }

        updateRunning.set(true);

        List<ADIndex> updates = new ArrayList<>();
        for (ADIndex index : ADIndex.values()) {
            Boolean updated = indexStates.computeIfAbsent(index, IndexState::new).updated;
            if (Boolean.FALSE.equals(updated)) {
                updates.add(index);
            }
        }
        if (updates.size() == 0) {
            allUpdated = true;
            updateRunning.set(false);
            return;
        }

        final GroupedActionListener<Void> conglomerateListeneer = new GroupedActionListener<>(
            ActionListener.wrap(r -> updateRunning.set(false), exception -> {
                // TODO: don't retry endlessly. Can be annoying if there are too many exception logs.
                updateRunning.set(false);
                logger.error("Fail to update AD indices' mappings");
            }),
            updates.size()
        );

        for (ADIndex adIndex : updates) {
            logger.info(new ParameterizedMessage("Check [{}]'s mapping", adIndex.getIndexName()));
            shouldUpdateIndex(adIndex, ActionListener.wrap(shouldUpdate -> {
                if (shouldUpdate) {
                    adminClient
                        .indices()
                        .putMapping(
                            new PutMappingRequest()
                                .indices(adIndex.getIndexName())
                                .type(CommonName.MAPPING_TYPE)
                                .source(adIndex.getMapping(), XContentType.JSON),
                            ActionListener.wrap(putMappingResponse -> {
                                if (putMappingResponse.isAcknowledged()) {
                                    logger.info(new ParameterizedMessage("Succeeded in updating [{}]'s mapping", adIndex.getIndexName()));
                                    markMappingUpdated(adIndex);
                                } else {
                                    logger.error(new ParameterizedMessage("Fail to update [{}]'s mapping", adIndex.getIndexName()));
                                }
                                conglomerateListeneer.onResponse(null);
                            }, exception -> {
                                logger
                                    .error(
                                        new ParameterizedMessage(
                                            "Fail to update [{}]'s mapping due to [{}]",
                                            adIndex.getIndexName(),
                                            exception.getMessage()
                                        )
                                    );
                                conglomerateListeneer.onFailure(exception);
                            })
                        );
                } else {
                    // index does not exist or the version is already up-to-date.
                    // When creating index, new mappings will be used.
                    // We don't need to update it.
                    logger.info(new ParameterizedMessage("We don't need to update [{}]'s mapping", adIndex.getIndexName()));
                    markMappingUpdated(adIndex);
                    conglomerateListeneer.onResponse(null);
                }
            }, exception -> {
                logger
                    .error(
                        new ParameterizedMessage("Fail to check whether we should update [{}]'s mapping", adIndex.getIndexName()),
                        exception
                    );
                conglomerateListeneer.onFailure(exception);
            }));

        }
    }

    private void markMappingUpdated(ADIndex adIndex) {
        IndexState indexState = indexStates.computeIfAbsent(adIndex, IndexState::new);
        if (Boolean.FALSE.equals(indexState.updated)) {
            indexState.updated = Boolean.TRUE;
            logger.info(new ParameterizedMessage("Mark [{}]'s mapping up-to-date", adIndex.getIndexName()));
        }
    }

    private void shouldUpdateIndex(ADIndex index, ActionListener<Boolean> thenDo) {
        boolean exists = false;
        if (index.isAlias()) {
            exists = AnomalyDetectionIndices.doesAliasExists(clusterService, index.getIndexName());
        } else {
            exists = AnomalyDetectionIndices.doesIndexExists(clusterService, index.getIndexName());
        }
        if (false == exists) {
            thenDo.onResponse(Boolean.FALSE);
            return;
        }

        Integer newVersion = indexStates.computeIfAbsent(index, IndexState::new).schemaVersion;
        if (index.isAlias()) {
            GetAliasesRequest getAliasRequest = new GetAliasesRequest()
                .aliases(index.getIndexName())
                .indicesOptions(IndicesOptions.lenientExpandOpenHidden());
            adminClient.indices().getAliases(getAliasRequest, ActionListener.wrap(getAliasResponse -> {
                String concreteIndex = null;
                for (ObjectObjectCursor<String, List<AliasMetadata>> entry : getAliasResponse.getAliases()) {
                    if (false == entry.value.isEmpty()) {
                        // we assume the alias map to one concrete index, thus we can return after finding one
                        concreteIndex = entry.key;
                        break;
                    }
                }
                shouldUpdateConcreteIndex(concreteIndex, newVersion, thenDo);
            }, exception -> logger.error(new ParameterizedMessage("Fail to get [{}]'s alias", index.getIndexName()), exception)));
        } else {
            shouldUpdateConcreteIndex(index.getIndexName(), newVersion, thenDo);
        }
    }

    @SuppressWarnings("unchecked")
    private void shouldUpdateConcreteIndex(String concreteIndex, Integer newVersion, ActionListener<Boolean> thenDo) {
        IndexMetadata indexMeataData = clusterService.state().getMetadata().indices().get(concreteIndex);
        if (indexMeataData == null) {
            thenDo.onResponse(Boolean.FALSE);
            return;
        }
        Integer oldVersion = CommonValue.NO_SCHEMA_VERSION;

        Map<String, Object> indexMapping = indexMeataData.mapping().getSourceAsMap();
        Object meta = indexMapping.get(META);
        if (meta != null && meta instanceof Map) {
            Map<String, Object> metaMapping = (Map<String, Object>) meta;
            Object schemaVersion = metaMapping.get(CommonName.SCHEMA_VERSION_FIELD);
            if (schemaVersion instanceof Integer) {
                oldVersion = (Integer) schemaVersion;
            }
        }
        thenDo.onResponse(newVersion > oldVersion);
    }

    private static Integer parseSchemaVersion(String mapping) {
        try {
            XContentParser xcp = XContentType.JSON
                .xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, mapping);

            while (!xcp.isClosed()) {
                Token token = xcp.currentToken();
                if (token != null && token != XContentParser.Token.END_OBJECT && token != XContentParser.Token.START_OBJECT) {
                    if (xcp.currentName() != META) {
                        xcp.nextToken();
                        xcp.skipChildren();
                    } else {
                        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                            if (xcp.currentName().equals(SCHEMA_VERSION)) {

                                Integer version = xcp.intValue();
                                if (version < 0) {
                                    version = CommonValue.NO_SCHEMA_VERSION;
                                }
                                return version;
                            } else {
                                xcp.nextToken();
                            }
                        }

                    }
                }
                xcp.nextToken();
            }
            return CommonValue.NO_SCHEMA_VERSION;
        } catch (Exception e) {
            // since this method is called in the constructor that is called by AnomalyDetectorPlugin.createComponents,
            // we cannot throw checked exception
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * @param index Index metadata
     * @return The schema version of the given Index
     */
    public int getSchemaVersion(ADIndex index) {
        IndexState indexState = this.indexStates.computeIfAbsent(index, IndexState::new);
        return indexState.schemaVersion;
    }

    /**
     *
     * @param index Index metadata
     * @return Whether the given index's mapping is up-to-date
     */
    public Boolean isUpdated(ADIndex index) {
        IndexState indexState = this.indexStates.computeIfAbsent(index, IndexState::new);
        return indexState.updated;
    }
}
