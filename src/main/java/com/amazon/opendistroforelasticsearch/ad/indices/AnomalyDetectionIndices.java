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

package com.amazon.opendistroforelasticsearch.ad.indices;

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.URL;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.AD_RESULT_HISTORY_INDEX_MAX_AGE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.ANOMALY_DETECTORS_INDEX_MAPPING_FILE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.ANOMALY_DETECTOR_JOBS_INDEX_MAPPING_FILE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.ANOMALY_RESULTS_INDEX_MAPPING_FILE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.REQUEST_TIMEOUT;

/**
 * This class manages creation of anomaly detector index.
 */
public class AnomalyDetectionIndices implements LocalNodeMasterListener, ClusterStateListener {

    // The alias of the index in which to write AD result history
    public static final String AD_RESULT_HISTORY_WRITE_INDEX_ALIAS = AnomalyResult.ANOMALY_RESULT_INDEX;

    // The index name pattern to query all the AD result history indices
    public static final String AD_RESULT_HISTORY_INDEX_PATTERN = "<.opendistro-anomaly-results-history-{now/d}-1>";

    // The index name pattern to query all AD result, history and current AD result
    public static final String ALL_AD_RESULTS_INDEX_PATTERN = ".opendistro-anomaly-results*";

    // Elastic mapping type
    private static final String MAPPING_TYPE = "_doc";

    private ClusterService clusterService;
    private final AdminClient adminClient;
    private final Client client;
    private final ThreadPool threadPool;

    private volatile TimeValue requestTimeout;
    private volatile TimeValue historyMaxAge;
    private volatile TimeValue historyRolloverPeriod;
    private volatile Long historyMaxDocs;

    private Scheduler.Cancellable scheduledRollover = null;

    private static final Logger logger = LogManager.getLogger(AnomalyDetectionIndices.class);
    private TimeValue lastRolloverTime = null;
    private ClientUtil requestUtil;

    /**
     * Constructor function
     *
     * @param client         ES client supports administrative actions
     * @param clusterService ES cluster service
     * @param threadPool     ES thread pool
     * @param settings       ES cluster setting
     * @param requestUtil   wrapper to send a non-blocking timed request
     */
    public AnomalyDetectionIndices(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        Settings settings,
        ClientUtil requestUtil
    ) {
        this.client = client;
        this.adminClient = client.admin();
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.clusterService.addListener(this);
        this.clusterService.addLocalNodeMasterListener(this);
        this.requestTimeout = REQUEST_TIMEOUT.get(settings);
        this.historyMaxAge = AD_RESULT_HISTORY_INDEX_MAX_AGE.get(settings);
        this.historyRolloverPeriod = AD_RESULT_HISTORY_ROLLOVER_PERIOD.get(settings);
        this.historyMaxDocs = AD_RESULT_HISTORY_MAX_DOCS.get(settings);
        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_RESULT_HISTORY_MAX_DOCS, it -> historyMaxDocs = it);
        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_RESULT_HISTORY_INDEX_MAX_AGE, it -> historyMaxAge = it);
        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_RESULT_HISTORY_ROLLOVER_PERIOD, it -> {
            historyRolloverPeriod = it;
            rescheduleRollover();
        });
        clusterService.getClusterSettings().addSettingsUpdateConsumer(REQUEST_TIMEOUT, it -> requestTimeout = it);
        this.requestUtil = requestUtil;
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
        return clusterService.state().metaData().hasAlias(AnomalyResult.ANOMALY_RESULT_INDEX);
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
            .alias(new Alias(AD_RESULT_HISTORY_WRITE_INDEX_ALIAS));
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

    @Override
    public void onMaster() {
        try {
            // try to rollover immediately as we might be restarting the cluster
            rolloverHistoryIndex();
            // schedule the next rollover for approx MAX_AGE later
            scheduledRollover = threadPool.scheduleWithFixedDelay(() -> rolloverHistoryIndex(), historyRolloverPeriod, executorName());
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

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        boolean hasAdResultAlias = event.state().metaData().hasAlias(AD_RESULT_HISTORY_WRITE_INDEX_ALIAS);
    }

    private void rescheduleRollover() {
        if (clusterService.state().getNodes().isLocalNodeElectedMaster()) {
            if (scheduledRollover != null) {
                scheduledRollover.cancel();
            }
            scheduledRollover = threadPool.scheduleWithFixedDelay(() -> rolloverHistoryIndex(), historyRolloverPeriod, executorName());
        }
    }

    private boolean rolloverHistoryIndex() {
        if (!doesAnomalyResultIndexExist()) {
            return false;
        }

        // We have to pass null for newIndexName in order to get Elastic to increment the index count.
        RolloverRequest request = new RolloverRequest(AD_RESULT_HISTORY_WRITE_INDEX_ALIAS, null);
        String adResultMapping = null;
        try {
            adResultMapping = getAnomalyResultMappings();
        } catch (IOException e) {
            logger.error("Fail to roll over AD result index, as can't get AD result index mapping");
            return false;
        }
        request.getCreateIndexRequest().index(AD_RESULT_HISTORY_INDEX_PATTERN).mapping(MAPPING_TYPE, adResultMapping, XContentType.JSON);
        request.addMaxIndexDocsCondition(historyMaxDocs);
        request.addMaxIndexAgeCondition(historyMaxAge);
        RolloverResponse response = adminClient.indices().rolloversIndex(request).actionGet(requestTimeout);
        if (!response.isRolledOver()) {
            logger.warn("{} not rolled over. Conditions were: {}", AD_RESULT_HISTORY_WRITE_INDEX_ALIAS, response.getConditionStatus());
        } else {
            lastRolloverTime = TimeValue.timeValueMillis(threadPool.absoluteTimeInMillis());
        }
        return response.isRolledOver();
    }
}
