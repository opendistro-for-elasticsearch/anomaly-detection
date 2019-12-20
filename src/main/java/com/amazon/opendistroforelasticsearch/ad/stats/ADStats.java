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

package com.amazon.opendistroforelasticsearch.ad.stats;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.stats.counters.BasicCounter;
import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.CounterSupplier;
import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.DocumentCountSupplier;
import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.IndexStatusSupplier;
import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.ModelsOnNodeSupplier;
import com.amazon.opendistroforelasticsearch.ad.util.IndexUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ADStats
 *
 * This class is the main entry-point for access to the stats that the AD plugin keeps track of.
 */
public class ADStats {

    private static ADStats adStats = null;

    private IndexUtils indexUtils;
    private ModelManager modelManager;
    private Map<String, ADStat<?>> stats;

    /**
     * getInstance
     *
     * @param indexUtils utility to get information about indices
     * @param modelManager used to get information about which models are hosted on a particular node
     * @return ADStats instance
     */
    public static ADStats getInstance(IndexUtils indexUtils, ModelManager modelManager) {
        if (adStats == null) {
            adStats = new ADStats(indexUtils, modelManager);
        }
        return adStats;
    }

    /**
     * Enum containing names of all stats
     */
    public enum StatNames {
        AD_EXECUTE_REQUEST_COUNT("ad_execute_request_count"),
        AD_EXECUTE_FAIL_COUNT("ad_execute_failure_count"),
        DETECTOR_COUNT("detector_count"),
        ANOMALY_DETECTORS_INDEX_STATUS("anomaly_detectors_index_status"),
        ANOMALY_RESULTS_INDEX_STATUS("anomaly_results_index_status"),
        MODELS_CHECKPOINT_INDEX_STATUS("models_checkpoint_index_status"),
        MODEL_INFORMATION("models");

        private String name;

        StatNames(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public static List<String> getNames() {
            List<String> names = new ArrayList<>();

            for (StatNames statName : StatNames.values()) {
                names.add(statName.getName());
            }
            return names;
        }
    }

    /**
     * ADStats constructor
     *
     * @param indexUtils utility to get information about indices
     * @param modelManager used to get information about which models are hosted on a particular node
     */
    private ADStats(IndexUtils indexUtils, ModelManager modelManager) {
        this.indexUtils = indexUtils;
        this.modelManager = modelManager;
        initStats();
    }

    /**
     * Initialize the map that keeps track of all of the stats
     */
    private void initStats() {
        stats = new HashMap<String, ADStat<?>>() {
            {
                // Stateful Node stats
                put(StatNames.AD_EXECUTE_REQUEST_COUNT.getName(), new ADStat<>(
                        StatNames.AD_EXECUTE_REQUEST_COUNT.getName(), false,
                        new CounterSupplier(new BasicCounter())));
                put(StatNames.AD_EXECUTE_FAIL_COUNT.getName(), new ADStat<>(StatNames.AD_EXECUTE_FAIL_COUNT.getName(),
                        false, new CounterSupplier(new BasicCounter())));

                // Stateless Node stats
                put(StatNames.MODEL_INFORMATION.getName(), new ADStat<>(StatNames.MODEL_INFORMATION.getName(),
                        false, new ModelsOnNodeSupplier(modelManager)));

                // Stateless Cluster stats
                put(StatNames.ANOMALY_DETECTORS_INDEX_STATUS.getName(), new ADStat<>(
                        StatNames.ANOMALY_DETECTORS_INDEX_STATUS.getName(), true,
                        new IndexStatusSupplier(indexUtils, AnomalyDetector.ANOMALY_DETECTORS_INDEX)));
                put(StatNames.ANOMALY_RESULTS_INDEX_STATUS.getName(),
                        new ADStat<>(StatNames.ANOMALY_RESULTS_INDEX_STATUS.getName(), true,
                                new IndexStatusSupplier(indexUtils, AnomalyResult.ANOMALY_RESULT_INDEX)));
                put(StatNames.MODELS_CHECKPOINT_INDEX_STATUS.getName(),
                        new ADStat<>(StatNames.MODELS_CHECKPOINT_INDEX_STATUS.getName(), true,
                                new IndexStatusSupplier(indexUtils, CommonName.CHECKPOINT_INDEX_NAME)));
                put(StatNames.DETECTOR_COUNT.getName(),
                        new ADStat<>(StatNames.DETECTOR_COUNT.getName(), true,
                                new DocumentCountSupplier(indexUtils, AnomalyDetector.ANOMALY_DETECTORS_INDEX)));
            }
        };
    }

    /**
     * Get the stats
     *
     * @return all of the stats
     */
    public Map<String, ADStat<?>> getStats() {
        return stats;
    }

    /**
     * Get individual statName
     *
     * @param key Name of stat
     * @return ADStat
     * @throws IllegalArgumentException thrown on illegal statName
     */
    public ADStat<?> getStat(String key) throws IllegalArgumentException {
        if (!stats.keySet().contains(key)) {
            throw new IllegalArgumentException("Stat=\"" + key + "\" does not exist");
        }
        return stats.get(key);
    }

    /**
     * Get a map of the stats that are kept at the node level
     *
     * @return Map of stats kept at the node level
     */
    public Map<String, ADStat<?>> getNodeStats() {
        Map<String, ADStat<?>> nodeStats = new HashMap<>();

        for (Map.Entry<String, ADStat<?>> entry : stats.entrySet()) {
            if (!entry.getValue().isClusterLevel()) {
                nodeStats.put(entry.getKey(), entry.getValue());
            }
        }
        return nodeStats;
    }

    /**
     * Get a map of the stats that are kept at the cluster level
     *
     * @return Map of stats kept at the cluster level
     */
    public Map<String, ADStat<?>> getClusterStats() {
        Map<String, ADStat<?>> clusterStats = new HashMap<>();

        for (Map.Entry<String, ADStat<?>> entry : stats.entrySet()) {
            if (entry.getValue().isClusterLevel()) {
                clusterStats.put(entry.getKey(), entry.getValue());
            }
        }
        return clusterStats;
    }
}
