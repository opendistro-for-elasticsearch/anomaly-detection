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

import com.amazon.opendistroforelasticsearch.ad.ml.HybridThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelState;
import com.amazon.opendistroforelasticsearch.ad.util.IndexUtils;
import com.amazon.randomcutforest.RandomCutForest;
import org.elasticsearch.test.ESTestCase;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ADStatsTests extends ESTestCase {

    private ADStats adStats;
    private RandomCutForest rcf;
    private HybridThresholdingModel thresholdingModel;

    @Mock
    private Clock clock;

    @Mock
    private ModelManager modelManager;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        rcf = RandomCutForest.builder().dimensions(1).sampleSize(1).numberOfTrees(1).build();
        thresholdingModel = new HybridThresholdingModel(1e-8, 1e-5, 200,
                10_000, 2, 5_000_000);

        List<ModelState<?>> modelsInformation = new ArrayList<>(Arrays.asList(
                new ModelState<>(rcf, "rcf-model-1", "detector-1",
                        ModelManager.ModelType.RCF.getName(), clock.instant()),
                new ModelState<>(thresholdingModel,"thr-model-1",  "detector-1",
                        ModelManager.ModelType.RCF.getName(), clock.instant()),
                new ModelState<>(rcf, "rcf-model-2",  "detector-2",
                        ModelManager.ModelType.THRESHOLD.getName(), clock.instant()),
                new ModelState<>(thresholdingModel,"thr-model-2", "detector-2",
                        ModelManager.ModelType.THRESHOLD.getName(), clock.instant())
        ));

        when(modelManager.getAllModels()).thenReturn(modelsInformation);
        IndexUtils indexUtils = mock(IndexUtils.class);

        when(indexUtils.getIndexHealthStatus(anyString())).thenReturn("yellow");
        when(indexUtils.getNumberOfDocumentsInIndex(anyString())).thenReturn(100L);
        adStats = new ADStats(indexUtils, modelManager);
    }

    @Test
    public void testStatNamesGetNames() {
        assertEquals("getNames of StatNames returns the incorrect number of stats",
                ADStats.StatNames.getNames().size(), ADStats.StatNames.values().length);
    }

    @Test
    public void testGetStats() {
        Map<String, ADStat<?>> stats = adStats.getStats();

        assertEquals("getStats returns the incorrect number of stats",
                stats.size(), ADStats.StatNames.values().length);

        for (Map.Entry<String, ADStat<?>> stat : stats.entrySet()) {
            assertTrue("getStats returns incorrect stats",
                    adStats.getStats().containsKey(stat.getKey()) &&
                            adStats.getStats().get(stat.getKey()) == stat.getValue());
        }
    }

    @Test
    public void testGetStat() {
        ADStat<?> stat = adStats.getStat(ADStats.StatNames.AD_EXECUTE_REQUEST_COUNT.getName());

        assertTrue("getStat returns incorrect stat",
                adStats.getStats().containsKey(stat.getName()) &&
                        adStats.getStats().get(stat.getName()) == stat);
    }

    @Test
    public void testGetNodeStats() {
        Map<String, ADStat<?>> stats = adStats.getStats();
        Set<ADStat<?>> nodeStats = new HashSet<>(adStats.getNodeStats().values());

        for (ADStat<?> stat : stats.values()) {
            assertTrue("getNodeStats returns incorrect stats", stat.isClusterLevel() || nodeStats.contains(stat));
        }
    }

    @Test
    public void testGetClusterStats() {
        Map<String, ADStat<?>> stats = adStats.getStats();
        Set<ADStat<?>> clusterStats = new HashSet<>(adStats.getClusterStats().values());

        for (ADStat<?> stat : stats.values()) {
            assertTrue("getNodeStats returns incorrect stats", !stat.isClusterLevel() || clusterStats.contains(stat));
        }
    }
}
