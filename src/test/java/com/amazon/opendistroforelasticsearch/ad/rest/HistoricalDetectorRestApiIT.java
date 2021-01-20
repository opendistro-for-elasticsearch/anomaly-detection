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

package com.amazon.opendistroforelasticsearch.ad.rest;

import static com.amazon.opendistroforelasticsearch.ad.TestHelpers.AD_BASE_STATS_URI;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.rest.RestStatus;

import com.amazon.opendistroforelasticsearch.ad.HistoricalDetectorRestTestCase;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskProfile;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskState;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.google.common.collect.ImmutableMap;

public class HistoricalDetectorRestApiIT extends HistoricalDetectorRestTestCase {

    public void testHistoricalDetectorWorkflow() throws Exception {
        // create historical detector
        AnomalyDetector detector = createHistoricalDetector();
        String detectorId = detector.getDetectorId();

        // start historical detector
        String taskId = startHistoricalDetector(detectorId);

        // get task stats
        Response statsResponse = TestHelpers.makeRequest(client(), "GET", AD_BASE_STATS_URI, ImmutableMap.of(), "", null);
        String statsResult = EntityUtils.toString(statsResponse.getEntity());
        assertTrue(statsResult.contains("\"ad_executing_batch_task_count\":1"));

        // get task profile
        ADTaskProfile adTaskProfile = getADTaskProfile(detectorId);
        ADTask adTask = adTaskProfile.getAdTask();
        assertEquals(taskId, adTask.getTaskId());
        assertTrue(TestHelpers.historicalDetectorRunningStats.contains(adTask.getState()));

        // get historical detector with AD task
        ToXContentObject[] result = getHistoricalAnomalyDetector(detectorId, true, client());
        AnomalyDetector parsedDetector = (AnomalyDetector) result[0];
        AnomalyDetectorJob parsedJob = (AnomalyDetectorJob) result[1];
        ADTask parsedADTask = (ADTask) result[2];
        assertNull(parsedJob);
        assertNotNull(parsedDetector);
        assertNotNull(parsedADTask);
        assertEquals(taskId, parsedADTask.getTaskId());

        // stop historical detector
        Response stopDetectorResponse = stopAnomalyDetector(detectorId, client());
        assertEquals(RestStatus.OK, restStatus(stopDetectorResponse));

        // get task profile
        ADTaskProfile stoppedAdTaskProfile = getADTaskProfile(detectorId);
        int i = 0;
        while (TestHelpers.historicalDetectorRunningStats.contains(stoppedAdTaskProfile.getAdTask().getState()) && i < 10) {
            stoppedAdTaskProfile = getADTaskProfile(detectorId);
            Thread.sleep(2000);
            i++;
        }
        ADTask stoppedAdTask = stoppedAdTaskProfile.getAdTask();
        assertEquals(taskId, stoppedAdTask.getTaskId());
        assertEquals(ADTaskState.STOPPED.name(), stoppedAdTask.getState());
        updateClusterSettings(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1);
    }

}
