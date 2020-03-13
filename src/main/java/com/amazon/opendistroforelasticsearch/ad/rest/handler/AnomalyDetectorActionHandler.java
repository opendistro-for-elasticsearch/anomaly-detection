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

package com.amazon.opendistroforelasticsearch.ad.rest.handler;

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Common handler to process AD request.
 */
public class AnomalyDetectorActionHandler {

    private final Logger logger = LogManager.getLogger(AnomalyDetectorActionHandler.class);

    /**
     * Get detector job for update/delete AD job.
     * If AD job exist, will return error message; otherwise, execute function.
     *
     * @param clusterService ES cluster service
     * @param client ES node client
     * @param detectorId detector identifier
     * @param channel ES rest channel
     * @param function AD function
     */
    public void getDetectorJob(
        ClusterService clusterService,
        NodeClient client,
        String detectorId,
        RestChannel channel,
        AnomalyDetectorFunction function
    ) {
        if (clusterService.state().getMetaData().indices().containsKey(ANOMALY_DETECTOR_JOB_INDEX)) {
            GetRequest request = new GetRequest(ANOMALY_DETECTOR_JOB_INDEX).id(detectorId);
            client.get(request, ActionListener.wrap(response -> onGetAdJobResponseForWrite(response, channel, function), exception -> {
                logger.error("Fail to get anomaly detector job: " + detectorId, exception);
                try {
                    channel.sendResponse(new BytesRestResponse(channel, exception));
                } catch (IOException e) {
                    logger.error("Fail to send exception" + detectorId, e);
                }
            }));
        } else {
            function.execute();
        }
    }

    private void onGetAdJobResponseForWrite(GetResponse response, RestChannel channel, AnomalyDetectorFunction function) {
        if (response.isExists()) {
            String adJobId = response.getId();
            if (adJobId != null) {
                // check if AD job is running on the detector, if yes, we can't delete the detector
                try (XContentParser parser = RestHandlerUtils.createXContentParser(channel, response.getSourceAsBytesRef())) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                    AnomalyDetectorJob adJob = AnomalyDetectorJob.parse(parser);
                    if (adJob.isEnabled()) {
                        channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, "Detector job is running: " + adJobId));
                        return;
                    }
                } catch (IOException e) {
                    String message = "Failed to parse anomaly detector job " + adJobId;
                    logger.error(message, e);
                    channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, message));
                }
            }
        }
        function.execute();
    }
}
