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

package com.amazon.opendistroforelasticsearch.ad.rest.handler;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.transport.StopDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.transport.StopDetectorRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.StopDetectorResponse;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.Schedule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.XCONTENT_WITH_TYPE;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Anomaly detector job REST action handler to process POST/PUT request.
 */
public class IndexAnomalyDetectorJobActionHandler extends AbstractActionHandler {

    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final String detectorId;
    private final Long seqNo;
    private final Long primaryTerm;
    private final WriteRequest.RefreshPolicy refreshPolicy;
    private final ClusterService clusterService;

    private final Logger logger = LogManager.getLogger(IndexAnomalyDetectorJobActionHandler.class);
    private final TimeValue requestTimeout;

    /**
     * Constructor function.
     *
     * @param clusterService          ClusterService
     * @param client                  ES node client that executes actions on the local node
     * @param channel                 ES channel used to construct bytes / builder based outputs, and send responses
     * @param anomalyDetectionIndices anomaly detector index manager
     * @param detectorId              detector identifier
     * @param seqNo                   sequence number of last modification
     * @param primaryTerm             primary term of last modification
     * @param refreshPolicy           refresh policy
     * @param requestTimeout          request time out configuration
     */
    public IndexAnomalyDetectorJobActionHandler(
        ClusterService clusterService,
        NodeClient client,
        RestChannel channel,
        AnomalyDetectionIndices anomalyDetectionIndices,
        String detectorId,
        Long seqNo,
        Long primaryTerm,
        WriteRequest.RefreshPolicy refreshPolicy,
        TimeValue requestTimeout
    ) {
        super(client, channel);
        this.clusterService = clusterService;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.detectorId = detectorId;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.refreshPolicy = refreshPolicy;
        this.requestTimeout = requestTimeout;
    }

    /**
     * Create anomaly detector job.
     *
     * @throws IOException IOException from {@link AnomalyDetectionIndices#getAnomalyDetectorJobMappings}
     */
    public void createAnomalyDetectorJob() throws IOException {
        if (!anomalyDetectionIndices.doesAnomalyDetectorJobIndexExist()) {
            anomalyDetectionIndices
                .initAnomalyDetectorJobIndex(
                    ActionListener.wrap(response -> onCreateMappingsResponse(response), exception -> onFailure(exception))
                );
        } else {
            prepareAnomalyDetectorJobIndexing();
        }
    }

    private void prepareAnomalyDetectorJobIndexing() {
        GetRequest getRequest = new GetRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX).id(detectorId);
        client.get(getRequest, ActionListener.wrap(response -> onGetAnomalyDetectorResponse(response), exception -> onFailure(exception)));
    }

    private void onGetAnomalyDetectorResponse(GetResponse response) throws IOException {
        if (!response.isExists()) {
            XContentBuilder builder = channel
                .newErrorBuilder()
                .startObject()
                .field("Message", "AnomalyDetector is not found with id: " + detectorId)
                .endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.NOT_FOUND, response.toXContent(builder, EMPTY_PARAMS)));
            return;
        }
        XContentParser parser = XContentType.JSON
            .xContent()
            .createParser(
                channel.request().getXContentRegistry(),
                LoggingDeprecationHandler.INSTANCE,
                response.getSourceAsBytesRef().streamInput()
            );

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        AnomalyDetector detector = AnomalyDetector.parse(parser, response.getId(), response.getVersion());

        IntervalTimeConfiguration interval = (IntervalTimeConfiguration) detector.getDetectionInterval();
        Schedule schedule = new IntervalSchedule(Instant.now(), (int) interval.getInterval(), interval.getUnit());
        Duration duration = Duration.of(interval.getInterval(), interval.getUnit());
        AnomalyDetectorJob job = new AnomalyDetectorJob(
            detector.getDetectorId(),
            schedule,
            true,
            Instant.now(),
            Instant.now(),
            duration.getSeconds()
        );

        getAnomalyDetectorJob(job);
    }

    private void getAnomalyDetectorJob(AnomalyDetectorJob job) {
        GetRequest getRequest = new GetRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX).id(detectorId);

        client.get(getRequest, ActionListener.wrap(response -> onGetAnomalyDetectorJob(response, job), exception -> onFailure(exception)));
    }

    private void onGetAnomalyDetectorJob(GetResponse response, AnomalyDetectorJob job) throws IOException {
        if (response.isExists()) {
            XContentBuilder builder = channel
                .newErrorBuilder()
                .startObject()
                .field("Message", "AnomalyDetectorJob exists: " + detectorId)
                .endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.NOT_FOUND, response.toXContent(builder, EMPTY_PARAMS)));
            return;
        }

        indexAnomalyDetectorJob(job);
    }

    private void indexAnomalyDetectorJob(AnomalyDetectorJob job) throws IOException {
        IndexRequest indexRequest = new IndexRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX)
            .setRefreshPolicy(refreshPolicy)
            .source(job.toXContent(channel.newBuilder(), XCONTENT_WITH_TYPE))
            .setIfSeqNo(seqNo)
            .setIfPrimaryTerm(primaryTerm)
            .timeout(requestTimeout)
            .id(detectorId);
        client.index(indexRequest, indexAnomalyDetectorJobResponse());
    }

    private ActionListener<IndexResponse> indexAnomalyDetectorJobResponse() {
        return new RestResponseListener<IndexResponse>(channel) {
            @Override
            public RestResponse buildResponse(IndexResponse response) throws Exception {
                if (response.getShardInfo().getSuccessful() < 1) {
                    return new BytesRestResponse(response.status(), response.toXContent(channel.newErrorBuilder(), EMPTY_PARAMS));
                }

                XContentBuilder builder = channel
                    .newBuilder()
                    .startObject()
                    .field(RestHandlerUtils._ID, response.getId())
                    .field(RestHandlerUtils._VERSION, response.getVersion())
                    .field(RestHandlerUtils._SEQ_NO, response.getSeqNo())
                    .field(RestHandlerUtils._PRIMARY_TERM, response.getPrimaryTerm())
                    .endObject();

                BytesRestResponse restResponse = new BytesRestResponse(response.status(), builder);
                if (response.status() == RestStatus.CREATED) {
                    String location = String.format(Locale.ROOT, "%s/%s", AnomalyDetectorPlugin.AD_BASE_URI, response.getId());
                    restResponse.addHeader("Location", location);
                }
                return restResponse;
            }
        };
    }

    private void onCreateMappingsResponse(CreateIndexResponse response) throws IOException {
        if (response.isAcknowledged()) {
            logger.info("Created {} with mappings.", ANOMALY_DETECTORS_INDEX);
            prepareAnomalyDetectorJobIndexing();
        } else {
            logger.warn("Created {} with mappings call not acknowledged.", ANOMALY_DETECTORS_INDEX);
            channel
                .sendResponse(
                    new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, response.toXContent(channel.newErrorBuilder(), EMPTY_PARAMS))
                );
        }
    }

    /**
     * Delete anomaly detector job
     * @param detectorId detector identifier
     */
    public void deleteAnomalyDetectorJob(String detectorId) {
        GetRequest getRequest = new GetRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX).id(detectorId);

        client
            .get(
                getRequest,
                ActionListener
                    .wrap(
                        response -> deleteAnomalyDetectorJobDoc(client, detectorId, channel, refreshPolicy),
                        exception -> onFailure(exception)
                    )
            );
    }

    private void deleteAnomalyDetectorJobDoc(
        NodeClient client,
        String detectorId,
        RestChannel channel,
        WriteRequest.RefreshPolicy refreshPolicy
    ) {
        logger.info("Delete anomaly detector job {}", detectorId);
        DeleteRequest deleteRequest = new DeleteRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX, detectorId)
            .setRefreshPolicy(refreshPolicy);
        client.delete(deleteRequest, ActionListener.wrap(response -> {
            if (response.getResult() == DocWriteResponse.Result.DELETED) {
                logger.info("Stop anomaly detector {}", detectorId);
                StopDetectorRequest stopDetectorRequest = new StopDetectorRequest(detectorId);
                client.execute(StopDetectorAction.INSTANCE, stopDetectorRequest, stopAdDetectorListener(channel, detectorId));
            } else if (response.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                logger.info("Anomaly detector job not found: {}", detectorId);
                StopDetectorRequest stopDetectorRequest = new StopDetectorRequest(detectorId);
                client.execute(StopDetectorAction.INSTANCE, stopDetectorRequest, stopAdDetectorListener(channel, detectorId));
            } else {
                channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, "Failed to stop AD job " + detectorId));
            }
        }, exception -> {
            logger.error("Failed to stop AD job " + detectorId, exception);
            onFailure(exception);
        }));

    }

    private ActionListener<StopDetectorResponse> stopAdDetectorListener(RestChannel channel, String detectorId) {
        return new ActionListener<StopDetectorResponse>() {
            @Override
            public void onResponse(StopDetectorResponse stopDetectorResponse) {
                if (stopDetectorResponse.success()) {
                    logger.info("AD model deleted successfully for detector {}", detectorId);
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, "Stopped detector: " + detectorId));
                } else {
                    logger.error("Failed to delete AD model for detector {}", detectorId);
                    channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, "Failed to delete AD model"));
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to delete AD model for detector " + detectorId, e);
                channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, "Failed to execute stop detector action"));
            }
        };
    }

}
