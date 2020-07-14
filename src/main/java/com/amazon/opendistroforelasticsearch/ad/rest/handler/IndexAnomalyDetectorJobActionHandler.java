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

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.XCONTENT_WITH_TYPE;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.createXContentParser;
import static org.elasticsearch.action.DocWriteResponse.Result.CREATED;
import static org.elasticsearch.action.DocWriteResponse.Result.UPDATED;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;

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

/**
 * Anomaly detector job REST action handler to process POST/PUT request.
 */
public class IndexAnomalyDetectorJobActionHandler extends AbstractActionHandler {

    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final String detectorId;
    private final Long seqNo;
    private final Long primaryTerm;

    private final Logger logger = LogManager.getLogger(IndexAnomalyDetectorJobActionHandler.class);
    private final TimeValue requestTimeout;

    /**
     * Constructor function.
     *
     * @param client                  ES node client that executes actions on the local node
     * @param channel                 ES channel used to construct bytes / builder based outputs, and send responses
     * @param anomalyDetectionIndices anomaly detector index manager
     * @param detectorId              detector identifier
     * @param seqNo                   sequence number of last modification
     * @param primaryTerm             primary term of last modification
     * @param requestTimeout          request time out configuration
     */
    public IndexAnomalyDetectorJobActionHandler(
        NodeClient client,
        RestChannel channel,
        AnomalyDetectionIndices anomalyDetectionIndices,
        String detectorId,
        Long seqNo,
        Long primaryTerm,
        TimeValue requestTimeout
    ) {
        super(client, channel);
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.detectorId = detectorId;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.requestTimeout = requestTimeout;
    }

    /**
     * Start anomaly detector job.
     * 1.If job not exists, create new job.
     * 2.If job exists: a). if job enabled, return error message; b). if job disabled, enable job.
     *
     * @throws IOException IOException from {@link AnomalyDetectionIndices#getAnomalyDetectorJobMappings}
     */
    public void startAnomalyDetectorJob() throws IOException {
        if (!anomalyDetectionIndices.doesAnomalyDetectorJobIndexExist()) {
            anomalyDetectionIndices
                .initAnomalyDetectorJobIndex(
                    ActionListener.wrap(response -> onCreateMappingsResponse(response), exception -> onFailure(exception))
                );
        } else {
            prepareAnomalyDetectorJobIndexing();
        }
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
        try (XContentParser parser = RestHandlerUtils.createXContentParser(channel, response.getSourceAsBytesRef())) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            AnomalyDetector detector = AnomalyDetector.parse(parser, response.getId(), response.getVersion());

            if (detector.getFeatureAttributes().size() == 0) {
                channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, "Can't start detector job as no features configured"));
                return;
            }

            IntervalTimeConfiguration interval = (IntervalTimeConfiguration) detector.getDetectionInterval();
            Schedule schedule = new IntervalSchedule(Instant.now(), (int) interval.getInterval(), interval.getUnit());
            Duration duration = Duration.of(interval.getInterval(), interval.getUnit());

            AnomalyDetectorJob job = new AnomalyDetectorJob(
                detector.getDetectorId(),
                schedule,
                detector.getWindowDelay(),
                true,
                Instant.now(),
                null,
                Instant.now(),
                duration.getSeconds()
            );

            getAnomalyDetectorJobForWrite(job);
        } catch (IOException e) {
            String message = "Failed to parse anomaly detector job " + detectorId;
            logger.error(message, e);
            channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, message));
        }
    }

    private void getAnomalyDetectorJobForWrite(AnomalyDetectorJob job) {
        GetRequest getRequest = new GetRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX).id(detectorId);

        client
            .get(
                getRequest,
                ActionListener.wrap(response -> onGetAnomalyDetectorJobForWrite(response, job), exception -> onFailure(exception))
            );
    }

    private void onGetAnomalyDetectorJobForWrite(GetResponse response, AnomalyDetectorJob job) throws IOException {
        if (response.isExists()) {
            try (XContentParser parser = createXContentParser(channel, response.getSourceAsBytesRef())) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                AnomalyDetectorJob currentAdJob = AnomalyDetectorJob.parse(parser);
                if (currentAdJob.isEnabled()) {
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, "Anomaly detector job is already running: " + detectorId));
                    return;
                } else {
                    AnomalyDetectorJob newJob = new AnomalyDetectorJob(
                        job.getName(),
                        job.getSchedule(),
                        job.getWindowDelay(),
                        job.isEnabled(),
                        Instant.now(),
                        currentAdJob.getDisabledTime(),
                        Instant.now(),
                        job.getLockDurationSeconds()
                    );
                    indexAnomalyDetectorJob(newJob, null);
                }
            } catch (IOException e) {
                String message = "Failed to parse anomaly detector job " + job.getName();
                logger.error(message, e);
                channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, message));
            }
        } else {
            indexAnomalyDetectorJob(job, null);
        }
    }

    private void indexAnomalyDetectorJob(AnomalyDetectorJob job, AnomalyDetectorFunction function) throws IOException {
        IndexRequest indexRequest = new IndexRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .source(job.toXContent(channel.newBuilder(), XCONTENT_WITH_TYPE))
            .setIfSeqNo(seqNo)
            .setIfPrimaryTerm(primaryTerm)
            .timeout(requestTimeout)
            .id(detectorId);
        client
            .index(
                indexRequest,
                ActionListener.wrap(response -> onIndexAnomalyDetectorJobResponse(response, function), exception -> onFailure(exception))
            );
    }

    private void onIndexAnomalyDetectorJobResponse(IndexResponse response, AnomalyDetectorFunction function) throws IOException {
        if (response == null || (response.getResult() != CREATED && response.getResult() != UPDATED)) {
            channel.sendResponse(new BytesRestResponse(response.status(), response.toXContent(channel.newErrorBuilder(), EMPTY_PARAMS)));
            return;
        }
        if (function != null) {
            function.execute();
        } else {
            XContentBuilder builder = channel
                .newBuilder()
                .startObject()
                .field(RestHandlerUtils._ID, response.getId())
                .field(RestHandlerUtils._VERSION, response.getVersion())
                .field(RestHandlerUtils._SEQ_NO, response.getSeqNo())
                .field(RestHandlerUtils._PRIMARY_TERM, response.getPrimaryTerm())
                .endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    /**
     * Stop anomaly detector job.
     * 1.If job not exists, return error message
     * 2.If job exists: a).if job state is disabled, return error message; b).if job state is enabled, disable job.
     *
     * @param detectorId detector identifier
     */
    public void stopAnomalyDetectorJob(String detectorId) {
        GetRequest getRequest = new GetRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX).id(detectorId);

        client.get(getRequest, ActionListener.wrap(response -> {
            if (response.isExists()) {
                try (XContentParser parser = createXContentParser(channel, response.getSourceAsBytesRef())) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                    AnomalyDetectorJob job = AnomalyDetectorJob.parse(parser);
                    if (!job.isEnabled()) {
                        channel
                            .sendResponse(new BytesRestResponse(RestStatus.OK, "Anomaly detector job is already stopped: " + detectorId));
                        return;
                    } else {
                        AnomalyDetectorJob newJob = new AnomalyDetectorJob(
                            job.getName(),
                            job.getSchedule(),
                            job.getWindowDelay(),
                            false,
                            job.getEnabledTime(),
                            Instant.now(),
                            Instant.now(),
                            job.getLockDurationSeconds()
                        );
                        indexAnomalyDetectorJob(
                            newJob,
                            () -> client
                                .execute(
                                    StopDetectorAction.INSTANCE,
                                    new StopDetectorRequest(detectorId),
                                    stopAdDetectorListener(channel, detectorId)
                                )
                        );
                    }
                } catch (IOException e) {
                    String message = "Failed to parse anomaly detector job " + detectorId;
                    logger.error(message, e);
                    channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, message));
                }
            } else {
                channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, "Anomaly detector job not exist: " + detectorId));
            }
        }, exception -> onFailure(exception)));
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
