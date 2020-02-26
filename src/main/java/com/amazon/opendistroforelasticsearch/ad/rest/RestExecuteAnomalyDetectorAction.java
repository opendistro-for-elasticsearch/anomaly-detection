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

package com.amazon.opendistroforelasticsearch.ad.rest;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorRunner;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorExecutionInput;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyResultAction;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyResultRequest;
import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Locale;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.REQUEST_TIMEOUT;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.DETECTOR_ID;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.PREVIEW;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.RUN;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * This class consists of the REST handler to handle request to detect data.
 */
public class RestExecuteAnomalyDetectorAction extends BaseRestHandler {

    public static final String DETECT_DATA_ACTION = "execute_anomaly_detector";
    public static final String ANOMALY_RESULT = "anomaly_result";
    public static final String ANOMALY_DETECTOR = "anomaly_detector";
    private final AnomalyDetectorRunner anomalyDetectorRunner;
    // TODO: apply timeout config
    private volatile TimeValue requestTimeout;

    private final Logger logger = LogManager.getLogger(RestExecuteAnomalyDetectorAction.class);

    public RestExecuteAnomalyDetectorAction(
        Settings settings,
        RestController controller,
        ClusterService clusterService,
        AnomalyDetectorRunner anomalyDetectorRunner
    ) {
        this.anomalyDetectorRunner = anomalyDetectorRunner;
        this.requestTimeout = REQUEST_TIMEOUT.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(REQUEST_TIMEOUT, it -> requestTimeout = it);

        // get AD result, for regular run
        controller
            .registerHandler(
                RestRequest.Method.POST,
                String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID, RUN),
                this
            );

        // preivew AD
        controller
            .registerHandler(
                RestRequest.Method.POST,
                String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID, PREVIEW),
                this
            );
    }

    @Override
    public String getName() {
        return DETECT_DATA_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        AnomalyDetectorExecutionInput input = getAnomalyDetectorExecutionInput(request);
        return channel -> {
            String rawPath = request.rawPath();
            String error = validateAdExecutionInput(input);
            if (error != null) {
                channel.sendResponse(new BytesRestResponse(RestStatus.NOT_FOUND, error));
                return;
            }

            if (rawPath.endsWith(PREVIEW)) {
                preivewAnomalyDetector(client, channel, input);
            } else if (rawPath.endsWith(RUN)) {
                AnomalyResultRequest getRequest = new AnomalyResultRequest(
                    input.getDetectorId(),
                    input.getPeriodStart().toEpochMilli(),
                    input.getPeriodEnd().toEpochMilli()
                );
                client.execute(AnomalyResultAction.INSTANCE, getRequest, new RestToXContentListener<>(channel));
            }
        };
    }

    private AnomalyDetectorExecutionInput getAnomalyDetectorExecutionInput(RestRequest request) throws IOException {
        String detectorId = null;
        if (request.hasParam(DETECTOR_ID)) {
            detectorId = request.param(DETECTOR_ID);
        }

        XContentParser parser = request.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        AnomalyDetectorExecutionInput input = AnomalyDetectorExecutionInput.parse(parser);
        if (detectorId != null) {
            input.setDetectorId(detectorId);
        }
        return input;
    }

    private String validateAdExecutionInput(AnomalyDetectorExecutionInput input) {
        if (StringUtils.isBlank(input.getDetectorId())) {
            return "Must set anomaly detector id";
        }
        if (input.getPeriodStart() == null || input.getPeriodEnd() == null) {
            return "Must set both period start and end date with epoch of milliseconds";
        }
        if (!input.getPeriodStart().isBefore(input.getPeriodEnd())) {
            return "Period start date should be before end date";
        }
        return null;
    }

    private void preivewAnomalyDetector(NodeClient client, RestChannel channel, AnomalyDetectorExecutionInput input) {
        if (!StringUtils.isBlank(input.getDetectorId())) {
            GetRequest getRequest = new GetRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX).id(input.getDetectorId());
            client.get(getRequest, onGetAnomalyDetectorResponse(channel, input));
        } else {
            channel.sendResponse(new BytesRestResponse(RestStatus.NOT_FOUND, "Wrong input, no detector id"));
        }
    }

    private RestActionListener<GetResponse> onGetAnomalyDetectorResponse(RestChannel channel, AnomalyDetectorExecutionInput input) {
        return new RestActionListener<GetResponse>(channel) {
            @Override
            protected void processResponse(GetResponse response) throws Exception {
                if (!response.isExists()) {
                    XContentBuilder message = channel
                        .newErrorBuilder()
                        .startObject()
                        .field("message", "Can't find anomaly detector with id:" + response.getId())
                        .endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.NOT_FOUND, message));
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

                anomalyDetectorRunner.run(detector, input.getPeriodStart(), input.getPeriodEnd(), ActionListener.wrap(anomalyResult -> {
                    XContentBuilder builder = channel
                        .newBuilder()
                        .startObject()
                        .field(ANOMALY_RESULT, anomalyResult)
                        .field(ANOMALY_DETECTOR, detector)
                        .endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                }, exception -> {
                    logger.error("Unexpected error running anomaly detector " + detector.getDetectorId(), exception);
                    try {
                        XContentBuilder builder = channel.newBuilder().startObject().field(ANOMALY_DETECTOR, detector).endObject();
                        channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                    } catch (IOException e) {
                        logger.error("Fail to send back exception message" + detector.getDetectorId(), exception);
                    }
                }));
            }
        };
    }
}
