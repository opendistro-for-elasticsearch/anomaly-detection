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

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.Locale;

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.DETECTOR_ID;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.createXContentParser;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * This class consists of the REST handler to retrieve an anomaly detector.
 */
public class RestGetAnomalyDetectorAction extends BaseRestHandler {

    private static final String GET_ANOMALY_DETECTOR_ACTION = "get_anomaly_detector";
    private static final Logger logger = LogManager.getLogger(RestGetAnomalyDetectorAction.class);

    public RestGetAnomalyDetectorAction(RestController controller) {
        String path = String.format(Locale.ROOT, "%s/{%s}", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID);
        controller.registerHandler(RestRequest.Method.GET, path, this);
        controller.registerHandler(RestRequest.Method.HEAD, path, this);
    }

    @Override
    public String getName() {
        return GET_ANOMALY_DETECTOR_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String detectorId = request.param(DETECTOR_ID);
        boolean returnJob = request.paramAsBoolean("job", false);
        MultiGetRequest.Item adItem = new MultiGetRequest.Item(ANOMALY_DETECTORS_INDEX, detectorId)
            .version(RestActions.parseVersion(request));
        MultiGetRequest multiGetRequest = new MultiGetRequest().add(adItem);
        if (returnJob) {
            MultiGetRequest.Item adJobItem = new MultiGetRequest.Item(ANOMALY_DETECTOR_JOB_INDEX, detectorId)
                .version(RestActions.parseVersion(request));
            multiGetRequest.add(adJobItem);
        }

        return channel -> client.multiGet(multiGetRequest, onMultiGetResponse(channel, returnJob, detectorId));
    }

    private ActionListener<MultiGetResponse> onMultiGetResponse(RestChannel channel, boolean returnJob, String detectorId) {
        return new RestResponseListener<MultiGetResponse>(channel) {
            @Override
            public RestResponse buildResponse(MultiGetResponse multiGetResponse) throws Exception {
                MultiGetItemResponse[] responses = multiGetResponse.getResponses();
                XContentBuilder builder = null;
                AnomalyDetector detector = null;
                AnomalyDetectorJob adJob = null;
                for (MultiGetItemResponse response : responses) {
                    if (ANOMALY_DETECTORS_INDEX.equals(response.getIndex())) {
                        if (response.getResponse() == null || !response.getResponse().isExists()) {
                            return new BytesRestResponse(RestStatus.NOT_FOUND, "Can't find detector with id: " + detectorId);
                        }
                        builder = channel
                            .newBuilder()
                            .startObject()
                            .field(RestHandlerUtils._ID, response.getId())
                            .field(RestHandlerUtils._VERSION, response.getResponse().getVersion())
                            .field(RestHandlerUtils._PRIMARY_TERM, response.getResponse().getPrimaryTerm())
                            .field(RestHandlerUtils._SEQ_NO, response.getResponse().getSeqNo());
                        if (!response.getResponse().isSourceEmpty()) {
                            try (
                                XContentParser parser = RestHandlerUtils
                                    .createXContentParser(channel, response.getResponse().getSourceAsBytesRef())
                            ) {
                                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                                detector = parser.namedObject(AnomalyDetector.class, AnomalyDetector.PARSE_FIELD_NAME, null);
                            } catch (Throwable t) {
                                logger.error("Fail to parse detector", t);
                                return new BytesRestResponse(
                                    RestStatus.INTERNAL_SERVER_ERROR,
                                    "Failed to parse detector with id: " + detectorId
                                );
                            }
                        }
                    }

                    if (ANOMALY_DETECTOR_JOB_INDEX.equals(response.getIndex())) {
                        if (response.getResponse() != null
                            && response.getResponse().isExists()
                            && !response.getResponse().isSourceEmpty()) {
                            try (XContentParser parser = createXContentParser(channel, response.getResponse().getSourceAsBytesRef())) {
                                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                                adJob = AnomalyDetectorJob.parse(parser);
                            } catch (Throwable t) {
                                logger.error("Fail to parse detector job ", t);
                                return new BytesRestResponse(
                                    RestStatus.INTERNAL_SERVER_ERROR,
                                    "Failed to parse detector job with id: " + detectorId
                                );
                            }
                        }
                    }
                }

                builder.field(RestHandlerUtils.ANOMALY_DETECTOR, detector);
                if (returnJob) {
                    builder.field(RestHandlerUtils.ANOMALY_DETECTOR_JOB, adJob);
                }
                builder.endObject();
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        };
    }

}
