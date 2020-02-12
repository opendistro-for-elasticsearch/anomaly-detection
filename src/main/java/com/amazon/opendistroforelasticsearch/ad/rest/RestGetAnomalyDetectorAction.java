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
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
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
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.DETECTOR_ID;
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
        GetRequest getRequest = new GetRequest(ANOMALY_DETECTORS_INDEX, detectorId)
            .version(RestActions.parseVersion(request))
            .fetchSourceContext(RestHandlerUtils.getSourceContext(request));
        return channel -> client.get(getRequest, getDetectorResponse(channel));
    }

    private RestResponseListener<GetResponse> getDetectorResponse(RestChannel channel) {
        return new RestResponseListener<GetResponse>(channel) {

            @Override
            public RestResponse buildResponse(GetResponse response) throws Exception {
                if (!response.isExists()) {
                    return new BytesRestResponse(RestStatus.NOT_FOUND, channel.newBuilder());
                }

                XContentBuilder builder = channel
                    .newBuilder()
                    .startObject()
                    .field(RestHandlerUtils._ID, response.getId())
                    .field(RestHandlerUtils._VERSION, response.getVersion())
                    .field(RestHandlerUtils._PRIMARY_TERM, response.getPrimaryTerm())
                    .field(RestHandlerUtils._SEQ_NO, response.getSeqNo());
                if (!response.isSourceEmpty()) {
                    XContentParser parser = XContentHelper
                        .createParser(
                            channel.request().getXContentRegistry(),
                            LoggingDeprecationHandler.INSTANCE,
                            response.getSourceAsBytesRef(),
                            XContentType.JSON
                        );
                    try {
                        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                        AnomalyDetector detector = parser.namedObject(AnomalyDetector.class, AnomalyDetector.PARSE_FIELD_NAME, null);
                        builder.field(RestHandlerUtils.ANOMALY_DETECTOR, detector);
                    } catch (Throwable t) {
                        logger.error("Fail to parse detector", t);
                        throw t;
                    } finally {
                        parser.close();
                    }
                }
                builder.endObject();
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        };
    }

}
