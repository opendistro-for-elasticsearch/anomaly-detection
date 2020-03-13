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

package com.amazon.opendistroforelasticsearch.ad.util;

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;

/**
 * Utility functions for REST handlers.
 */
public final class RestHandlerUtils {

    public static final String _ID = "_id";
    public static final String _VERSION = "_version";
    public static final String _SEQ_NO = "_seq_no";
    public static final String IF_SEQ_NO = "if_seq_no";
    public static final String _PRIMARY_TERM = "_primary_term";
    public static final String IF_PRIMARY_TERM = "if_primary_term";
    public static final String REFRESH = "refresh";
    public static final String DETECTOR_ID = "detectorID";
    public static final String ANOMALY_DETECTOR = "anomaly_detector";
    public static final String ANOMALY_DETECTOR_JOB = "anomaly_detector_job";
    public static final String RUN = "_run";
    public static final String PREVIEW = "_preview";
    public static final String START_JOB = "_start";
    public static final String STOP_JOB = "_stop";
    public static final ToXContent.MapParams XCONTENT_WITH_TYPE = new ToXContent.MapParams(ImmutableMap.of("with_type", "true"));

    private static final String KIBANA_USER_AGENT = "Kibana";
    private static final String[] UI_METADATA_EXCLUDE = new String[] { AnomalyDetector.UI_METADATA_FIELD };

    private RestHandlerUtils() {}

    /**
     * Checks to see if the request came from Kibana, if so we want to return the UI Metadata from the document.
     * If the request came from the client then we exclude the UI Metadata from the search result.
     *
     * @param request rest request
     * @return instance of {@link org.elasticsearch.search.fetch.subphase.FetchSourceContext}
     */
    public static FetchSourceContext getSourceContext(RestRequest request) {
        String userAgent = Strings.coalesceToEmpty(request.header("User-Agent"));
        if (!userAgent.contains(KIBANA_USER_AGENT)) {
            return new FetchSourceContext(true, Strings.EMPTY_ARRAY, UI_METADATA_EXCLUDE);
        } else {
            return null;
        }
    }

    public static XContentParser createXContentParser(RestChannel channel, BytesReference bytesReference) throws IOException {
        return XContentHelper
            .createParser(channel.request().getXContentRegistry(), LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON);
    }
}
