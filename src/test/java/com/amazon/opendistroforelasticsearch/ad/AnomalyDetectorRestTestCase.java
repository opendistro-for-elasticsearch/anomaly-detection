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

package com.amazon.opendistroforelasticsearch.ad;

import static org.apache.http.entity.ContentType.APPLICATION_JSON;
import static org.elasticsearch.common.xcontent.json.JsonXContent.jsonXContent;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public abstract class AnomalyDetectorRestTestCase extends ODFERestTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(ImmutableList.of(AnomalyDetector.XCONTENT_REGISTRY));
    }

    @Override
    protected Settings restClientSettings() {
        return super.restClientSettings();
    }

    protected AnomalyDetector createRandomAnomalyDetector(Boolean refresh, Boolean withMetadata) throws IOException {
        Map<String, Object> uiMetadata = null;
        if (withMetadata) {
            uiMetadata = TestHelpers.randomUiMetadata();
        }
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(uiMetadata, null);
        String indexName = detector.getIndices().get(0);
        TestHelpers
            .makeRequest(
                client(),
                "POST",
                "/" + indexName + "/_doc/" + randomAlphaOfLength(5) + "?refresh=true",
                ImmutableMap.of(),
                toHttpEntity("{\"name\": \"test\"}"),
                null
            );
        AnomalyDetector createdDetector = createAnomalyDetector(detector, refresh);

        if (withMetadata) {
            return getAnomalyDetector(createdDetector.getDetectorId(), new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"));
        }
        return getAnomalyDetector(createdDetector.getDetectorId(), new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"));
    }

    protected AnomalyDetector createAnomalyDetector(AnomalyDetector detector, Boolean refresh) throws IOException {
        Response response = TestHelpers
            .makeRequest(client(), "POST", TestHelpers.AD_BASE_DETECTORS_URI, ImmutableMap.of(), toHttpEntity(detector), null);
        assertEquals("Create anomaly detector failed", RestStatus.CREATED, restStatus(response));

        Map<String, Object> detectorJson = jsonXContent
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.getEntity().getContent())
            .map();
        return new AnomalyDetector(
            (String) detectorJson.get("_id"),
            ((Integer) detectorJson.get("_version")).longValue(),
            detector.getName(),
            detector.getDescription(),
            detector.getTimeField(),
            detector.getIndices(),
            detector.getFeatureAttributes(),
            detector.getFilterQuery(),
            detector.getDetectionInterval(),
            detector.getWindowDelay(),
            detector.getShingleSize(),
            detector.getUiMetadata(),
            detector.getSchemaVersion(),
            detector.getLastUpdateTime(),
            null
        );
    }

    public AnomalyDetector getAnomalyDetector(String detectorId) throws IOException {
        return (AnomalyDetector) getAnomalyDetector(detectorId, false)[0];
    }

    public AnomalyDetector getAnomalyDetector(String detectorId, BasicHeader header) throws IOException {
        return (AnomalyDetector) getAnomalyDetector(detectorId, header, false)[0];
    }

    public ToXContentObject[] getAnomalyDetector(String detectorId, boolean returnJob) throws IOException {
        BasicHeader header = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        return getAnomalyDetector(detectorId, header, returnJob);
    }

    public ToXContentObject[] getAnomalyDetector(String detectorId, BasicHeader header, boolean returnJob) throws IOException {
        Response response = TestHelpers
            .makeRequest(
                client(),
                "GET",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId + "?job=" + returnJob,
                null,
                "",
                ImmutableList.of(header)
            );
        assertEquals("Unable to get anomaly detector " + detectorId, RestStatus.OK, restStatus(response));
        XContentParser parser = createAdParser(XContentType.JSON.xContent(), response.getEntity().getContent());
        parser.nextToken();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);

        String id = null;
        Long version = null;
        AnomalyDetector detector = null;
        AnomalyDetectorJob detectorJob = null;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case "_id":
                    id = parser.text();
                    break;
                case "_version":
                    version = parser.longValue();
                    break;
                case "anomaly_detector":
                    detector = AnomalyDetector.parse(parser);
                    break;
                case "anomaly_detector_job":
                    detectorJob = AnomalyDetectorJob.parse(parser);
                    break;
            }
        }

        return new ToXContentObject[] {
            new AnomalyDetector(
                id,
                version,
                detector.getName(),
                detector.getDescription(),
                detector.getTimeField(),
                detector.getIndices(),
                detector.getFeatureAttributes(),
                detector.getFilterQuery(),
                detector.getDetectionInterval(),
                detector.getWindowDelay(),
                detector.getShingleSize(),
                detector.getUiMetadata(),
                detector.getSchemaVersion(),
                detector.getLastUpdateTime(),
                null
            ),
            detectorJob };
    }

    protected HttpEntity toHttpEntity(ToXContentObject object) throws IOException {
        return new StringEntity(toJsonString(object), APPLICATION_JSON);
    }

    protected HttpEntity toHttpEntity(String jsonString) throws IOException {
        return new StringEntity(jsonString, APPLICATION_JSON);
    }

    protected String toJsonString(ToXContentObject object) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        return TestHelpers.xContentBuilderToString(shuffleXContent(object.toXContent(builder, ToXContent.EMPTY_PARAMS)));
    }

    protected RestStatus restStatus(Response response) {
        return RestStatus.fromCode(response.getStatusLine().getStatusCode());
    }

    protected final XContentParser createAdParser(XContent xContent, InputStream data) throws IOException {
        return xContent.createParser(TestHelpers.xContentRegistry(), LoggingDeprecationHandler.INSTANCE, data);
    }

    public void updateClusterSettings(String settingKey, Object value) throws Exception {
        XContentBuilder builder = XContentFactory
            .jsonBuilder()
            .startObject()
            .startObject("persistent")
            .field(settingKey, value)
            .endObject()
            .endObject();
        Request request = new Request("PUT", "_cluster/settings");
        request.setJsonEntity(Strings.toString(builder));
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    public Response getDetectorProfile(String detectorId, boolean all, String customizedProfile) throws IOException {
        return TestHelpers
            .makeRequest(
                client(),
                "GET",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId + "/" + RestHandlerUtils.PROFILE + customizedProfile + "?_all=" + all,
                null,
                "",
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
    }

    public Response getDetectorProfile(String detectorId) throws IOException {
        return getDetectorProfile(detectorId, false, "");
    }

    public Response getDetectorProfile(String detectorId, boolean all) throws IOException {
        return getDetectorProfile(detectorId, all, "");
    }
}
