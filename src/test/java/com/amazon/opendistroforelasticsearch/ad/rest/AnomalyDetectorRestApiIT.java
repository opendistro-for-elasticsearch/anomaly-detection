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

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorRestTestCase;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorExecutionInput;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.google.common.collect.ImmutableMap;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import static com.amazon.opendistroforelasticsearch.ad.TestHelpers.AD_BASE_PREVIEW_URI;
import static com.amazon.opendistroforelasticsearch.ad.TestHelpers.randomAnomalyDetectorWithEmptyFeature;

public class AnomalyDetectorRestApiIT extends AnomalyDetectorRestTestCase {

    public void testCreateAnomalyDetectorWithNotExistingIndices() throws Exception {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null);
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "index_not_found_exception",
                () -> TestHelpers
                    .makeRequest(client(), "POST", TestHelpers.AD_BASE_DETECTORS_URI, ImmutableMap.of(), toHttpEntity(detector), null)
            );
    }

    public void testCreateAnomalyDetectorWithEmptyIndices() throws Exception {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null);
        TestHelpers
            .makeRequest(
                client(),
                "PUT",
                "/" + detector.getIndices().get(0),
                ImmutableMap.of(),
                toHttpEntity("{\"settings\":{\"number_of_shards\":1},\"mappings\":{\"properties\":" + "{\"field1\":{\"type\":\"text\"}}}}"),
                null
            );

        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "Can't create anomaly detector as no document found in indices",
                () -> TestHelpers
                    .makeRequest(client(), "POST", TestHelpers.AD_BASE_DETECTORS_URI, ImmutableMap.of(), toHttpEntity(detector), null)
            );
    }

    public void testCreateAnomalyDetector() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null);
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

        Response response = TestHelpers
            .makeRequest(client(), "POST", TestHelpers.AD_BASE_DETECTORS_URI, ImmutableMap.of(), toHttpEntity(detector), null);
        assertEquals("Create anomaly detector failed", RestStatus.CREATED, restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        String id = (String) responseMap.get("_id");
        int version = (int) responseMap.get("_version");
        assertNotEquals("response is missing Id", AnomalyDetector.NO_ID, id);
        assertTrue("incorrect version", version > 0);
    }

    public void testGetAnomalyDetector() throws IOException {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true);
        AnomalyDetector createdDetector = getAnomalyDetector(detector.getDetectorId());
        assertEquals("Incorrect Location header", detector, createdDetector);
    }

    public void testGetNotExistingAnomalyDetector() throws Exception {
        createRandomAnomalyDetector(true, true);
        TestHelpers.assertFailWith(ResponseException.class, null, () -> getAnomalyDetector(randomAlphaOfLength(5)));
    }

    public void testUpdateAnomalyDetectorA() throws IOException {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true);

        String newDescription = randomAlphaOfLength(5);

        AnomalyDetector newDetector = new AnomalyDetector(
            detector.getDetectorId(),
            detector.getVersion(),
            detector.getName(),
            newDescription,
            detector.getTimeField(),
            detector.getIndices(),
            detector.getFeatureAttributes(),
            detector.getFilterQuery(),
            detector.getDetectionInterval(),
            detector.getWindowDelay(),
            detector.getUiMetadata(),
            detector.getSchemaVersion(),
            detector.getLastUpdateTime()
        );

        Response updateResponse = TestHelpers
            .makeRequest(
                client(),
                "PUT",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "?refresh=true",
                ImmutableMap.of(),
                toHttpEntity(newDetector),
                null
            );

        assertEquals("Update anomaly detector failed", RestStatus.OK, restStatus(updateResponse));
        Map<String, Object> responseBody = entityAsMap(updateResponse);
        assertEquals("Updated anomaly detector id doesn't match", detector.getDetectorId(), responseBody.get("_id"));
        assertEquals("Version not incremented", (detector.getVersion().intValue() + 1), (int) responseBody.get("_version"));

        AnomalyDetector updatedDetector = getAnomalyDetector(detector.getDetectorId());
        assertNotEquals("Anomaly detector last update time not changed", updatedDetector.getLastUpdateTime(), detector.getLastUpdateTime());
        assertEquals("Anomaly detector description not updated", newDescription, updatedDetector.getDescription());
    }

    public void testUpdateAnomalyDetectorWithNotExistingIndex() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true);

        String newDescription = randomAlphaOfLength(5);

        AnomalyDetector newDetector = new AnomalyDetector(
            detector.getDetectorId(),
            detector.getVersion(),
            detector.getName(),
            newDescription,
            detector.getTimeField(),
            detector.getIndices(),
            detector.getFeatureAttributes(),
            detector.getFilterQuery(),
            detector.getDetectionInterval(),
            detector.getWindowDelay(),
            detector.getUiMetadata(),
            detector.getSchemaVersion(),
            detector.getLastUpdateTime()
        );

        deleteIndex(AnomalyDetector.ANOMALY_DETECTORS_INDEX);

        TestHelpers
            .assertFailWith(
                ResponseException.class,
                null,
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "PUT",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId(),
                        ImmutableMap.of(),
                        toHttpEntity(newDetector),
                        null
                    )
            );
    }

    public void testSearchAnomalyDetector() throws IOException {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true);
        SearchSourceBuilder search = (new SearchSourceBuilder()).query(QueryBuilders.termQuery("_id", detector.getDetectorId()));
        Response searchResponse = TestHelpers
            .makeRequest(
                client(),
                "GET",
                TestHelpers.AD_BASE_DETECTORS_URI + "/_search",
                ImmutableMap.of(),
                new NStringEntity(search.toString(), ContentType.APPLICATION_JSON),
                null
            );
        assertEquals("Search anomaly detector failed", RestStatus.OK, restStatus(searchResponse));
    }

    public void testStatsAnomalyDetector() throws IOException {
        Response statsResponse = TestHelpers
            .makeRequest(client(), "GET", AnomalyDetectorPlugin.AD_BASE_URI + "/stats", ImmutableMap.of(), "", null);

        assertEquals("Get stats failed", RestStatus.OK, restStatus(statsResponse));
    }

    public void testPreviewAnomalyDetector() throws IOException {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false);
        AnomalyDetectorExecutionInput input = new AnomalyDetectorExecutionInput(
            detector.getDetectorId(),
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            null
        );
        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(AD_BASE_PREVIEW_URI, input.getDetectorId()),
                ImmutableMap.of(),
                toHttpEntity(input),
                null
            );
        assertEquals("Execute anomaly detector failed", RestStatus.OK, restStatus(response));
    }

    public void testPreviewAnomalyDetectorWhichNotExist() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false);
        AnomalyDetectorExecutionInput input = new AnomalyDetectorExecutionInput(
            randomAlphaOfLength(5),
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            null
        );
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        String.format(TestHelpers.AD_BASE_PREVIEW_URI, input.getDetectorId()),
                        ImmutableMap.of(),
                        toHttpEntity(input),
                        null
                    )
            );
    }

    public void testExecuteAnomalyDetectorWithNullDetectorId() throws Exception {
        AnomalyDetectorExecutionInput input = new AnomalyDetectorExecutionInput(
            null,
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            null
        );
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        String.format(TestHelpers.AD_BASE_PREVIEW_URI, input.getDetectorId()),
                        ImmutableMap.of(),
                        toHttpEntity(input),
                        null
                    )
            );
    }

    public void testPreviewAnomalyDetectorWithDetector() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true);
        AnomalyDetectorExecutionInput input = new AnomalyDetectorExecutionInput(
            detector.getDetectorId(),
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            detector
        );
        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(AD_BASE_PREVIEW_URI, input.getDetectorId()),
                ImmutableMap.of(),
                toHttpEntity(input),
                null
            );
        assertEquals("Execute anomaly detector failed", RestStatus.OK, restStatus(response));
    }

    public void testPreviewAnomalyDetectorWithDetectorAndNoFeatures() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true);
        AnomalyDetectorExecutionInput input = new AnomalyDetectorExecutionInput(
            detector.getDetectorId(),
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            randomAnomalyDetectorWithEmptyFeature()
        );
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "Can't preview detector without feature",
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        String.format(TestHelpers.AD_BASE_PREVIEW_URI, input.getDetectorId()),
                        ImmutableMap.of(),
                        toHttpEntity(input),
                        null
                    )
            );
    }

    public void testSearchAnomalyResult() throws IOException {
        AnomalyResult anomalyResult = TestHelpers.randomAnomalyDetectResult();
        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                "/.opendistro-anomaly-results/_doc/" + UUIDs.base64UUID(),
                ImmutableMap.of(),
                toHttpEntity(anomalyResult),
                null
            );
        assertEquals("Post anomaly result failed", RestStatus.CREATED, restStatus(response));

        SearchSourceBuilder search = (new SearchSourceBuilder())
            .query(QueryBuilders.termQuery("detector_id", anomalyResult.getDetectorId()));
        Response searchResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_RESULT_URI + "/_search",
                ImmutableMap.of(),
                new NStringEntity(search.toString(), ContentType.APPLICATION_JSON),
                null
            );
        assertEquals("Search anomaly result failed", RestStatus.OK, restStatus(searchResponse));

        SearchSourceBuilder searchAll = SearchSourceBuilder.fromXContent(TestHelpers.parser("{\"query\":{\"match_all\":{}}}"));
        Response searchAllResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_RESULT_URI + "/_search",
                ImmutableMap.of(),
                new NStringEntity(searchAll.toString(), ContentType.APPLICATION_JSON),
                null
            );
        assertEquals("Search anomaly result failed", RestStatus.OK, restStatus(searchAllResponse));
    }

    public void testDeleteAnomalyDetector() throws IOException {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false);
        Response response = TestHelpers
            .makeRequest(
                client(),
                "DELETE",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId(),
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Delete anomaly detector failed", RestStatus.OK, restStatus(response));
    }

    public void testDeleteAnomalyDetectorWhichNotExist() throws Exception {
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "DELETE",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + randomAlphaOfLength(5),
                        ImmutableMap.of(),
                        "",
                        null
                    )
            );
    }

    public void testDeleteAnomalyDetectorWithNoAdJob() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false);
        Response response = TestHelpers
            .makeRequest(
                client(),
                "DELETE",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId(),
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Delete anomaly detector failed", RestStatus.OK, restStatus(response));
    }

    public void testDeleteAnomalyDetectorWithRunningAdJob() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false);

        Response startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );

        assertEquals("Fail to start AD job", RestStatus.OK, restStatus(startAdJobResponse));

        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "Detector job is running",
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "DELETE",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId(),
                        ImmutableMap.of(),
                        "",
                        null
                    )
            );
    }

    public void testUpdateAnomalyDetectorWithRunningAdJob() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false);

        Response startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );

        assertEquals("Fail to start AD job", RestStatus.OK, restStatus(startAdJobResponse));

        String newDescription = randomAlphaOfLength(5);

        AnomalyDetector newDetector = new AnomalyDetector(
            detector.getDetectorId(),
            detector.getVersion(),
            detector.getName(),
            newDescription,
            detector.getTimeField(),
            detector.getIndices(),
            detector.getFeatureAttributes(),
            detector.getFilterQuery(),
            detector.getDetectionInterval(),
            detector.getWindowDelay(),
            detector.getUiMetadata(),
            detector.getSchemaVersion(),
            detector.getLastUpdateTime()
        );

        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "Detector job is running",
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "PUT",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId(),
                        ImmutableMap.of(),
                        toHttpEntity(newDetector),
                        null
                    )
            );
    }

    public void testGetDetectorWithAdJob() throws IOException {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false);

        Response startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );

        assertEquals("Fail to start AD job", RestStatus.OK, restStatus(startAdJobResponse));

        ToXContentObject[] results = getAnomalyDetector(detector.getDetectorId(), true);
        assertEquals("Incorrect Location header", detector, results[0]);
        assertEquals("Incorrect detector job name", detector.getDetectorId(), ((AnomalyDetectorJob) results[1]).getName());
        assertTrue(((AnomalyDetectorJob) results[1]).isEnabled());

        results = getAnomalyDetector(detector.getDetectorId(), false);
        assertEquals("Incorrect Location header", detector, results[0]);
        assertEquals("Should not return detector job", null, results[1]);
    }

    public void testStartAdJobWithExistingDetector() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false);

        Response startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );

        assertEquals("Fail to start AD job", RestStatus.OK, restStatus(startAdJobResponse));

        startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );

        assertEquals("Fail to start AD job", RestStatus.OK, restStatus(startAdJobResponse));
    }

    public void testStartAdJobWithNonexistingDetectorIndex() throws Exception {
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "no such index [.opendistro-anomaly-detectors]",
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + randomAlphaOfLength(10) + "/_start",
                        ImmutableMap.of(),
                        "",
                        null
                    )
            );
    }

    public void testStartAdJobWithNonexistingDetector() throws Exception {
        createRandomAnomalyDetector(true, false);
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "AnomalyDetector is not found with id",
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + randomAlphaOfLength(10) + "/_start",
                        ImmutableMap.of(),
                        "",
                        null
                    )
            );
    }

    public void testStopAdJob() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false);
        Response startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Fail to start AD job", RestStatus.OK, restStatus(startAdJobResponse));

        Response stopAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_stop",
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Fail to stop AD job", RestStatus.OK, restStatus(stopAdJobResponse));

        stopAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_stop",
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Fail to stop AD job", RestStatus.OK, restStatus(stopAdJobResponse));
    }

    public void testStopNonExistingAdJobIndex() throws Exception {
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "no such index [.opendistro-anomaly-detector-jobs]",
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + randomAlphaOfLength(10) + "/_stop",
                        ImmutableMap.of(),
                        "",
                        null
                    )
            );
    }

    public void testStopNonExistingAdJob() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false);
        Response startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Fail to start AD job", RestStatus.OK, restStatus(startAdJobResponse));

        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "Anomaly detector job not exist",
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + randomAlphaOfLength(10) + "/_stop",
                        ImmutableMap.of(),
                        "",
                        null
                    )
            );
    }

    public void testStartDisabledAdjob() throws IOException {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false);
        Response startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Fail to start AD job", RestStatus.OK, restStatus(startAdJobResponse));

        Response stopAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_stop",
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Fail to stop AD job", RestStatus.OK, restStatus(stopAdJobResponse));

        startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );

        assertEquals("Fail to start AD job", RestStatus.OK, restStatus(startAdJobResponse));
    }

}
