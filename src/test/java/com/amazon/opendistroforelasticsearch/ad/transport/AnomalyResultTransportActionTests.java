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

package com.amazon.opendistroforelasticsearch.ad.transport;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.junit.Before;

import com.amazon.opendistroforelasticsearch.ad.ADIntegTestCase;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorType;
import com.amazon.opendistroforelasticsearch.ad.model.Feature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class AnomalyResultTransportActionTests extends ADIntegTestCase {
    private String testIndex;
    private Instant testDataTimeStamp;
    private long start;
    private long end;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        testIndex = "test_data";
        testDataTimeStamp = Instant.now();
        start = testDataTimeStamp.minus(10, ChronoUnit.MINUTES).toEpochMilli();
        end = testDataTimeStamp.plus(10, ChronoUnit.MINUTES).toEpochMilli();
        ingestTestData();
    }

    private void ingestTestData() throws IOException {
        String mappings = "{\"properties\":{\"timestamp\":{\"type\":\"date\",\"format\":\"strict_date_time||epoch_millis\"},"
            + "\"value\":{\"type\":\"double\"}, \"type\":{\"type\":\"keyword\"},"
            + "\"is_error\":{\"type\":\"boolean\"}, \"message\":{\"type\":\"text\"}}}";
        createIndex(testIndex, mappings);
        double value = randomDouble();
        String type = randomAlphaOfLength(5);
        boolean isError = randomBoolean();
        String message = randomAlphaOfLength(10);
        String id = indexDoc(
            testIndex,
            ImmutableMap
                .of("timestamp", testDataTimeStamp.toEpochMilli(), "value", value, "type", type, "is_error", isError, "message", message)
        );
        GetResponse doc = getDoc(testIndex, id);
        Map<String, Object> sourceAsMap = doc.getSourceAsMap();
        assertEquals(testDataTimeStamp.toEpochMilli(), sourceAsMap.get("timestamp"));
        assertEquals(value, sourceAsMap.get("value"));
        assertEquals(type, sourceAsMap.get("type"));
        assertEquals(isError, sourceAsMap.get("is_error"));
        assertEquals(message, sourceAsMap.get("message"));
        createDetectorIndex();
    }

    public void testFeatureQueryWithTermsAggregation() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"terms\":{\"field\":\"type\"}}}");
        assertErrorMessage(adId, "Failed to parse aggregation");
    }

    public void testFeatureWithSumOfTextField() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"sum\":{\"field\":\"message\"}}}");
        assertErrorMessage(adId, "Text fields are not optimised for operations");
    }

    public void testFeatureWithSumOfTypeField() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"sum\":{\"field\":\"type\"}}}");
        assertErrorMessage(adId, "Field [type] of type [keyword] is not supported for aggregation [sum]");
    }

    public void testFeatureWithMaxOfTextField() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"max\":{\"field\":\"message\"}}}");
        assertErrorMessage(adId, "Text fields are not optimised for operations");
    }

    public void testFeatureWithMaxOfTypeField() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"max\":{\"field\":\"type\"}}}");
        assertErrorMessage(adId, "Field [type] of type [keyword] is not supported for aggregation [max]");
    }

    public void testFeatureWithMinOfTextField() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"min\":{\"field\":\"message\"}}}");
        assertErrorMessage(adId, "Text fields are not optimised for operations");
    }

    public void testFeatureWithMinOfTypeField() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"min\":{\"field\":\"type\"}}}");
        assertErrorMessage(adId, "Field [type] of type [keyword] is not supported for aggregation [min]");
    }

    public void testFeatureWithAvgOfTextField() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"avg\":{\"field\":\"message\"}}}");
        assertErrorMessage(adId, "Text fields are not optimised for operations");
    }

    public void testFeatureWithAvgOfTypeField() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"avg\":{\"field\":\"type\"}}}");
        assertErrorMessage(adId, "Field [type] of type [keyword] is not supported for aggregation [avg]");
    }

    public void testFeatureWithCountOfTextField() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"value_count\":{\"field\":\"message\"}}}");
        assertErrorMessage(adId, "Text fields are not optimised for operations");
    }

    public void testFeatureWithCardinalityOfTextField() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"cardinality\":{\"field\":\"message\"}}}");
        assertErrorMessage(adId, "Text fields are not optimised for operations");
    }

    private String createDetectorWithFeatureAgg(String aggQuery) throws IOException {
        AggregationBuilder aggregationBuilder = TestHelpers.parseAggregation(aggQuery);
        Feature feature = new Feature(randomAlphaOfLength(5), randomAlphaOfLength(10), true, aggregationBuilder);
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetector(
                ImmutableList.of(testIndex),
                ImmutableList.of(feature),
                ImmutableMap.of(),
                Instant.now(),
                AnomalyDetectorType.REALTIME_SINGLE_ENTITY.name(),
                null,
                false
            );
        String adId = createDetectors(detector);
        return adId;
    }

    private void assertErrorMessage(String adId, String errorMessage) {
        AnomalyResultRequest resultRequest = new AnomalyResultRequest(adId, start, end);
        RuntimeException e = expectThrowsAnyOf(
            ImmutableList.of(NotSerializableExceptionWrapper.class, AnomalyDetectionException.class),
            () -> client().execute(AnomalyResultAction.INSTANCE, resultRequest).actionGet(30_000)
        );
        assertTrue(e.getMessage().contains(errorMessage));
    }
}
