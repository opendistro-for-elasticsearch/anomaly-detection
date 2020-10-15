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

package com.amazon.opendistroforelasticsearch.ad.model;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.index.query.MatchAllQueryBuilder;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class AnomalyDetectorTests extends AbstractADTest {

    public void testParseAnomalyDetector() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), Instant.now());
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        LOG.info(detectorString);
        detectorString = detectorString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyDetector parsedDetector = AnomalyDetector.parse(TestHelpers.parser(detectorString));
        assertEquals("Parsing anomaly detector doesn't work", detector, parsedDetector);
    }

    public void testParseAnomalyDetectorWithNullFilterQuery() throws IOException {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\""
            + ":true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"detection_interval\":"
            + "{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},\"window_delay\":{\"period\":{\"interval\":973,"
            + "\"unit\":\"Minutes\"}},\"shingle_size\":4,\"schema_version\":-1203962153,\"ui_metadata\":{\"JbAaV\":{\"feature_id\":"
            + "\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,\"aggregation_query\":{\"aa\":"
            + "{\"value_count\":{\"field\":\"ok\"}}}}},\"last_update_time\":1568396089028}";
        AnomalyDetector parsedDetector = AnomalyDetector.parse(TestHelpers.parser(detectorString));
        assertTrue(parsedDetector.getFilterQuery() instanceof MatchAllQueryBuilder);
    }

    public void testParseAnomalyDetectorWithEmptyFilterQuery() throws IOException {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\":"
            + "true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"filter_query\":{},"
            + "\"detection_interval\":{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},\"window_delay\":"
            + "{\"period\":{\"interval\":973,\"unit\":\"Minutes\"}},\"shingle_size\":4,\"schema_version\":-1203962153,\"ui_metadata\":"
            + "{\"JbAaV\":{\"feature_id\":\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,"
            + "\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}},"
            + "\"last_update_time\":1568396089028}";
        AnomalyDetector parsedDetector = AnomalyDetector.parse(TestHelpers.parser(detectorString));
        assertTrue(parsedDetector.getFilterQuery() instanceof MatchAllQueryBuilder);
    }

    public void testParseAnomalyDetectorWithWrongFilterQuery() throws Exception {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\":"
            + "true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"filter_query\":"
            + "{\"aa\":\"bb\"},\"detection_interval\":{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},"
            + "\"window_delay\":{\"period\":{\"interval\":973,\"unit\":\"Minutes\"}},\"shingle_size\":4,\"schema_version\":"
            + "-1203962153,\"ui_metadata\":{\"JbAaV\":{\"feature_id\":\"rIFjS\",\"feature_name\":\"QXCmS\","
            + "\"feature_enabled\":false,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}},"
            + "\"last_update_time\":1568396089028}";
        TestHelpers.assertFailWith(ParsingException.class, () -> AnomalyDetector.parse(TestHelpers.parser(detectorString)));
    }

    public void testParseAnomalyDetectorWithoutOptionalParams() throws IOException {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\""
            + ":true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"detection_interval\":"
            + "{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},\"schema_version\":-1203962153,\"ui_metadata\":"
            + "{\"JbAaV\":{\"feature_id\":\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,"
            + "\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}},\"last_update_time\":1568396089028}";
        AnomalyDetector parsedDetector = AnomalyDetector.parse(TestHelpers.parser(detectorString), "id", 1L, null, null);
        assertTrue(parsedDetector.getFilterQuery() instanceof MatchAllQueryBuilder);
        assertEquals((long) parsedDetector.getShingleSize(), (long) AnomalyDetectorSettings.DEFAULT_SHINGLE_SIZE);
    }

    public void testParseAnomalyDetectorWithInvalidShingleSize() throws Exception {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\""
            + ":true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"detection_interval\":"
            + "{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},\"shingle_size\":-1,\"schema_version\":-1203962153,\"ui_metadata\":"
            + "{\"JbAaV\":{\"feature_id\":\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,"
            + "\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}},\"last_update_time\":1568396089028}";
        TestHelpers.assertFailWith(IllegalArgumentException.class, () -> AnomalyDetector.parse(TestHelpers.parser(detectorString)));
    }

    public void testParseAnomalyDetectorWithNullUiMetadata() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(null, Instant.now());
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        AnomalyDetector parsedDetector = AnomalyDetector.parse(TestHelpers.parser(detectorString));
        assertEquals("Parsing anomaly detector doesn't work", detector, parsedDetector);
        assertNull(parsedDetector.getUiMetadata());
    }

    public void testParseAnomalyDetectorWithEmptyUiMetadata() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of(), Instant.now());
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        AnomalyDetector parsedDetector = AnomalyDetector.parse(TestHelpers.parser(detectorString));
        assertEquals("Parsing anomaly detector doesn't work", detector, parsedDetector);
    }

    public void testInvalidShingleSize() throws Exception {
        TestHelpers
            .assertFailWith(
                IllegalArgumentException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    ImmutableList.of(randomAlphaOfLength(5)),
                    ImmutableList.of(TestHelpers.randomFeature()),
                    TestHelpers.randomQuery(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    0,
                    null,
                    1,
                    Instant.now(),
                    null
                )
            );
    }

    public void testNullDetectorName() throws Exception {
        TestHelpers
            .assertFailWith(
                IllegalArgumentException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    null,
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    ImmutableList.of(randomAlphaOfLength(5)),
                    ImmutableList.of(TestHelpers.randomFeature()),
                    TestHelpers.randomQuery(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    AnomalyDetectorSettings.DEFAULT_SHINGLE_SIZE,
                    null,
                    1,
                    Instant.now(),
                    null
                )
            );
    }

    public void testBlankDetectorName() throws Exception {
        TestHelpers
            .assertFailWith(
                IllegalArgumentException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    "",
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    ImmutableList.of(randomAlphaOfLength(5)),
                    ImmutableList.of(TestHelpers.randomFeature()),
                    TestHelpers.randomQuery(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    AnomalyDetectorSettings.DEFAULT_SHINGLE_SIZE,
                    null,
                    1,
                    Instant.now(),
                    null
                )
            );
    }

    public void testNullTimeField() throws Exception {
        TestHelpers
            .assertFailWith(
                IllegalArgumentException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    null,
                    ImmutableList.of(randomAlphaOfLength(5)),
                    ImmutableList.of(TestHelpers.randomFeature()),
                    TestHelpers.randomQuery(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    AnomalyDetectorSettings.DEFAULT_SHINGLE_SIZE,
                    null,
                    1,
                    Instant.now(),
                    null
                )
            );
    }

    public void testNullIndices() throws Exception {
        TestHelpers
            .assertFailWith(
                IllegalArgumentException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    null,
                    ImmutableList.of(TestHelpers.randomFeature()),
                    TestHelpers.randomQuery(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    AnomalyDetectorSettings.DEFAULT_SHINGLE_SIZE,
                    null,
                    1,
                    Instant.now(),
                    null
                )
            );
    }

    public void testEmptyIndices() throws Exception {
        TestHelpers
            .assertFailWith(
                IllegalArgumentException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    ImmutableList.of(),
                    ImmutableList.of(TestHelpers.randomFeature()),
                    TestHelpers.randomQuery(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    AnomalyDetectorSettings.DEFAULT_SHINGLE_SIZE,
                    null,
                    1,
                    Instant.now(),
                    null
                )
            );
    }

    public void testNullDetectionInterval() throws Exception {
        TestHelpers
            .assertFailWith(
                IllegalArgumentException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    ImmutableList.of(randomAlphaOfLength(5)),
                    ImmutableList.of(TestHelpers.randomFeature()),
                    TestHelpers.randomQuery(),
                    null,
                    TestHelpers.randomIntervalTimeConfiguration(),
                    AnomalyDetectorSettings.DEFAULT_SHINGLE_SIZE,
                    null,
                    1,
                    Instant.now(),
                    null
                )
            );
    }

    public void testNullFeatures() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(null, null, Instant.now().truncatedTo(ChronoUnit.SECONDS));
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        AnomalyDetector parsedDetector = AnomalyDetector.parse(TestHelpers.parser(detectorString));
        assertEquals(0, parsedDetector.getFeatureAttributes().size());
    }

    public void testEmptyFeatures() throws IOException {
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetector(ImmutableList.of(), null, Instant.now().truncatedTo(ChronoUnit.SECONDS));
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        AnomalyDetector parsedDetector = AnomalyDetector.parse(TestHelpers.parser(detectorString));
        assertEquals(0, parsedDetector.getFeatureAttributes().size());
    }

    public void testGetShingleSize() throws IOException {
        AnomalyDetector anomalyDetector = new AnomalyDetector(
            randomAlphaOfLength(5),
            randomLong(),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(5)),
            ImmutableList.of(TestHelpers.randomFeature()),
            TestHelpers.randomQuery(),
            TestHelpers.randomIntervalTimeConfiguration(),
            TestHelpers.randomIntervalTimeConfiguration(),
            5,
            null,
            1,
            Instant.now(),
            null
        );
        assertEquals((int) anomalyDetector.getShingleSize(), 5);
    }

    public void testGetShingleSizeReturnsDefaultValue() throws IOException {
        AnomalyDetector anomalyDetector = new AnomalyDetector(
            randomAlphaOfLength(5),
            randomLong(),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(5)),
            ImmutableList.of(TestHelpers.randomFeature()),
            TestHelpers.randomQuery(),
            TestHelpers.randomIntervalTimeConfiguration(),
            TestHelpers.randomIntervalTimeConfiguration(),
            null,
            null,
            1,
            Instant.now(),
            null
        );
        assertEquals((int) anomalyDetector.getShingleSize(), AnomalyDetectorSettings.DEFAULT_SHINGLE_SIZE);
    }
}
