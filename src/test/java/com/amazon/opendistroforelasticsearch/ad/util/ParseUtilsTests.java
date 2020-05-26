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

package com.amazon.opendistroforelasticsearch.ad.util;

import java.io.IOException;
import java.time.Instant;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.Feature;

public class ParseUtilsTests extends ESTestCase {

    public void testToInstant() throws IOException {
        long epochMilli = Instant.now().toEpochMilli();
        XContentBuilder builder = XContentFactory.jsonBuilder().value(epochMilli);
        XContentParser parser = this.createParser(builder);
        parser.nextToken();
        Instant instant = ParseUtils.toInstant(parser);
        assertEquals(epochMilli, instant.toEpochMilli());
    }

    public void testToInstantWithNullToken() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().value((Long) null);
        XContentParser parser = this.createParser(builder);
        parser.nextToken();
        XContentParser.Token token = parser.currentToken();
        assertEquals(token, XContentParser.Token.VALUE_NULL);
        Instant instant = ParseUtils.toInstant(parser);
        assertNull(instant);
    }

    public void testToInstantWithNullValue() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().value(randomLong());
        XContentParser parser = this.createParser(builder);
        parser.nextToken();
        parser.nextToken();
        XContentParser.Token token = parser.currentToken();
        assertNull(token);
        Instant instant = ParseUtils.toInstant(parser);
        assertNull(instant);
    }

    public void testToInstantWithNotValue() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().nullField("test").endObject();
        XContentParser parser = this.createParser(builder);
        parser.nextToken();
        Instant instant = ParseUtils.toInstant(parser);
        assertNull(instant);
    }

    public void testToAggregationBuilder() throws IOException {
        XContentParser parser = TestHelpers.parser("{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}");
        AggregationBuilder aggregationBuilder = ParseUtils.toAggregationBuilder(parser);
        assertNotNull(aggregationBuilder);
        assertEquals("aa", aggregationBuilder.getName());
    }

    public void testParseAggregatorsWithAggregationQueryString() throws IOException {
        AggregatorFactories.Builder agg = ParseUtils
            .parseAggregators("{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}", TestHelpers.xContentRegistry(), "test");
        assertEquals("test", agg.getAggregatorFactories().iterator().next().getName());
    }

    public void testParseAggregatorsWithAggregationQueryStringAndNullAggName() throws IOException {
        AggregatorFactories.Builder agg = ParseUtils
            .parseAggregators("{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}", TestHelpers.xContentRegistry(), null);
        assertEquals("aa", agg.getAggregatorFactories().iterator().next().getName());
    }

    public void testGenerateInternalFeatureQuery() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(null, Instant.now());
        long startTime = randomLong();
        long endTime = randomLong();
        SearchSourceBuilder builder = ParseUtils.generateInternalFeatureQuery(detector, startTime, endTime, TestHelpers.xContentRegistry());
        for (Feature feature : detector.getFeatureAttributes()) {
            assertTrue(builder.toString().contains(feature.getId()));
        }
    }

    public void testGenerateInternalFeatureQueryTemplate() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(null, Instant.now());
        String builder = ParseUtils.generateInternalFeatureQueryTemplate(detector, TestHelpers.xContentRegistry());
        for (Feature feature : detector.getFeatureAttributes()) {
            assertTrue(builder.contains(feature.getId()));
        }
    }
}
