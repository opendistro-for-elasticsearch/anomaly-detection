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

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.DETECTOR_TYPE_FIELD;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.amazon.opendistroforelasticsearch.ad.HistoricalDetectorIntegTestCase;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorType;
import com.google.common.collect.ImmutableList;

public class SearchAnomalyDetectorActionTests extends HistoricalDetectorIntegTestCase {

    private String indexName = "test-data";
    private Instant startTime = Instant.now().minus(2, ChronoUnit.DAYS);

    public void testSearchDetectorAction() throws IOException {
        ingestTestData(indexName, startTime, 1, "test", 3000);
        String detectorType = AnomalyDetectorType.REALTIME_SINGLE_ENTITY.name();
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetector(
                ImmutableList.of(indexName),
                ImmutableList.of(TestHelpers.randomFeature(true)),
                null,
                Instant.now(),
                detectorType,
                1,
                null,
                false
            );
        createDetectorIndex();
        String detectorId = createDetector(detector);

        BoolQueryBuilder query = new BoolQueryBuilder().filter(new TermQueryBuilder(DETECTOR_TYPE_FIELD, detectorType));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest().source(searchSourceBuilder);

        SearchResponse searchResponse = client().execute(SearchAnomalyDetectorAction.INSTANCE, request).actionGet(10000);
        assertEquals(1, searchResponse.getInternalResponse().hits().getTotalHits().value);
        assertEquals(detectorId, searchResponse.getInternalResponse().hits().getAt(0).getId());
    }

    public void testNoIndex() {
        deleteIndexIfExists(AnomalyDetector.ANOMALY_DETECTORS_INDEX);

        BoolQueryBuilder query = new BoolQueryBuilder().filter(new MatchAllQueryBuilder());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest().source(searchSourceBuilder);

        SearchResponse searchResponse = client().execute(SearchAnomalyDetectorAction.INSTANCE, request).actionGet(10000);
        assertEquals(0, searchResponse.getInternalResponse().hits().getTotalHits().value);
    }

}
