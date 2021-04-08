package com.amazon.opendistroforelasticsearch.ad.ratelimit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonValue;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import com.google.common.collect.ImmutableList;

public class ResultWriteQueueTests extends AbstractADTest {
    public void testDeserializeIndexRequest() throws IOException {
        AnomalyResult detectResult = new AnomalyResult(
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            0.8,
            Double.NaN,
            Double.NaN,
            ImmutableList.of(),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            null,
            null,
            randomAlphaOfLength(5),
            null,
            null,
            CommonValue.NO_SCHEMA_VERSION
        );

        try (XContentBuilder builder = jsonBuilder()) {
            IndexRequest indexRequest = new IndexRequest(CommonName.ANOMALY_RESULT_INDEX_ALIAS).source(detectResult.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE));
            BytesReference indexSource = indexRequest.source();
            XContentType indexContentType = indexRequest.getContentType();
            final BytesArray array = (BytesArray) indexSource;
            try (
                XContentParser xContentParser = XContentHelper
                    .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, indexSource, indexContentType)

//                XContentParser xContentParser = XContentFactory.xContent(indexContentType)
//                .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, array.array(), array.offset(), array.length())
            ) {
                LOG.info("hello:"+indexContentType);
                LOG.info("hello2:"+indexSource);
                LOG.info("hello3:"+indexSource);
                xContentParser.nextToken();
                AnomalyResult result = AnomalyResult.parse(xContentParser);
                LOG.info("hello4:"+result.getAnomalyScore());
                assertEquals(result.getAnomalyScore(), 0.8, 0.001);
            }
        }

    }
}
