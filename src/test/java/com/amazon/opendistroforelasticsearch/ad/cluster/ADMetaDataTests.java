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

package com.amazon.opendistroforelasticsearch.ad.cluster;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaData.Custom;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Before;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class ADMetaDataTests extends AbstractADTest {
    AnomalyDetectorGraveyard deadDetector1;
    AnomalyDetectorGraveyard deadDetector2;
    AnomalyDetectorGraveyard deadDetector3;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        deadDetector1 = new AnomalyDetectorGraveyard("123", 1L);
        deadDetector2 = new AnomalyDetectorGraveyard("cnuRGW4BJpEADCIShyj9", 1572387170588L);
        deadDetector3 = new AnomalyDetectorGraveyard("456", 2L);
    }

    public void testDiffAdded() throws IOException {
        ADMetaData currentMeta = new ADMetaData(deadDetector1);
        ADMetaData newMeta = new ADMetaData(deadDetector1, deadDetector2);
        ADMetaData newMeta2 = new ADMetaData(deadDetector3);

        ADMetaData.ADMetaDataDiff diff = (ADMetaData.ADMetaDataDiff) newMeta.diff(currentMeta);

        try (BytesStreamOutput outputStream = new BytesStreamOutput()) {
            diff.writeTo(outputStream);
            byte[] diffBytes = BytesReference.toBytes(outputStream.bytes());

            NamedDiff<Custom> diffRead;
            NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());

            try (
                StreamInput input = StreamInput.wrap(diffBytes);
                StreamInput namedInput = new NamedWriteableAwareStreamInput(input, namedWriteableRegistry)
            ) {
                diffRead = new ADMetaData.ADMetaDataDiff(namedInput);
                Custom newMeta3 = diffRead.apply(newMeta2);

                assertTrue(newMeta3 instanceof ADMetaData);
                ADMetaData meta = (ADMetaData) newMeta3;
                assertEquals(2, meta.getAnomalyDetectorGraveyard().size());
                for (AnomalyDetectorGraveyard detector : meta.getAnomalyDetectorGraveyard()) {
                    assertTrue(detector.equals(deadDetector2) || detector.equals(deadDetector3));
                }
            }
        }
    }

    public void testDiffRemoved() throws IOException {
        ADMetaData currentMeta = new ADMetaData(deadDetector1, deadDetector2);
        ADMetaData newMeta = new ADMetaData(deadDetector1);
        ADMetaData newMeta2 = new ADMetaData(deadDetector2);

        ADMetaData.ADMetaDataDiff diff = (ADMetaData.ADMetaDataDiff) newMeta.diff(currentMeta);

        try (BytesStreamOutput outputStream = new BytesStreamOutput()) {
            diff.writeTo(outputStream);
            byte[] diffBytes = BytesReference.toBytes(outputStream.bytes());

            NamedDiff<Custom> diffRead;
            NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());

            try (
                StreamInput input = StreamInput.wrap(diffBytes);
                StreamInput namedInput = new NamedWriteableAwareStreamInput(input, namedWriteableRegistry)
            ) {
                diffRead = new ADMetaData.ADMetaDataDiff(namedInput);
                Custom newMeta3 = diffRead.apply(newMeta2);

                assertTrue(newMeta3 instanceof ADMetaData);
                ADMetaData meta = (ADMetaData) newMeta3;
                assertEquals(0, meta.getAnomalyDetectorGraveyard().size());
            }
        }
    }

    public void testDiffCurrentEmpty() throws IOException {
        ADMetaData currentMeta = new ADMetaData(deadDetector2);

        try (BytesStreamOutput outputStream = new BytesStreamOutput()) {
            currentMeta.writeTo(outputStream);
            byte[] currentMetaBytes = BytesReference.toBytes(outputStream.bytes());
            StreamInput input = StreamInput.wrap(currentMetaBytes);
            ADMetaData readCurrentMeta = new ADMetaData(input);
            ADMetaData.ADMetaDataDiff diff = (ADMetaData.ADMetaDataDiff) ADMetaData.EMPTY_METADATA.diff(readCurrentMeta);

            ADMetaData meta = (ADMetaData) diff.apply(currentMeta);
            assertEquals(0, meta.getAnomalyDetectorGraveyard().size());
        }
    }

    public void testDiffPreviousEmpty() throws IOException {
        ADMetaData currentMeta = new ADMetaData(deadDetector2);

        ADMetaData newMeta2 = new ADMetaData(deadDetector3);

        try (BytesStreamOutput outputStream = new BytesStreamOutput()) {
            currentMeta.writeTo(outputStream);
            byte[] currentMetaBytes = BytesReference.toBytes(outputStream.bytes());
            StreamInput input = StreamInput.wrap(currentMetaBytes);
            ADMetaData readCurrentMeta = new ADMetaData(input);
            ADMetaData.ADMetaDataDiff diff = (ADMetaData.ADMetaDataDiff) readCurrentMeta.diff(ADMetaData.EMPTY_METADATA);

            ADMetaData meta = (ADMetaData) diff.apply(newMeta2);
            assertEquals(2, meta.getAnomalyDetectorGraveyard().size());

            for (AnomalyDetectorGraveyard detector : meta.getAnomalyDetectorGraveyard()) {
                assertTrue(detector.equals(deadDetector2) || detector.equals(deadDetector3));
            }
        }
    }

    @SuppressWarnings("resource")
    public void testParse() throws IOException {
        Type type = new TypeToken<Map<String, Object>>() {
        }.getType();
        Gson gson = new Gson();
        String detectorID = "9EQ9120BDBl4sOnjHuIZ";
        long epoch = 1571274501462L;
        Map<String, Object> adMetaMap = gson
            .fromJson(
                " {\"ad\":{\""
                    + ADMetaData.DETECTOR_GRAVEYARD_FIELD
                    + "\":[{\""
                    + AnomalyDetectorGraveyard.DETECTOR_ID_KEY
                    + "\":\""
                    + detectorID
                    + "\",\""
                    + AnomalyDetectorGraveyard.DELETE_TIME_KEY
                    + "\":"
                    + epoch
                    + "}]}}",
                type
            );

        try (
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(adMetaMap);
            XContentParser parser = XContentType.JSON
                .xContent()
                .createParser(
                    new NamedXContentRegistry(new AnomalyDetectorPlugin().getNamedXContent()),
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    BytesReference.bytes(xContentBuilder).streamInput()
                )
        ) {
            XContentParser.Token token = parser.nextToken();
            assertTrue(token == XContentParser.Token.START_OBJECT);
            token = parser.nextToken();
            assertTrue(token == XContentParser.Token.FIELD_NAME);
            ADMetaData adMetaObj = (ADMetaData) parser.namedObject(MetaData.Custom.class, ADMetaData.TYPE, null);
            assertThat(adMetaObj, is(notNullValue()));
            Set<AnomalyDetectorGraveyard> deadDetectors = adMetaObj.getAnomalyDetectorGraveyard();
            assertEquals(1, deadDetectors.size());
            for (AnomalyDetectorGraveyard detector : deadDetectors) {
                assertEquals(detectorID, detector.getDetectorID());
                assertEquals(epoch, detector.getDeleteEpochMillis());
            }
        }
    }
}
