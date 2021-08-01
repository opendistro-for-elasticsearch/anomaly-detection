package com.amazon.opendistroforelasticsearch.ad.model;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;

public class DetectorProfileTests extends ESSingleNodeTestCase {

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    public void testDetectorProfileSerialization() throws IOException {
        DetectorProfile detectorProfile = TestHelpers.randomDetectorProfile();
        BytesStreamOutput output = new BytesStreamOutput();
        detectorProfile.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        DetectorProfile parsedDetectorProfile = new DetectorProfile(input);
        assertEquals("Detector profile serialization doesn't work", detectorProfile, parsedDetectorProfile);
    }

    public void testDetectorProfileMerge() throws IOException {
        DetectorProfile detectorProfile = TestHelpers.randomDetectorProfile();
        DetectorProfile mergeDetectorProfile = TestHelpers
                .randomDetectorProfileWithEntitiesCount(randomLongBetween(1, 5), randomLongBetween(5, 10));
        detectorProfile.merge(mergeDetectorProfile);
        assertEquals(detectorProfile.getTotalEntities(), mergeDetectorProfile.getTotalEntities());
        assertEquals(detectorProfile.getActiveEntities(), mergeDetectorProfile.getActiveEntities());
    }
}
