package com.amazon.opendistroforelasticsearch.ad.model;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class DetectorInternalStateTests extends ESTestCase {

    public void testParseDetectorInternalState() throws IOException {
        DetectorInternalState detectorInternalState = TestHelpers.randomDetectorInternalState();
        String parsedEntityString = TestHelpers.xContentBuilderToString(
                detectorInternalState.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        DetectorInternalState parsedDetectorInternalState = DetectorInternalState.parse(TestHelpers.parser(parsedEntityString));
        assertEquals("Parsing Detector Internal state doesn't work", detectorInternalState, parsedDetectorInternalState);
    }

    public void testCloneDetectorInternalState() {
        DetectorInternalState detectorInternalState = TestHelpers.randomDetectorInternalState();
        DetectorInternalState clonedInternalState = (DetectorInternalState)detectorInternalState.clone();
        assertEquals("Cloning Detector Internal state doesn't work", detectorInternalState, clonedInternalState);
    }

}
