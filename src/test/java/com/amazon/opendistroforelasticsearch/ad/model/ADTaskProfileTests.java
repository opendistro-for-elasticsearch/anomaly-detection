package com.amazon.opendistroforelasticsearch.ad.model;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;

public class ADTaskProfileTests extends ESSingleNodeTestCase {

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    public void testAdTaskProfileSerializationWithAdTask() throws IOException {
        ADTask adTask = TestHelpers.randomAdTask();
        ADTaskProfile adTaskProfile = TestHelpers.randomADTaskProfileWithAdTask(adTask);
        BytesStreamOutput output = new BytesStreamOutput();
        adTaskProfile.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        ADTaskProfile parsedADTaskProfile = new ADTaskProfile(input);
        assertEquals("AD task profile with AD Task serialization doesn't work", adTaskProfile, parsedADTaskProfile);
    }

    public void testAdTaskProfileSerializationWithNullAdTask() throws IOException {
        ADTaskProfile adTaskProfile = TestHelpers.randomADTaskProfileWithAdTask(null);
        BytesStreamOutput output = new BytesStreamOutput();
        adTaskProfile.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        ADTaskProfile parsedADTaskProfile = new ADTaskProfile(input);
        assertEquals("AD task profile with null AD task serialization doesn't work", adTaskProfile, parsedADTaskProfile);
    }
    public void testAdTaskProfileSerializationWithoutAdTask() throws IOException {
        ADTaskProfile adTaskProfile = TestHelpers.randomADTaskProfileWithoutAdTask();
        BytesStreamOutput output = new BytesStreamOutput();
        adTaskProfile.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        ADTaskProfile parsedADTaskProfile = new ADTaskProfile(input);
        assertEquals("AD task profile without Ad Task serialization doesn't work", adTaskProfile, parsedADTaskProfile);
    }

    public void testParseADTaskProfile() throws IOException {
        ADTaskProfile adTaskProfile = TestHelpers.randomADTaskProfileWithoutAdTask();
        adTaskProfile.setNodeId(randomAlphaOfLength(5));
        String adTaskProfileString = TestHelpers.xContentBuilderToString(adTaskProfile.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        ADTaskProfile parsedADTaskProfile = ADTaskProfile.parse(TestHelpers.parser(adTaskProfileString));
        assertEquals("Parsing AD task Profile doesn't work", adTaskProfile, parsedADTaskProfile);
    }

    public void testParseADTaskProfileWithAdTask() throws IOException {
        ADTask adTask = TestHelpers.randomAdTask();
        ADTaskProfile adTaskProfile = TestHelpers.randomADTaskProfileWithAdTask(adTask);
        adTaskProfile.setNodeId(randomAlphaOfLength(5));
        String adTaskProfileString = TestHelpers.xContentBuilderToString(adTaskProfile.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        ADTaskProfile parsedADTaskProfile = ADTaskProfile.parse(TestHelpers.parser(adTaskProfileString));
        assertEquals("Parsing AD task Profile with Ad Task doesn't work", adTaskProfile, parsedADTaskProfile);
    }
}
