package com.amazon.opendistroforelasticsearch.ad.model;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;

public class ModelProfileTests extends ESSingleNodeTestCase {

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    public void testModelProfileSerialization() throws IOException {
        ModelProfile modelProfile = TestHelpers.randomModelProfile();
        BytesStreamOutput output = new BytesStreamOutput();
        modelProfile.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        ModelProfile parsedModelProfile = new ModelProfile(input);
        assertEquals("Model Profile Serialization doesn't work", modelProfile, parsedModelProfile);
    }



}
