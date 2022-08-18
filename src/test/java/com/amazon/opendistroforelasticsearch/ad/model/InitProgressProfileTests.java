package com.amazon.opendistroforelasticsearch.ad.model;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;

import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;

public class InitProgressProfileTests extends ESSingleNodeTestCase {

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    public void testInitProgressProfileSerialization() throws IOException {
        InitProgressProfile initProgressProfile = TestHelpers.randomInitProgressProfile();
        BytesStreamOutput output = new BytesStreamOutput();
        initProgressProfile.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        InitProgressProfile parsedInitProgressProfile = new InitProgressProfile(input);
        assertEquals("InitProgressProfile serialization doesn't work",
                initProgressProfile, parsedInitProgressProfile);
    }

    public void testInitProgressProfileSerializationWithNegativeMinLeft() throws Exception {
        TestHelpers.assertFailWith(
                IllegalStateException.class,
                "Negative longs unsupported",
                () -> {
                    InitProgressProfile initProgressProfile = TestHelpers.createInitProgressProfile(randomAlphaOfLength(5), -2, randomInt());
                    BytesStreamOutput output = new BytesStreamOutput();
                    initProgressProfile.writeTo(output);
                    return initProgressProfile;
                }
        );
    }
}
