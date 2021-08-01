package com.amazon.opendistroforelasticsearch.ad.model;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;

public class EntityTests extends ESSingleNodeTestCase {

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    public void testEntitySerialization() throws IOException {
        Entity entity = TestHelpers.randomEntity();
        BytesStreamOutput output = new BytesStreamOutput();
        entity.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        Entity parsedEntity = new Entity(input);
        assertEquals("Entity serialization doesn't work", entity, parsedEntity);
    }

    public void testEntitySerializationWithNullValue() throws Exception {
        TestHelpers.assertFailWith(
                NullPointerException.class,
                () -> {
                    Entity entity = TestHelpers.createEntity(randomAlphaOfLength(5), null);
                    BytesStreamOutput output = new BytesStreamOutput();
                    entity.writeTo(output);
                    return entity;
                }
        );
    }

    public void testParseEntity() throws IOException {
        Entity entity = TestHelpers.randomEntity();
        String parsedEntityString = TestHelpers.xContentBuilderToString(
                entity.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        Entity paredEntity = Entity.parse(TestHelpers.parser(parsedEntityString));
        assertEquals("Parsing entity doesn't work", entity, paredEntity);
    }

}
