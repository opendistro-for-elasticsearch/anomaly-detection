package com.amazon.opendistroforelasticsearch.ad.model;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ValidationTests extends ESTestCase {

    public void testValidationSuggestedChanges() {
        assertEquals("others", ValidationSuggestedChanges.OTHERS.getName());
    }

    public void testValidationFailures() {
        assertEquals("missing", ValidationFailures.MISSING.getName());
    }

    public void testValidationResponse() throws IOException {
        Map<String, List<String>> failuresMap = new HashMap<>();
        Map<String, List<String>> suggestedChanges = new HashMap<>();
        failuresMap.put("missing", Arrays.asList("name"));
        suggestedChanges.put("detection_interval", Arrays.asList("200000"));
        ValidateResponse responseValidate = new ValidateResponse();
        responseValidate.setFailures(failuresMap);
        responseValidate.setSuggestedChanges(suggestedChanges);
        String validation = TestHelpers
                .xContentBuilderToString(responseValidate.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        System.out.println(validation);
        assertEquals("{\"failures\":{\"missing\":[\"name\"]}," + "\"suggestedChanges\":{\"detection_interval\":[\"200000\"]}}", validation);
        assertEquals(failuresMap, responseValidate.getFailures());
        assertEquals(suggestedChanges, responseValidate.getSuggestedChanges());
    }

}
