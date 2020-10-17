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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Collections;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;

import test.com.amazon.opendistroforelasticsearch.ad.util.JsonDeserializer;

import com.amazon.opendistroforelasticsearch.ad.common.exception.JsonPathNotFoundException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonMessageAttributes;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingResult;

public class ThresholdResultTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testNormal() {
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );

        ModelManager manager = mock(ModelManager.class);
        ThresholdResultTransportAction action = new ThresholdResultTransportAction(mock(ActionFilters.class), transportService, manager);
        doAnswer(invocation -> {
            ActionListener<ThresholdingResult> listener = invocation.getArgument(3);
            listener.onResponse(new ThresholdingResult(0, 1.0d, 0.2));
            return null;
        }).when(manager).getThresholdingResult(any(String.class), any(String.class), anyDouble(), any(ActionListener.class));

        final PlainActionFuture<ThresholdResultResponse> future = new PlainActionFuture<>();
        ThresholdResultRequest request = new ThresholdResultRequest("123", "123-threshold", 2);
        action.doExecute(mock(Task.class), request, future);

        ThresholdResultResponse response = future.actionGet();
        assertEquals(0, response.getAnomalyGrade(), 0.001);
        assertEquals(1, response.getConfidence(), 0.001);
    }

    @SuppressWarnings("unchecked")
    public void testExecutionException() {
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );

        ModelManager manager = mock(ModelManager.class);
        ThresholdResultTransportAction action = new ThresholdResultTransportAction(mock(ActionFilters.class), transportService, manager);
        doThrow(NullPointerException.class)
            .when(manager)
            .getThresholdingResult(any(String.class), any(String.class), anyDouble(), any(ActionListener.class));

        final PlainActionFuture<ThresholdResultResponse> future = new PlainActionFuture<>();
        ThresholdResultRequest request = new ThresholdResultRequest("123", "123-threshold", 2);
        action.doExecute(mock(Task.class), request, future);

        expectThrows(NullPointerException.class, () -> future.actionGet());
    }

    public void testSerialzationResponse() throws IOException {
        ThresholdResultResponse response = new ThresholdResultResponse(1, 0.8);
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        ThresholdResultResponse readResponse = ThresholdResultAction.INSTANCE.getResponseReader().read(streamInput);
        assertThat(response.getAnomalyGrade(), equalTo(readResponse.getAnomalyGrade()));
        assertThat(response.getConfidence(), equalTo(readResponse.getConfidence()));
    }

    public void testJsonResponse() throws IOException, JsonPathNotFoundException {
        ThresholdResultResponse response = new ThresholdResultResponse(1, 0.8);
        XContentBuilder builder = jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String json = Strings.toString(builder);
        assertEquals(
            JsonDeserializer.getDoubleValue(json, CommonMessageAttributes.ANOMALY_GRADE_JSON_KEY),
            response.getAnomalyGrade(),
            0.001
        );
        assertEquals(JsonDeserializer.getDoubleValue(json, CommonMessageAttributes.CONFIDENCE_JSON_KEY), response.getConfidence(), 0.001);
    }

    public void testEmptyID() {
        ActionRequestValidationException e = new ThresholdResultRequest(null, "123-threshold", 2).validate();
        assertThat(e.validationErrors(), Matchers.hasItem(CommonErrorMessages.AD_ID_MISSING_MSG));
    }

    public void testSerialzationRequest() throws IOException {
        ThresholdResultRequest response = new ThresholdResultRequest("123", "123-threshold", 2);
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        ThresholdResultRequest readResponse = new ThresholdResultRequest(streamInput);
        assertThat(response.getAdID(), equalTo(readResponse.getAdID()));
        assertThat(response.getRCFScore(), equalTo(readResponse.getRCFScore()));
    }

    public void testJsonRequest() throws IOException, JsonPathNotFoundException {
        ThresholdResultRequest request = new ThresholdResultRequest("123", "123-threshold", 2);
        XContentBuilder builder = jsonBuilder();
        request.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String json = Strings.toString(builder);
        assertEquals(JsonDeserializer.getTextValue(json, CommonMessageAttributes.ID_JSON_KEY), request.getAdID());
        assertEquals(JsonDeserializer.getDoubleValue(json, CommonMessageAttributes.RCF_SCORE_JSON_KEY), request.getRCFScore(), 0.001);
    }
}
