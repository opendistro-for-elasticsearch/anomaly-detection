/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;

import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.common.exception.JsonPathNotFoundException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonMessageAttributes;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.RcfResult;
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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.hamcrest.Matchers;
import test.com.amazon.opendistroforelasticsearch.ad.util.JsonDeserializer;

public class RCFResultTests extends ESTestCase {
    Gson gson = new GsonBuilder().create();

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
        ADCircuitBreakerService adCircuitBreakerService = mock(ADCircuitBreakerService.class);
        RCFResultTransportAction action = new RCFResultTransportAction(
            mock(ActionFilters.class),
            transportService,
            manager,
            adCircuitBreakerService
        );
        when(manager.getRcfResult(any(String.class), any(String.class), any(double[].class))).thenReturn(new RcfResult(0, 0, 25));
        when(adCircuitBreakerService.isOpen()).thenReturn(false);

        final PlainActionFuture<RCFResultResponse> future = new PlainActionFuture<>();
        RCFResultRequest request = new RCFResultRequest("123", "123-rcf-1", new double[] { 0 });
        action.doExecute(mock(Task.class), request, future);

        RCFResultResponse response = future.actionGet();
        assertEquals(0, response.getRCFScore(), 0.001);
        assertEquals(25, response.getForestSize(), 0.001);
    }

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
        ADCircuitBreakerService adCircuitBreakerService = mock(ADCircuitBreakerService.class);
        RCFResultTransportAction action = new RCFResultTransportAction(
            mock(ActionFilters.class),
            transportService,
            manager,
            adCircuitBreakerService
        );
        doThrow(NullPointerException.class).when(manager).getRcfResult(any(String.class), any(String.class), any(double[].class));
        when(adCircuitBreakerService.isOpen()).thenReturn(false);

        final PlainActionFuture<RCFResultResponse> future = new PlainActionFuture<>();
        RCFResultRequest request = new RCFResultRequest("123", "123-rcf-1", new double[] { 0 });
        action.doExecute(mock(Task.class), request, future);

        expectThrows(NullPointerException.class, () -> future.actionGet());
    }

    public void testSerialzationResponse() throws IOException {
        RCFResultResponse response = new RCFResultResponse(0.3, 0, 26);
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        RCFResultResponse readResponse = RCFResultAction.INSTANCE.getResponseReader().read(streamInput);
        assertThat(response.getForestSize(), equalTo(readResponse.getForestSize()));
        assertThat(response.getRCFScore(), equalTo(readResponse.getRCFScore()));
    }

    public void testIncorrectSerialzationResponse() throws IOException {
        expectThrows(UnsupportedOperationException.class, () -> RCFResultAction.INSTANCE.newResponse());
    }

    public void testJsonResponse() throws IOException, JsonPathNotFoundException {
        RCFResultResponse response = new RCFResultResponse(0.3, 0, 26);
        XContentBuilder builder = jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String json = Strings.toString(builder);
        assertEquals(JsonDeserializer.getDoubleValue(json, RCFResultResponse.RCF_SCORE_JSON_KEY), response.getRCFScore(), 0.001);
        assertEquals(JsonDeserializer.getDoubleValue(json, RCFResultResponse.FOREST_SIZE_JSON_KEY), response.getForestSize(), 0.001);
    }

    public void testEmptyID() {
        ActionRequestValidationException e = new RCFResultRequest(null, "123-rcf-1", new double[] { 0 }).validate();
        assertThat(e.validationErrors(), Matchers.hasItem(CommonErrorMessages.AD_ID_MISSING_MSG));
    }

    public void testFeatureIsNull() {
        ActionRequestValidationException e = new RCFResultRequest("123", "123-rcf-1", null).validate();
        assertThat(e.validationErrors(), hasItem(RCFResultRequest.INVALID_FEATURE_MSG));
    }

    public void testSerialzationRequest() throws IOException {
        RCFResultRequest response = new RCFResultRequest("123", "123-rcf-1", new double[] { 0 });
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        RCFResultRequest readResponse = new RCFResultRequest(streamInput);
        assertThat(response.getAdID(), equalTo(readResponse.getAdID()));
        assertThat(response.getFeatures(), equalTo(readResponse.getFeatures()));
    }

    public void testJsonRequest() throws IOException, JsonPathNotFoundException {
        RCFResultRequest request = new RCFResultRequest("123", "123-rcf-1", new double[] { 0 });
        XContentBuilder builder = jsonBuilder();
        request.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String json = Strings.toString(builder);
        assertEquals(JsonDeserializer.getTextValue(json, CommonMessageAttributes.ID_JSON_KEY), request.getAdID());
        assertArrayEquals(
            JsonDeserializer.getDoubleArrayValue(json, CommonMessageAttributes.FEATURE_JSON_KEY),
            request.getFeatures(),
            0.001
        );
    }

    public void testCircuitBreaker() {
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
        ADCircuitBreakerService breakerService = mock(ADCircuitBreakerService.class);
        RCFResultTransportAction action = new RCFResultTransportAction(
            mock(ActionFilters.class),
            transportService,
            manager,
            breakerService
        );
        when(manager.getRcfResult(any(String.class), any(String.class), any(double[].class))).thenReturn(new RcfResult(0, 0, 25));
        when(breakerService.isOpen()).thenReturn(true);

        final PlainActionFuture<RCFResultResponse> future = new PlainActionFuture<>();
        RCFResultRequest request = new RCFResultRequest("123", "123-rcf-1", new double[] { 0 });
        action.doExecute(mock(Task.class), request, future);

        expectThrows(LimitExceededException.class, () -> future.actionGet());
    }
}
