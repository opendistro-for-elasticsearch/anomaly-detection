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

package com.amazon.opendistroforelasticsearch.ad.util;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.REQUEST_TIMEOUT;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;

public class ClientUtil {
    private volatile TimeValue requestTimeout;
    private Client client;
    private final Throttler throttler;

    @Inject
    public ClientUtil(Settings setting, Client client, Throttler throttler) {
        this.requestTimeout = REQUEST_TIMEOUT.get(setting);
        this.client = client;
        this.throttler = throttler;
    }

    /**
     * Send a nonblocking request with a timeout and return response. Blocking is not allowed in a
     * transport call context. See BaseFuture.blockingAllowed
     * @param request request like index/search/get
     * @param LOG log
     * @param consumer functional interface to operate as a client request like client::get
     * @param <Request> ActionRequest
     * @param <Response> ActionResponse
     * @return the response
     * @throws ElasticsearchTimeoutException when we cannot get response within time.
     * @throws IllegalStateException when the waiting thread is interrupted
     */
    public <Request extends ActionRequest, Response extends ActionResponse> Optional<Response> timedRequest(
        Request request,
        Logger LOG,
        BiConsumer<Request, ActionListener<Response>> consumer
    ) {
        try {
            AtomicReference<Response> respReference = new AtomicReference<>();
            final CountDownLatch latch = new CountDownLatch(1);

            consumer
                .accept(
                    request,
                    new LatchedActionListener<Response>(
                        ActionListener
                            .wrap(
                                response -> { respReference.set(response); },
                                exception -> { LOG.error("Cannot get response for request {}, error: {}", request, exception); }
                            ),
                        latch
                    )
                );

            if (!latch.await(requestTimeout.getSeconds(), TimeUnit.SECONDS)) {
                throw new ElasticsearchTimeoutException("Cannot get response within time limit: " + request.toString());
            }
            return Optional.ofNullable(respReference.get());
        } catch (InterruptedException e1) {
            LOG.error(CommonErrorMessages.WAIT_ERR_MSG);
            throw new IllegalStateException(e1);
        }
    }

    /**
     * Send an asynchronous request and handle response with the provided listener.
     * @param <Request> ActionRequest
     * @param <Response> ActionResponse
     * @param request request body
     * @param consumer request method, functional interface to operate as a client request like client::get
     * @param listener needed to handle response
     */
    public <Request extends ActionRequest, Response extends ActionResponse> void asyncRequest(
        Request request,
        BiConsumer<Request, ActionListener<Response>> consumer,
        ActionListener<Response> listener
    ) {
        consumer
            .accept(
                request,
                ActionListener.wrap(response -> { listener.onResponse(response); }, exception -> { listener.onFailure(exception); })
            );
    }

    /**
     * Execute a transport action and handle response with the provided listener.
     * @param <Request> ActionRequest
     * @param <Response> ActionResponse
     * @param action transport action
     * @param request request body
     * @param listener needed to handle response
     */
    public <Request extends ActionRequest, Response extends ActionResponse> void execute(
        Action<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        client
            .execute(
                action,
                request,
                ActionListener.wrap(response -> { listener.onResponse(response); }, exception -> { listener.onFailure(exception); })
            );
    }

    /**
     * Send an synchronous request and handle response with the provided listener.
     *
     * @deprecated use asyncRequest with listener instead.
     *
     * @param <Request> ActionRequest
     * @param <Response> ActionResponse
     * @param request request body
     * @param function request method, functional interface to operate as a client request like client::get
     * @return the response
     */
    @Deprecated
    public <Request extends ActionRequest, Response extends ActionResponse> Response syncRequest(
        Request request,
        Function<Request, ActionFuture<Response>> function
    ) {
        return function.apply(request).actionGet(requestTimeout);
    }

    /**
     * Send a nonblocking request with a timeout and return response. The request will first be put into
     * the negative cache. Once the request complete, it will be removed from the negative cache.
     *
     * @param request request like index/search/get
     * @param LOG log
     * @param consumer functional interface to operate as a client request like client::get
     * @param <Request> ActionRequest
     * @param <Response> ActionResponse
     * @param detector Anomaly Detector
     * @return the response
     * @throws ElasticsearchTimeoutException when we cannot get response within time.
     * @throws IllegalStateException when the waiting thread is interrupted
     */
    public <Request extends ActionRequest, Response extends ActionResponse> Optional<Response> throttledTimedRequest(
        Request request,
        Logger LOG,
        BiConsumer<Request, ActionListener<Response>> consumer,
        AnomalyDetector detector
    ) {
        try {
            throttler.insertFilteredQuery(detector.getDetectorId(), request);
            AtomicReference<Response> respReference = new AtomicReference<>();
            final CountDownLatch latch = new CountDownLatch(1);

            consumer.accept(request, new LatchedActionListener<Response>(ActionListener.wrap(response -> {
                // clear negative cache
                throttler.clearFilteredQuery(detector.getDetectorId());
                respReference.set(response);
            }, exception -> {
                // clear negative cache
                throttler.clearFilteredQuery(detector.getDetectorId());
                LOG.error("Cannot get response for request {}, error: {}", request, exception);
            }), latch));

            if (!latch.await(requestTimeout.getSeconds(), TimeUnit.SECONDS)) {
                throw new ElasticsearchTimeoutException("Cannot get response within time limit: " + request.toString());
            }
            return Optional.ofNullable(respReference.get());
        } catch (InterruptedException e1) {
            LOG.error(CommonErrorMessages.WAIT_ERR_MSG);
            throw new IllegalStateException(e1);
        }
    }

    /**
     * Check if there is running query on given detector
     * @param detector Anomaly Detector
     * @return true if given detector has a running query else false
     */
    public boolean hasRunningQuery(AnomalyDetector detector) {
        return throttler.getFilteredQuery(detector.getDetectorId()).isPresent();
    }
}
