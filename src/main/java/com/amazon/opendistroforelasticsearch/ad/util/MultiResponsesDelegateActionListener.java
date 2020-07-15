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

package com.amazon.opendistroforelasticsearch.ad.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;

import com.amazon.opendistroforelasticsearch.ad.model.Mergeable;

/**
 * A listener wrapper to help send multiple requests asynchronously and return one final responses together
 */
public class MultiResponsesDelegateActionListener<T extends Mergeable> implements ActionListener<T> {
    private static final Logger LOG = LogManager.getLogger(MultiResponsesDelegateActionListener.class);
    static final String NO_RESPONSE = "No response collected";

    private final ActionListener<T> delegate;
    private final AtomicInteger collectedResponseCount;
    private final int maxResponseCount;
    // save responses from multiple requests
    private final List<T> savedResponses;
    private List<String> exceptions;
    private String finalErrorMsg;

    public MultiResponsesDelegateActionListener(ActionListener<T> delegate, int maxResponseCount, String finalErrorMsg) {
        this.delegate = delegate;
        this.collectedResponseCount = new AtomicInteger(0);
        this.maxResponseCount = maxResponseCount;
        this.savedResponses = Collections.synchronizedList(new ArrayList<T>());
        this.exceptions = Collections.synchronizedList(new ArrayList<String>());
        this.finalErrorMsg = finalErrorMsg;
    }

    @Override
    public void onResponse(T response) {
        try {
            if (response != null) {
                this.savedResponses.add(response);
            }
        } finally {
            // If expectedResponseCount == 0 , collectedResponseCount.incrementAndGet() will be greater than expectedResponseCount
            if (collectedResponseCount.incrementAndGet() >= maxResponseCount) {
                finish();
            }
        }

    }

    @Override
    public void onFailure(Exception e) {
        LOG.error(e);
        try {
            this.exceptions.add(e.getMessage());
        } finally {
            // no matter the asynchronous request is a failure or success, we need to increment the count.
            // We need finally here to increment the count when there is a failure.
            if (collectedResponseCount.incrementAndGet() >= maxResponseCount) {
                finish();
            }
        }
    }

    private void finish() {
        if (this.exceptions.size() == 0) {
            if (savedResponses.size() == 0) {
                this.delegate.onFailure(new RuntimeException(NO_RESPONSE));
            } else {
                T response0 = savedResponses.get(0);
                for (int i = 1; i < savedResponses.size(); i++) {
                    response0.merge(savedResponses.get(i));
                }
                this.delegate.onResponse(response0);
            }
        } else {
            this.delegate.onFailure(new RuntimeException(String.format(Locale.ROOT, finalErrorMsg + " Exceptions: %s", exceptions)));
        }
    }

    public void failImmediately(Exception e) {
        this.delegate.onFailure(new RuntimeException(finalErrorMsg, e));
    }

    public void failImmediately(String errMsg) {
        this.delegate.onFailure(new RuntimeException(errMsg));
    }

    public void failImmediately(String errMsg, Exception e) {
        this.delegate.onFailure(new RuntimeException(errMsg, e));
    }

    public void respondImmediately(T o) {
        this.delegate.onResponse(o);
    }
}
