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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;

import com.amazon.opendistroforelasticsearch.ad.common.exception.ResourceNotFoundException;

public class ExceptionUtil {
    public static final String RESOURCE_NOT_FOUND_EXCEPTION_NAME_UNDERSCORE = ElasticsearchException
        .getExceptionName(new ResourceNotFoundException("", ""));

    /**
     * Elasticsearch restricts the kind of exceptions can be thrown over the wire
     * (See ElasticsearchException.ElasticsearchExceptionHandle). Since we cannot
     * add our own exception like ResourceNotFoundException without modifying
     * Elasticsearch's code, we have to unwrap the remote transport exception and
     * check its root cause message.
     *
     * @param exception exception thrown locally or over the wire
     * @param expected  expected root cause
     * @param expectedExceptionName expected exception name
     * @return whether the exception wraps the expected exception as the cause
     */
    public static boolean isException(Throwable exception, Class<? extends Exception> expected, String expectedExceptionName) {
        if (exception == null) {
            return false;
        }

        if (expected.isAssignableFrom(exception.getClass())) {
            return true;
        }

        // all exception that has not been registered to sent over wire can be wrapped
        // inside NotSerializableExceptionWrapper.
        // see StreamOutput.writeException
        // ElasticsearchException.getExceptionName(exception) returns exception
        // separated by underscore. For example, ResourceNotFoundException is converted
        // to "resource_not_found_exception".
        if (exception instanceof NotSerializableExceptionWrapper && exception.getMessage().trim().startsWith(expectedExceptionName)) {
            return true;
        }
        return false;
    }

}
