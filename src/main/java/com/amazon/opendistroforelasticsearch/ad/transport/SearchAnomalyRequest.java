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

import java.io.IOException;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class SearchAnomalyRequest extends ActionRequest {

    private SearchRequest searchRequest;
    private String authHeader;

    public SearchAnomalyRequest(StreamInput in) throws IOException {
        super(in);
        searchRequest = new SearchRequest(in);
        authHeader = in.readOptionalString();
    }

    public SearchAnomalyRequest(SearchRequest searchRequest, String authHeader) throws IOException {
        super();
        this.searchRequest = searchRequest;
        this.authHeader = authHeader;
    }

    public SearchRequest getSearchRequest() {
        return searchRequest;
    }

    public String getAuthHeader() {
        return authHeader;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        searchRequest.writeTo(out);
        out.writeOptionalString(authHeader);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
