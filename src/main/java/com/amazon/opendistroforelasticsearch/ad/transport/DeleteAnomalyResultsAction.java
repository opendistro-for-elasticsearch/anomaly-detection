/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import org.elasticsearch.action.ActionType;
import org.elasticsearch.index.reindex.BulkByScrollResponse;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonValue;

public class DeleteAnomalyResultsAction extends ActionType<BulkByScrollResponse> {
    // External Action which used for public facing RestAPIs.
    public static final String NAME = CommonValue.EXTERNAL_ACTION_PREFIX + "results/delete";
    public static final DeleteAnomalyResultsAction INSTANCE = new DeleteAnomalyResultsAction();

    private DeleteAnomalyResultsAction() {
        super(NAME, BulkByScrollResponse::new);
    }
}
