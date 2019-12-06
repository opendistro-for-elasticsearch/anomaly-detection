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

package com.amazon.opendistroforelasticsearch.ad.stats.suppliers;

import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import java.util.function.Supplier;

/**
 * DocumentCountSupplier provides the number of documents for a given index
 */
public class DocumentCountSupplier implements Supplier<Long> {
    private AnomalyDetectionIndices indices;
    private String indexName;

    /**
     * Constructor
     * @param indices AnomalyDetectionIndices
     * @param indexName IndexName
     */
    public DocumentCountSupplier(AnomalyDetectionIndices indices, String indexName) {
        this.indices = indices;
        this.indexName = indexName;
    }

    @Override
    public Long get() {
        return indices.getNumberOfDocumentsInIndex(indexName);
    }
}