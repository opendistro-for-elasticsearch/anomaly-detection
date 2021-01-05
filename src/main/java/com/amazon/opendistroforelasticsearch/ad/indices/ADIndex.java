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

package com.amazon.opendistroforelasticsearch.ad.indices;

import java.util.function.Supplier;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorInternalState;
import com.amazon.opendistroforelasticsearch.ad.util.ThrowingSupplierWrapper;

/**
 * Represent an AD index
 *
 */
public enum ADIndex {

    // throw RuntimeException since we don't know how to handle the case when the mapping reading throws IOException
    RESULT(
        CommonName.ANOMALY_RESULT_INDEX_ALIAS,
        true,
        ThrowingSupplierWrapper.throwingSupplierWrapper(AnomalyDetectionIndices::getAnomalyResultMappings)
    ),
    CONFIG(
        AnomalyDetector.ANOMALY_DETECTORS_INDEX,
        false,
        ThrowingSupplierWrapper.throwingSupplierWrapper(AnomalyDetectionIndices::getAnomalyDetectorMappings)
    ),
    JOB(
        AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX,
        false,
        ThrowingSupplierWrapper.throwingSupplierWrapper(AnomalyDetectionIndices::getAnomalyDetectorJobMappings)
    ),
    CHECKPOINT(
        CommonName.CHECKPOINT_INDEX_NAME,
        false,
        ThrowingSupplierWrapper.throwingSupplierWrapper(AnomalyDetectionIndices::getCheckpointMappings)
    ),
    STATE(
        DetectorInternalState.DETECTOR_STATE_INDEX,
        false,
        ThrowingSupplierWrapper.throwingSupplierWrapper(AnomalyDetectionIndices::getDetectionStateMappings)
    );

    private final String indexName;
    // whether we use an alias for the index
    private final boolean alias;
    private final String mapping;

    ADIndex(String name, boolean alias, Supplier<String> mappingSupplier) {
        this.indexName = name;
        this.alias = alias;
        this.mapping = mappingSupplier.get();
    }

    public String getIndexName() {
        return indexName;
    }

    public boolean isAlias() {
        return alias;
    }

    public String getMapping() {
        return mapping;
    }

}
