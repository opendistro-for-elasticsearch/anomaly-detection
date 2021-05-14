/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package com.amazon.opendistroforelasticsearch.ad.transport;

import java.io.IOException;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;

public class ValidateAnomalyDetectorRequest extends ActionRequest {

    private final AnomalyDetector detector;
    private final String typeStr;
    private final Integer maxSingleEntityAnomalyDetectors;
    private final Integer maxMultiEntityAnomalyDetectors;
    private final Integer maxAnomalyFeatures;
    private final TimeValue requestTimeout;

    public ValidateAnomalyDetectorRequest(StreamInput in) throws IOException {
        super(in);
        detector = new AnomalyDetector(in);
        typeStr = in.readString();
        maxSingleEntityAnomalyDetectors = in.readInt();
        maxMultiEntityAnomalyDetectors = in.readInt();
        maxAnomalyFeatures = in.readInt();
        requestTimeout = in.readTimeValue();
    }

    public ValidateAnomalyDetectorRequest(
            AnomalyDetector detector,
            String typeStr,
            Integer maxSingleEntityAnomalyDetectors,
            Integer maxMultiEntityAnomalyDetectors,
            Integer maxAnomalyFeatures,
            TimeValue requestTimeout
    ) {
        this.detector = detector;
        this.typeStr = typeStr;
        this.maxSingleEntityAnomalyDetectors = maxSingleEntityAnomalyDetectors;
        this.maxMultiEntityAnomalyDetectors = maxMultiEntityAnomalyDetectors;
        this.maxAnomalyFeatures = maxAnomalyFeatures;
        this.requestTimeout = requestTimeout;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        detector.writeTo(out);
        out.writeString(typeStr);
        out.writeInt(maxSingleEntityAnomalyDetectors);
        out.writeInt(maxMultiEntityAnomalyDetectors);
        out.writeInt(maxAnomalyFeatures);
        out.writeTimeValue(requestTimeout);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public AnomalyDetector getDetector() {
        return detector;
    }

    public String getTypeStr() {
        return typeStr;
    }

    public Integer getMaxSingleEntityAnomalyDetectors() {
        return maxSingleEntityAnomalyDetectors;
    }

    public Integer getMaxMultiEntityAnomalyDetectors() {
        return maxMultiEntityAnomalyDetectors;
    }

    public Integer getMaxAnomalyFeatures() {
        return maxAnomalyFeatures;
    }

    public TimeValue getRequestTimeout() {
        return requestTimeout;
    }
}
