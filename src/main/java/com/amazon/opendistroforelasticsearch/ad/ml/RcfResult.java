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

package com.amazon.opendistroforelasticsearch.ad.ml;

import java.util.Objects;

/**
 * Data object containing results from RCF models.
 */
public class RcfResult {

    private final double score;
    private final double confidence;
    private final int forestSize;

    /**
     * Constructor with all arguments.
     *
     * @param score RCF score
     * @param confidence RCF confidence
     * @param forestSize number of RCF trees used for the score
     */
    public RcfResult(double score, double confidence, int forestSize) {
        this.score = score;
        this.confidence = confidence;
        this.forestSize = forestSize;
    }

    /**
     * Returns the RCF score.
     *
     * @return the RCF score
     */
    public double getScore() {
        return score;
    }

    /**
     * Returns the RCF confidence.
     *
     * @return the RCF confidence
     */
    public double getConfidence() {
        return confidence;
    }

    /**
     * Returns the number of RCF trees used for the score.
     *
     * @return the number of RCF trees used for the score
     */
    public int getForestSize() {
        return forestSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        RcfResult that = (RcfResult) o;
        return Objects.equals(this.score, that.score)
            && Objects.equals(this.confidence, that.confidence)
            && Objects.equals(this.forestSize, that.forestSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(score, confidence, forestSize);
    }
}
