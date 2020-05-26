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

package com.amazon.opendistroforelasticsearch.ad.ml;

import java.util.Objects;

/**
 * Data object containing thresholding results.
 */
public class ThresholdingResult {

    private final double grade;
    private final double confidence;

    /**
     * Constructor with all arguments.
     *
     * @param grade anomaly grade
     * @param confidence confidence for the grade
     */
    public ThresholdingResult(double grade, double confidence) {
        this.grade = grade;
        this.confidence = confidence;
    }

    /**
     * Returns the anomaly grade.
     *
     * @return the anoamly grade
     */
    public double getGrade() {
        return grade;
    }

    /**
     * Returns the confidence for the grade.
     *
     * @return confidence for the grade
     */
    public double getConfidence() {
        return confidence;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ThresholdingResult that = (ThresholdingResult) o;
        return Objects.equals(this.grade, that.grade) && Objects.equals(this.confidence, that.confidence);
    }

    @Override
    public int hashCode() {
        return Objects.hash(grade, confidence);
    }
}
