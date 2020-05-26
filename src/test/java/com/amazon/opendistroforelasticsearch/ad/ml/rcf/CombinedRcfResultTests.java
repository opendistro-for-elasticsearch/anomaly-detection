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

package com.amazon.opendistroforelasticsearch.ad.ml.rcf;

import static org.junit.Assert.assertEquals;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class CombinedRcfResultTests {

    private double score = 1.;
    private double confidence = .5;
    private CombinedRcfResult rcfResult = new CombinedRcfResult(score, confidence);

    @Test
    public void getters_returnExcepted() {
        assertEquals(score, rcfResult.getScore(), 1e-8);
        assertEquals(confidence, rcfResult.getConfidence(), 1e-8);
    }

    private Object[] equalsData() {
        return new Object[] {
            new Object[] { rcfResult, null, false },
            new Object[] { rcfResult, rcfResult, true },
            new Object[] { rcfResult, 1, false },
            new Object[] { rcfResult, new CombinedRcfResult(score, confidence), true },
            new Object[] { rcfResult, new CombinedRcfResult(score + 1, confidence), false },
            new Object[] { rcfResult, new CombinedRcfResult(score, confidence + 1), false },
            new Object[] { rcfResult, new CombinedRcfResult(score + 1, confidence + 1), false }, };
    }

    @Test
    @Parameters(method = "equalsData")
    public void equals_returnExpected(CombinedRcfResult result, Object other, boolean expected) {
        assertEquals(expected, result.equals(other));
    }

    private Object[] hashCodeData() {
        return new Object[] {
            new Object[] { rcfResult, new CombinedRcfResult(score, confidence), true },
            new Object[] { rcfResult, new CombinedRcfResult(score + 1, confidence), false },
            new Object[] { rcfResult, new CombinedRcfResult(score, confidence + 1), false },
            new Object[] { rcfResult, new CombinedRcfResult(score + 1, confidence + 1), false }, };
    }

    @Test
    @Parameters(method = "hashCodeData")
    public void hashCode_returnExpected(CombinedRcfResult result, CombinedRcfResult other, boolean expected) {
        assertEquals(expected, result.hashCode() == other.hashCode());
    }
}
