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

package com.amazon.opendistroforelasticsearch.ad.stats;

import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.CounterSupplier;

import java.util.function.Supplier;

/**
 * Class represents a stat the plugin keeps track of
 */
public class ADStat<T> {
    private String name;
    private Boolean clusterLevel;
    private Supplier<T> supplier;

    public ADStat(String name, Boolean clusterLevel, Supplier<T> supplier) {
        this.name = name;
        this.clusterLevel = clusterLevel;
        this.supplier = supplier;
    }

    public String getName() {
        return name;
    }

    public Boolean isClusterLevel() {
        return clusterLevel;
    }

    /**
     * Get the value
     * @return T value of the stat
     */
    public T getValue() {
        return supplier.get();
    }

    /**
     * If the supplier can be incremented, increment it
     */
    public void increment() {
        if (supplier instanceof CounterSupplier) {
            ((CounterSupplier) supplier).increment();
        }
    }
}