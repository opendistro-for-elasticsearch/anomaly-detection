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

package com.amazon.opendistroforelasticsearch.ad.model;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public enum ProfileName {
    STATE("state"),
    ERROR("error");

    private String name;

    ProfileName(String name) {
        this.name = name;
    }

    /**
     * Get profile name
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     * Get set of profile names
     *
     * @return set of profile names
     */
    public static Set<String> getNames() {
        Set<String> names = new HashSet<>();

        for (ProfileName statName : ProfileName.values()) {
            names.add(statName.getName());
        }
        return names;
    }

    public static ProfileName getName(String name) {
        switch (name) {
            case "state":
                return STATE;
            case "error":
                return ERROR;
            default:
                throw new IllegalArgumentException("Unsupported profile types");
        }
    }

    public static Set<ProfileName> getNames(Collection<String> names) {
        Set<ProfileName> res = new HashSet<>();
        for (String name : names) {
            res.add(getName(name));
        }
        return res;
    }
}
