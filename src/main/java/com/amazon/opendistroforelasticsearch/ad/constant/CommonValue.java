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

package com.amazon.opendistroforelasticsearch.ad.constant;

public class CommonValue {
    // unknown or no schema version
    public static Integer NO_SCHEMA_VERSION = 0;
    public static String INTERNAL_ACTION_PREFIX = "cluster:admin/opendistro/adinternal/";
    public static String EXTERNAL_ACTION_PREFIX = "cluster:admin/opendistro/ad/";
}
