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

package com.amazon.opendistroforelasticsearch.ad.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

public class ClusterStateUtils {
    private static final Logger LOG = LogManager.getLogger(ClusterStateUtils.class);
    private final ClusterService clusterService;
    private final Map<String, String> ignoredAttributes = new HashMap<String, String>();

    @Inject
    public ClusterStateUtils(ClusterService clusterService) {
        this.clusterService = clusterService;
        ignoredAttributes.put(CommonName.BOX_TYPE_KEY, CommonName.WARM_BOX_TYPE);
    }

    public ImmutableOpenMap<String, DiscoveryNode> getEligibleDataNodes() {
        ImmutableOpenMap<String, DiscoveryNode> dataNodes = clusterService.state().nodes().getDataNodes();
        ImmutableOpenMap.Builder<String, DiscoveryNode> modelNodes = ImmutableOpenMap.builder();

        for (Iterator<ObjectObjectCursor<String, DiscoveryNode>> it = dataNodes.iterator(); it.hasNext();) {
            ObjectObjectCursor<String, DiscoveryNode> cursor = it.next();
            if (!isIgnoredNode(cursor.value)) {
                modelNodes.put(cursor.key, cursor.value);
            }
        }
        return modelNodes.build();
    }

    public boolean isIgnoredNode(DiscoveryNode node) {
        if (!node.isDataNode()) {
            return true;
        }
        for (Map.Entry<String, String> entry : ignoredAttributes.entrySet()) {
            String attribute = node.getAttributes().get(entry.getKey());
            if (attribute != null && attribute.equals(entry.getValue())) {
                return true;
            }
        }
        return false;
    }
}
