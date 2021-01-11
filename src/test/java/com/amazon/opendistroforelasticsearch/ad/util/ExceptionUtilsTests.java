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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;

public class ExceptionUtilsTests extends ESTestCase {

    public void testGetShardsFailure() {
        ShardId shardId = new ShardId(randomAlphaOfLength(5), randomAlphaOfLength(5), 1);
        ReplicationResponse.ShardInfo.Failure failure = new ReplicationResponse.ShardInfo.Failure(
            shardId,
            randomAlphaOfLength(5),
            new RuntimeException("test"),
            RestStatus.BAD_REQUEST,
            false
        );
        ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(2, 1, failure);
        IndexResponse indexResponse = new IndexResponse(
            shardId,
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomLong(),
            randomLong(),
            randomLong(),
            randomBoolean()
        );
        indexResponse.setShardInfo(shardInfo);
        String shardsFailure = ExceptionUtil.getShardsFailure(indexResponse);
        assertEquals("RuntimeException[test]", shardsFailure);
    }

    public void testGetShardsFailureWithoutError() {
        ShardId shardId = new ShardId(randomAlphaOfLength(5), randomAlphaOfLength(5), 1);
        IndexResponse indexResponse = new IndexResponse(
            shardId,
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomLong(),
            randomLong(),
            randomLong(),
            randomBoolean()
        );
        assertNull(ExceptionUtil.getShardsFailure(indexResponse));

        ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(2, 1, ReplicationResponse.EMPTY);
        indexResponse.setShardInfo(shardInfo);
        assertNull(ExceptionUtil.getShardsFailure(indexResponse));
    }

    public void testCountInStats() {
        assertTrue(ExceptionUtil.countInStats(new AnomalyDetectionException("test")));
        assertFalse(ExceptionUtil.countInStats(new AnomalyDetectionException("test").countedInStats(false)));
        assertTrue(ExceptionUtil.countInStats(new RuntimeException("test")));
    }

    public void testGetErrorMessage() {
        assertEquals("test", ExceptionUtil.getErrorMessage(new AnomalyDetectionException("test")));
        assertEquals("test", ExceptionUtil.getErrorMessage(new IllegalArgumentException("test")));
        assertEquals("org.elasticsearch.ElasticsearchException: test", ExceptionUtil.getErrorMessage(new ElasticsearchException("test")));
        assertTrue(
            ExceptionUtil
                .getErrorMessage(new RuntimeException("test"))
                .contains("at com.amazon.opendistroforelasticsearch.ad.util.ExceptionUtilsTests.testGetErrorMessage")
        );
    }
}
