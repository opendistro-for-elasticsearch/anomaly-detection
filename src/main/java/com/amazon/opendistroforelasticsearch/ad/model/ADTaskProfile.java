/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import com.amazon.opendistroforelasticsearch.ad.annotation.Generated;
import com.google.common.base.Objects;

/**
 * One anomaly detection task means one detector starts to run until stopped.
 */
public class ADTaskProfile implements ToXContentObject, Writeable, Writeable.Writer, Writeable.Reader {

    public static final String AD_TASK_FIELD = "ad_task";
    public static final String SHINGLE_SIZE_FIELD = "shingle_size";
    public static final String RCF_TOTAL_UPDATES_FIELD = "rcf_total_updates";
    public static final String THRESHOLD_MODEL_TRAINED_FIELD = "threshold_model_trained";
    public static final String THRESHOLD_MODEL_TRAINING_DATA_SIZE_FIELD = "threshold_model_training_data_size";
    public static final String MODEL_SIZE_IN_BYTES = "model_size_in_bytes";
    public static final String NODE_ID_FIELD = "node_id";
    public static final String ENTITY_FIELD = "entity";
    public static final String TASK_ID_FIELD = "task_id";
    public static final String AD_TASK_TYPE_FIELD = "task_type";
    public static final String TOTAL_ENTITIES_FIELD = "total_entities";
    public static final String PENDING_ENTITIES_FIELD = "pending_entities";
    public static final String RUNNING_ENTITIES_FIELD = "running_entities";

    private ADTask adTask;
    private Integer shingleSize;
    private Long rcfTotalUpdates;
    private Boolean thresholdModelTrained;
    private Integer thresholdModelTrainingDataSize;
    private Long modelSizeInBytes;
    private String nodeId;
    private List<Entity> entity;
    private String taskId;
    private ADTaskType adTaskType;
    private Integer totalEntities;
    private Integer pendingEntities;
    private Integer runningEntities;

    public ADTaskProfile(
        Integer shingleSize,
        Long rcfTotalUpdates,
        Boolean thresholdModelTrained,
        Integer thresholdModelTrainingDataSize,
        Long modelSizeInBytes,
        String nodeId
    ) {
        this(null, shingleSize, rcfTotalUpdates, thresholdModelTrained, thresholdModelTrainingDataSize, modelSizeInBytes, nodeId);
    }

    public ADTaskProfile(
            String nodeId,
            Integer totalEntities,
            Integer pendingEntities,
            Integer runningEntities
    ) {
        this(null, null, null, null, null, null,
                nodeId, null, null,
                totalEntities, pendingEntities, runningEntities);
    }

    public ADTaskProfile(
            Integer shingleSize,
            Long rcfTotalUpdates,
            Boolean thresholdModelTrained,
            Integer thresholdModelTrainingDataSize,
            Long modelSizeInBytes,
            String nodeId,
            List<Entity> entity,
            String taskId
    ) {
        this(null, shingleSize, rcfTotalUpdates, thresholdModelTrained, thresholdModelTrainingDataSize, modelSizeInBytes, nodeId, entity,
                taskId,
                null, null, null);
    }

    public ADTaskProfile(
        ADTask adTask,
        Integer shingleSize,
        Long rcfTotalUpdates,
        Boolean thresholdModelTrained,
        Integer thresholdModelTrainingDataSize,
        Long modelSizeInBytes,
        String nodeId
    ) {
        this.adTask = adTask;
        this.shingleSize = shingleSize;
        this.rcfTotalUpdates = rcfTotalUpdates;
        this.thresholdModelTrained = thresholdModelTrained;
        this.thresholdModelTrainingDataSize = thresholdModelTrainingDataSize;
        this.modelSizeInBytes = modelSizeInBytes;
        this.nodeId = nodeId;
    }

    public ADTaskProfile(
            ADTask adTask,
            Integer shingleSize,
            Long rcfTotalUpdates,
            Boolean thresholdModelTrained,
            Integer thresholdModelTrainingDataSize,
            Long modelSizeInBytes,
            String nodeId,
            List<Entity> entity,
            String taskId,
            Integer totalEntities,
            Integer pendingEntities,
            Integer runningEntities
    ) {
        this.adTask = adTask;
        this.shingleSize = shingleSize;
        this.rcfTotalUpdates = rcfTotalUpdates;
        this.thresholdModelTrained = thresholdModelTrained;
        this.thresholdModelTrainingDataSize = thresholdModelTrainingDataSize;
        this.modelSizeInBytes = modelSizeInBytes;
        this.nodeId = nodeId;
        this.entity = entity;
        this.taskId = taskId;
        this.totalEntities = totalEntities;
        this.pendingEntities = pendingEntities;
        this.runningEntities = runningEntities;
        if (entity != null && entity.size() > 0) {
            setAdTaskType(ADTaskType.HISTORICAL_HC_ENTITY);
        } else if (pendingEntities != null || runningEntities != null) {
            setAdTaskType(ADTaskType.HISTORICAL_HC_DETECTOR);
        }
//        if(adTask != null) {
//            setAdTaskType(adTask.getTaskType());
//        }
    }

    public ADTaskType getAdTaskType() {
        return adTaskType;
    }

    public ADTaskProfile(StreamInput input) throws IOException {
        if (input.readBoolean()) {
            this.adTask = new ADTask(input);
        } else {
            this.adTask = null;
        }
        this.shingleSize = input.readOptionalInt();
        this.rcfTotalUpdates = input.readOptionalLong();
        this.thresholdModelTrained = input.readOptionalBoolean();
        this.thresholdModelTrainingDataSize = input.readOptionalInt();
        this.modelSizeInBytes = input.readOptionalLong();
        this.nodeId = input.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (adTask != null) {
            out.writeBoolean(true);
            adTask.writeTo(out);
        } else {
            out.writeBoolean(false);
        }

        out.writeOptionalInt(shingleSize);
        out.writeOptionalLong(rcfTotalUpdates);
        out.writeOptionalBoolean(thresholdModelTrained);
        out.writeOptionalInt(thresholdModelTrainingDataSize);
        out.writeOptionalLong(modelSizeInBytes);
        out.writeOptionalString(nodeId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        if (adTask != null) {
            xContentBuilder.field(AD_TASK_FIELD, adTask);
        }
        if (shingleSize != null) {
            xContentBuilder.field(SHINGLE_SIZE_FIELD, shingleSize);
        }
        if (rcfTotalUpdates != null) {
            xContentBuilder.field(RCF_TOTAL_UPDATES_FIELD, rcfTotalUpdates);
        }
        if (thresholdModelTrained != null) {
            xContentBuilder.field(THRESHOLD_MODEL_TRAINED_FIELD, thresholdModelTrained);
        }
        if (thresholdModelTrainingDataSize != null) {
            xContentBuilder.field(THRESHOLD_MODEL_TRAINING_DATA_SIZE_FIELD, thresholdModelTrainingDataSize);
        }
        if (modelSizeInBytes != null) {
            xContentBuilder.field(MODEL_SIZE_IN_BYTES, modelSizeInBytes);
        }
        if (nodeId != null) {
            xContentBuilder.field(NODE_ID_FIELD, nodeId);
        }
        if (entity != null && entity.size() > 0) {
            xContentBuilder.field(ENTITY_FIELD, entity.toArray());
        }
        if (adTaskType != null) {
            xContentBuilder.field(AD_TASK_TYPE_FIELD, adTaskType);
        }
        if (totalEntities != null) {
            xContentBuilder.field(TOTAL_ENTITIES_FIELD, totalEntities);
        }
        if (pendingEntities != null) {
            xContentBuilder.field(PENDING_ENTITIES_FIELD, pendingEntities);
        }
        if (runningEntities != null) {
            xContentBuilder.field(RUNNING_ENTITIES_FIELD, runningEntities);
        }
        return xContentBuilder.endObject();
    }

    public static ADTaskProfile parse(XContentParser parser) throws IOException {
        ADTask adTask = null;
        Integer shingleSize = null;
        Long rcfTotalUpdates = null;
        Boolean thresholdModelTrained = null;
        Integer thresholdNodelTrainingDataSize = null;
        Long modelSizeInBytes = null;
        String nodeId = null;
        List<Entity> entity = null;
        String taskId = null;
        String adTaskType = null;
        Integer totalEntities = null;
        Integer pendingEntities = null;
        Integer runningEntities = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case AD_TASK_FIELD:
                    adTask = ADTask.parse(parser);
                    break;
                case SHINGLE_SIZE_FIELD:
                    shingleSize = parser.intValue();
                    break;
                case RCF_TOTAL_UPDATES_FIELD:
                    rcfTotalUpdates = parser.longValue();
                    break;
                case THRESHOLD_MODEL_TRAINED_FIELD:
                    thresholdModelTrained = parser.booleanValue();
                    break;
                case THRESHOLD_MODEL_TRAINING_DATA_SIZE_FIELD:
                    thresholdNodelTrainingDataSize = parser.intValue();
                    break;
                case MODEL_SIZE_IN_BYTES:
                    modelSizeInBytes = parser.longValue();
                    break;
                case NODE_ID_FIELD:
                    nodeId = parser.text();
                    break;
                case ENTITY_FIELD:
                    entity = new ArrayList<>();
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        entity.add(Entity.parse(parser));
                    }
                    break;
                case TASK_ID_FIELD:
                    taskId = parser.text();
                    break;
                case AD_TASK_TYPE_FIELD:
                    adTaskType = parser.text();
                    break;
                case TOTAL_ENTITIES_FIELD:
                    totalEntities = parser.intValue();
                    break;
                case PENDING_ENTITIES_FIELD:
                    pendingEntities = parser.intValue();
                    break;
                case RUNNING_ENTITIES_FIELD:
                    runningEntities = parser.intValue();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return new ADTaskProfile(
                adTask,
                shingleSize,
                rcfTotalUpdates,
                thresholdModelTrained,
                thresholdNodelTrainingDataSize,
                modelSizeInBytes,
                nodeId,
                entity, taskId, totalEntities, pendingEntities, runningEntities
        );
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ADTaskProfile that = (ADTaskProfile) o;
        return Objects.equal(getAdTask(), that.getAdTask())
            && Objects.equal(getShingleSize(), that.getShingleSize())
            && Objects.equal(getRcfTotalUpdates(), that.getRcfTotalUpdates())
            && Objects.equal(getThresholdModelTrained(), that.getThresholdModelTrained())
            && Objects.equal(getModelSizeInBytes(), that.getModelSizeInBytes())
            && Objects.equal(getNodeId(), that.getNodeId())
            && Objects.equal(getThresholdModelTrainingDataSize(), that.getThresholdModelTrainingDataSize());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects
            .hashCode(
                adTask,
                shingleSize,
                rcfTotalUpdates,
                thresholdModelTrained,
                thresholdModelTrainingDataSize,
                modelSizeInBytes,
                nodeId
            );
    }

    public ADTask getAdTask() {
        return adTask;
    }

    public void setAdTask(ADTask adTask) {
        this.adTask = adTask;
    }

    public Integer getShingleSize() {
        return shingleSize;
    }

    public void setShingleSize(Integer shingleSize) {
        this.shingleSize = shingleSize;
    }

    public Long getRcfTotalUpdates() {
        return rcfTotalUpdates;
    }

    public void setRcfTotalUpdates(Long rcfTotalUpdates) {
        this.rcfTotalUpdates = rcfTotalUpdates;
    }

    public Boolean getThresholdModelTrained() {
        return thresholdModelTrained;
    }

    public void setThresholdModelTrained(Boolean thresholdModelTrained) {
        this.thresholdModelTrained = thresholdModelTrained;
    }

    public Integer getThresholdModelTrainingDataSize() {
        return thresholdModelTrainingDataSize;
    }

    public void setThresholdModelTrainingDataSize(Integer thresholdModelTrainingDataSize) {
        this.thresholdModelTrainingDataSize = thresholdModelTrainingDataSize;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public Long getModelSizeInBytes() {
        return modelSizeInBytes;
    }

    public void setModelSizeInBytes(Long modelSizeInBytes) {
        this.modelSizeInBytes = modelSizeInBytes;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public void setAdTaskType(ADTaskType adTaskType) {
        this.adTaskType = adTaskType;
    }

    @Override
    public String toString() {
        return "ADTaskProfile{"
            + "adTask="
            + adTask
            + ", shingleSize="
            + shingleSize
            + ", rcfTotalUpdates="
            + rcfTotalUpdates
            + ", thresholdModelTrained="
            + thresholdModelTrained
            + ", thresholdNodelTrainingDataSize="
            + thresholdModelTrainingDataSize
            + ", modelSizeInBytes="
            + modelSizeInBytes
            + ", nodeId='"
            + nodeId
            + '\''
            + '}';
    }

    @Override
    public Object read(StreamInput in) throws IOException {
        return new ADTaskProfile(in);
    }

    @Override
    public void write(StreamOutput out, Object value) throws IOException {
        if (value instanceof ADTaskProfile) {
            ((ADTaskProfile)value).writeTo(out);
        }
    }
}
