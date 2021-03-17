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

package com.amazon.opendistroforelasticsearch.ad.transport;

import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskState;
import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.model.ADTaskAction;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.task.ADTaskManager;

import java.time.Instant;

import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.ERROR_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.EXECUTION_END_TIME_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.STATE_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.TASK_PROGRESS_FIELD;

public class ForwardADTaskTransportAction extends HandledTransportAction<ForwardADTaskRequest, AnomalyDetectorJobResponse> {
    private final Logger logger = LogManager.getLogger(ForwardADTaskTransportAction.class);
    private final ADTaskManager adTaskManager;
//    private final ADTaskCacheManager adTaskCacheManager;
    private final TransportService transportService;

    @Inject
    public ForwardADTaskTransportAction(ActionFilters actionFilters, TransportService transportService, ADTaskManager adTaskManager) {
        super(ForwardADTaskAction.NAME, transportService, actionFilters, ForwardADTaskRequest::new);
        this.adTaskManager = adTaskManager;
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(Task task, ForwardADTaskRequest request, ActionListener<AnomalyDetectorJobResponse> listener) {
        ADTaskAction adTaskAction = request.getAdTaskAction();
        AnomalyDetector detector = request.getDetector();
        String detectorId = detector.getDetectorId();
        ADTask adTask = request.getAdTask();

        switch (adTaskAction) {
            case START:
                adTaskManager.startHistoricalDetector(detector, request.getDetectionDateRange(), request.getUser(), transportService, listener);
                break;
            case FINISHED:
                adTaskManager.removeDetectorFromCache(detectorId);
                listener.onResponse(new AnomalyDetectorJobResponse(detectorId, 0, 0, 0, RestStatus.OK));
                break;
            case NEXT_ENTITY:
                if (detector.isMultientityDetector()) {
                    adTaskManager.removeRunningEntity(detectorId, adTask.getEntity());

//                    if (adTaskManager.hcDetectorDone(detectorId) && adTaskManager.hcDetectorInCache(detectorId)) {
                    if (adTaskManager.hcDetectorDone(detectorId)) {
//                        adTaskManager.updateADTask(adTask.getParentTaskId(), ImmutableMap.of(STATE_FIELD, ADTaskState.FINISHED.name()));
                        listener.onResponse(new AnomalyDetectorJobResponse(detectorId, 0, 0, 0, RestStatus.OK));
                        //TODO: reset task state when get task
                        adTaskManager.updateADHCDetectorTask(detectorId, adTask.getParentTaskId(), ImmutableMap.of(STATE_FIELD, ADTaskState.FINISHED.name(),
                                TASK_PROGRESS_FIELD, 1.0,
                                EXECUTION_END_TIME_FIELD, Instant.now().toEpochMilli()), false);//TODO: check how to handle if no entity case

                        logger.info("Historical HC detector done, will remove from cache, detector id:{}", detectorId);
                        adTaskManager.removeDetectorFromCache(detectorId);
                    } else {
                        logger.debug("Run next entity for detector " + detectorId);
                        adTaskManager.runBatchResultActionForEntity(adTask, listener);
                        adTaskManager.updateADHCDetectorTask(detectorId, adTask.getParentTaskId(),
                                ImmutableMap.of(STATE_FIELD, ADTaskState.RUNNING.name(),
                                TASK_PROGRESS_FIELD, adTaskManager.hcDetectorProgress(detectorId),
                                ERROR_FIELD, adTask.getError() != null? adTask.getError() : ""), true);
                    }
                } else {
                    listener.onFailure(new IllegalArgumentException("Can only get HC entity task"));
                }
//                else {
//                    adTaskManager.removeDetectorFromCache(detectorId);
//                    listener.onResponse(new AnomalyDetectorJobResponse(detectorId, 0, 0, 0, RestStatus.OK));
//                }

                break;
            case PUSH_BACK_ENTITY:
                if (detector.isMultientityDetector() && adTask.isEntityTask()) {
                    adTaskManager.removeRunningEntity(detectorId, adTask.getEntity());
                    logger.warn("Push back entity to cache : " + adTask.getEntity().get(0).getValue());
                    adTaskManager.addEntityToCache(adTask.getDetectorId(), adTask.getEntity().get(0).getValue());
                    adTaskManager.runBatchResultActionForEntity(adTask, listener);
                } else {
                    listener.onFailure(new IllegalArgumentException("Can only push back entity task"));
                }

                break;

            case CANCEL:
                if (detector.isMultientityDetector()/* && adTask.isEntityTask()*/) {
                    adTaskManager.removePendingEntities(detectorId);
                    adTaskManager.removeRunningEntity(detectorId, adTask.getEntity());
                    if (adTaskManager.hcDetectorDone(detectorId) || !adTask.isEntityTask()) {
                        String taskId = adTask.isEntityTask()? adTask.getParentTaskId() : adTask.getTaskId();
                        logger.info("Set HC task as stopped : task id {}", taskId);
                        adTaskManager.updateADHCDetectorTask(detectorId, taskId,
                                ImmutableMap.of(STATE_FIELD, ADTaskState.STOPPED.name(),
                                        EXECUTION_END_TIME_FIELD, Instant.now().toEpochMilli()), true);//TODO: change to false?
                        logger.info("Historical HC detector done, will remove from cache, detector id:{}", detectorId);
                        adTaskManager.removeDetectorFromCache(detectorId);
                    }
                    listener.onResponse(new AnomalyDetectorJobResponse(adTask.getTaskId(), 0, 0, 0, RestStatus.OK));
                } else {
                    listener.onFailure(new IllegalArgumentException("Only support cancel HC now"));
                }

                break;
            default:
                listener.onFailure(new ElasticsearchStatusException("Unsupported AD task action " + adTaskAction, RestStatus.BAD_REQUEST));
                break;
        }

    }
}
