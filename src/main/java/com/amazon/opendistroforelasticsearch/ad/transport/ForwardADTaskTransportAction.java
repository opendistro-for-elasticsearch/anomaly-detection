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
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.update.UpdateResponse;
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
import static com.amazon.opendistroforelasticsearch.ad.util.ExceptionUtil.getErrorMessage;

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
                adTaskManager.startAnomalyDetector(detector, request.getDetectionDateRange(), request.getUser(), transportService, listener);
                break;
            case FINISHED:
                adTaskManager.removeDetectorFromCache(detectorId);
                listener.onResponse(new AnomalyDetectorJobResponse(detectorId, 0, 0, 0, RestStatus.OK));
                break;
            case NEXT_ENTITY:
                logger.info("3333333333 Received task for NEXT_ENTITY action: {}", adTask.getTaskId());
                if (detector.isMultientityDetector()) {
                    adTaskManager.removeRunningEntity(detectorId, adTask.getEntity());

//                    if (adTaskManager.hcDetectorDone(detectorId) && adTaskManager.hcDetectorInCache(detectorId)) {
                    logger.info("3333333333 HC detector done: {}, taskId: {}, detectorId: {}", adTaskManager.hcDetectorDone(detectorId), adTask.getTaskId(), detectorId);
                    if (adTaskManager.hcDetectorDone(detectorId)) {
                        logger.info("3333333333-2 Historical HC detector done, will remove from cache, detector id:{}", detectorId);
//                        adTaskManager.updateADTask(adTask.getParentTaskId(), ImmutableMap.of(STATE_FIELD, ADTaskState.FINISHED.name()));
                        listener.onResponse(new AnomalyDetectorJobResponse(detectorId, 0, 0, 0, RestStatus.OK));
                        //TODO: reset task state when get task
//                        String taskId = adTask.getTaskType().equals(ADTaskType.HISTORICAL_HC_ENTITY.name()) ? adTask.getParentTaskId() : adTask.getTaskId();
//                        if (!adTask.isEntityTask() && adTask.getError() != null) {
//                            logger.info("+++++++++++++++ set task as failed {}", adTask.getTaskId());
//                            setHCDetectorTaskDone(adTask, ADTaskState.FAILED, adTask.getError(), listener);
////                            adTaskManager.updateADHCDetectorTask(detectorId, taskId, ImmutableMap.of(STATE_FIELD, ADTaskState.FAILED.name(),
////                                    TASK_PROGRESS_FIELD, 1.0,
////                                    ERROR_FIELD, adTask.getError(),
////                                    EXECUTION_END_TIME_FIELD, Instant.now().toEpochMilli()));//TODO: check how to handle if no entity case, if there is only 1 entity, false will not work
//                        } else {
//                            setHCDetectorTaskDone(adTask, ADTaskState.FINISHED, null, listener);
//                            /*String detectorTaskId = adTask.isEntityTask()? adTask.getParentTaskId() : adTask.getTaskId();
//                            adTaskManager.countEntityTasks(detectorTaskId, ImmutableList.of(ADTaskState.FINISHED), ActionListener.wrap(r -> {
//                                ADTaskState hcDetectorTaskState = ADTaskState.FINISHED;
//                                if (r == 0) {
//                                    hcDetectorTaskState = ADTaskState.FAILED;
//                                }
//                                adTaskManager.updateADHCDetectorTask(detectorId, taskId, ImmutableMap.of(STATE_FIELD, hcDetectorTaskState.name(),
//                                        TASK_PROGRESS_FIELD, 1.0,
//                                        EXECUTION_END_TIME_FIELD, Instant.now().toEpochMilli()));
//                            }, e -> {
//                                logger.error("Failed to get finished entity tasks", e);
//                                adTaskManager.updateADHCDetectorTask(detectorId, taskId, ImmutableMap.of(STATE_FIELD, ADTaskState.FINISHED.name(),
//                                        TASK_PROGRESS_FIELD, 1.0,
//                                        EXECUTION_END_TIME_FIELD, Instant.now().toEpochMilli()));//TODO: check how to handle if no entity case, if there is only 1 entity, false will not work
//                            }));*/
//                        }
                        ADTaskState state = !adTask.isEntityTask() && adTask.getError() != null ? ADTaskState.FAILED : ADTaskState.FINISHED;
                        logger.info("+++++++++++++++ set task as {} for task {}", state.name(), adTask.getTaskId());
                        setHCDetectorTaskDone(adTask, state, adTask.getError(), listener);

//                        adTaskManager.removeDetectorFromCache(detectorId);
                    } else {
                        logger.info("3333333333-3 Run next entity for detector " + detectorId);
                        adTaskManager.runBatchResultActionForEntity(adTask, listener);
                        adTaskManager.updateADHCDetectorTask(detectorId, adTask.getParentTaskId(),
                                ImmutableMap.of(STATE_FIELD, ADTaskState.RUNNING.name(),
                                TASK_PROGRESS_FIELD, adTaskManager.hcDetectorProgress(detectorId),
                                ERROR_FIELD, adTask.getError() != null? adTask.getError() : ""));
                    }
                } else {
                    logger.warn("Can only handle HC entity task for NEXT_ENTITY action, taskId:{} , taskType:{}", adTask.getTaskId(), adTask.getTaskType());
                    listener.onFailure(new IllegalArgumentException("Can only get HC entity task"));
//                    listener.onResponse(new AnomalyDetectorJobResponse("cc", 0,0,0,RestStatus.OK));
                }
//                else {
//                    adTaskManager.removeDetectorFromCache(detectorId);
//                    listener.onResponse(new AnomalyDetectorJobResponse(detectorId, 0, 0, 0, RestStatus.OK));
//                }

                break;
            case PUSH_BACK_ENTITY:
                logger.info("3333333333 Received task for PUSH_BACK_ENTITY action: {}, HC detector done: {}",
                        adTask.getTaskId(), adTaskManager.hcDetectorDone(detectorId));
                if (detector.isMultientityDetector() && adTask.isEntityTask()) {
                    adTaskManager.removeRunningEntity(detectorId, adTask.getEntity());
//                    logger.warn("Push back entity to cache : " + adTask.getEntity().get(0).getValue());
//                    if (adTaskManager.retryableError(adTask.getError()) && !adTaskManager.taskRetryExceedLimits(detectorId, adTask.getTaskId())) {
//                        adTaskManager.pushBackEntityToCache(adTask.getTaskId(), adTask.getDetectorId(), adTask.getEntity().get(0).getValue());
//                    } else {
//                        logger.warn("Task retry exceed limits. Task id: {}, entity: {}", adTask.getTaskId(), adTask.getEntity().get(0).getValue());
//                    }

                    if (adTaskManager.hcDetectorDone(detectorId)) {
                        setHCDetectorTaskDone(adTask, ADTaskState.FINISHED, null, listener);
                    } else {
                        adTaskManager.runBatchResultActionForEntity(adTask, listener);
                    }

                } else {
                    logger.warn("3333333333 Can only push back entity task");
                    listener.onFailure(new IllegalArgumentException("Can only push back entity task"));
//                    listener.onResponse(new AnomalyDetectorJobResponse("aa", 0,0,0,RestStatus.OK));
                }

                break;

            case CANCEL:
                if (detector.isMultientityDetector()/* && adTask.isEntityTask()*/) {
                    adTaskManager.clearPendingEntities(detectorId);
                    adTaskManager.removeRunningEntity(detectorId, adTask.getEntity());
                    if (adTaskManager.hcDetectorDone(detectorId) || !adTask.isEntityTask()) {
//                        String taskId = adTask.isEntityTask()? adTask.getParentTaskId() : adTask.getTaskId();
//                        logger.info("Set HC task as stopped : task id {}", taskId);
//                        adTaskManager.updateADHCDetectorTask(detectorId, taskId,
//                                ImmutableMap.of(STATE_FIELD, ADTaskState.STOPPED.name(),
//                                        EXECUTION_END_TIME_FIELD, Instant.now().toEpochMilli()));//TODO: change to false?
//                        logger.info("Historical HC detector run cancelled. Remove from cache, detector id:{}", detectorId);
//                        adTaskManager.removeDetectorFromCache(detectorId);
                        setHCDetectorTaskDone(adTask, ADTaskState.STOPPED, adTask.getError(), listener);
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

    private void setHCDetectorTaskDone(ADTask adTask, ADTaskState state, String errorMsg,  ActionListener<AnomalyDetectorJobResponse> listener) {
        String detectorId = adTask.getDetectorId();
        String taskId = adTask.isEntityTask()? adTask.getParentTaskId() : adTask.getTaskId();
//        logger.info("Set HC task as {} : task id {}", state.name(), taskId);

        String detectorTaskId = adTask.isEntityTask()? adTask.getParentTaskId() : adTask.getTaskId();

        ActionListener<UpdateResponse> wrappedListener = ActionListener.wrap(response -> {
            logger.info("Historical HC detector done with state: {}. Remove from cache, detector id:{}",
                    state.name(), detectorId);
            adTaskManager.removeDetectorFromCache(detectorId);
//                    if (response.status() == RestStatus.OK) {
//                        logger.debug("Updated AD task successfully: {}, taskId: {}", response.status(), taskId);
//                    } else {
//                        logger.error("Failed to update AD task {}, status: {}", taskId, response.status());
//                    }
        }, e -> {
            logger.error("Failed to update task: " + taskId, e);
            logger.info("Historical HC detector done with state: {}. Remove from cache, detector id:{}",
                    state.name(), detectorId);
            adTaskManager.removeDetectorFromCache(detectorId);
        });

        if (state == ADTaskState.FINISHED) {
//            logger.info("9999999999999999999999999999 search ad entity finished tasks");
            adTaskManager.countEntityTasks(detectorTaskId, ImmutableList.of(ADTaskState.FINISHED), ActionListener.wrap(r -> {
                logger.info("+++++++++++++++ count ad finished entity tasks, {}", r);
                ADTaskState hcDetectorTaskState = r == 0 ? ADTaskState.FAILED : ADTaskState.FINISHED;
                adTaskManager.updateADHCDetectorTask(detectorId, taskId, ImmutableMap.of(STATE_FIELD, hcDetectorTaskState.name(),
                        TASK_PROGRESS_FIELD, 1.0,
                        EXECUTION_END_TIME_FIELD, Instant.now().toEpochMilli()), wrappedListener);
            }, e -> {
                logger.error("Failed to get finished entity tasks", e);
                adTaskManager.updateADHCDetectorTask(detectorId, taskId, ImmutableMap.of(STATE_FIELD, ADTaskState.FAILED.name(),
                        TASK_PROGRESS_FIELD, 1.0,
                        ERROR_FIELD, getErrorMessage(e),
                        EXECUTION_END_TIME_FIELD, Instant.now().toEpochMilli()), wrappedListener);//TODO: check how to handle if no entity case, if there is only 1 entity, false will not work
            }));
        } else {
            adTaskManager.updateADHCDetectorTask(detectorId, taskId,
                    ImmutableMap.of(STATE_FIELD, state.name(),
                            ERROR_FIELD, adTask.getError(),
                            EXECUTION_END_TIME_FIELD, Instant.now().toEpochMilli()),
                    wrappedListener);
        }

        listener.onResponse(new AnomalyDetectorJobResponse(taskId, 0,0,0,RestStatus.OK));
    }
}
