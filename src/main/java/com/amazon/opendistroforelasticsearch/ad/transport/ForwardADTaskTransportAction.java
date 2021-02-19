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
import com.amazon.opendistroforelasticsearch.ad.task.ADTaskCacheManager;
import com.google.common.collect.ImmutableMap;
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

import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.STATE_FIELD;

public class ForwardADTaskTransportAction extends HandledTransportAction<ForwardADTaskRequest, AnomalyDetectorJobResponse> {

    private final ADTaskManager adTaskManager;
//    private final ADTaskCacheManager adTaskCacheManager;
    private final TransportService transportService;

    @Inject
    public ForwardADTaskTransportAction(ActionFilters actionFilters, TransportService transportService, ADTaskManager adTaskManager) {
        super(ForwardADTaskAction.NAME, transportService, actionFilters, ForwardADTaskRequest::new);
        this.adTaskManager = adTaskManager;
        this.transportService = transportService;
//        this.adTaskCacheManager = adTaskCacheManager;
    }

    @Override
    protected void doExecute(Task task, ForwardADTaskRequest request, ActionListener<AnomalyDetectorJobResponse> listener) {
        ADTaskAction adTaskAction = request.getAdTaskAction();
        AnomalyDetector detector = request.getDetector();
        ADTask adTask = request.getAdTask();

        switch (adTaskAction) {
            case START:
                adTaskManager.startHistoricalDetector(detector, request.getDetectionDateRange(), request.getUser(), transportService, listener);
                break;
            case STOP:
                adTaskManager.removeDetectorFromCache(detector.getDetectorId());
                listener.onResponse(new AnomalyDetectorJobResponse(detector.getDetectorId(), 0, 0, 0, RestStatus.OK));
                break;
            case NEXT_ENTITY:
                if (detector.isMultientityDetector()) {
                    if (adTaskManager.hcDetectorDone(detector.getDetectorId())) {
                        adTaskManager.removeDetectorFromCache(detector.getDetectorId());
//                        adTaskManager.updateADTask(adTask.getParentTaskId(), ImmutableMap.of(STATE_FIELD, ADTaskState.FINISHED.name()));
                        listener.onResponse(new AnomalyDetectorJobResponse(detector.getDetectorId(), 0, 0, 0, RestStatus.OK));
                    } else {
                        adTaskManager.runBatchResultActionForEntity(adTask, listener);
                    }
                }
//                else {
//                    adTaskManager.removeDetectorFromCache(detector.getDetectorId());
//                    listener.onResponse(new AnomalyDetectorJobResponse(detector.getDetectorId(), 0, 0, 0, RestStatus.OK));
//                }

                break;
            default:
                listener.onFailure(new ElasticsearchStatusException("Unsupported AD task action " + adTaskAction, RestStatus.BAD_REQUEST));
                break;
        }

    }
}
