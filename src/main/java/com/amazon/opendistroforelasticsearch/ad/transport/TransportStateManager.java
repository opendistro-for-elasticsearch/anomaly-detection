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

package com.amazon.opendistroforelasticsearch.ad.transport;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;

/**
 * ADStateManager is used by transport layer to manage AnomalyDetector object
 * and the number of partitions for a detector id.
 *
 */
public class TransportStateManager {
    private static final Logger LOG = LogManager.getLogger(TransportStateManager.class);
    private ConcurrentHashMap<String, TransportState> transportStates;
    private Client client;
    private ModelManager modelManager;
    private NamedXContentRegistry xContentRegistry;
    private ClientUtil clientUtil;
    // map from ES node id to the node's backpressureMuter
    private Map<String, BackPressureRouting> backpressureMuter;
    private final Clock clock;
    private final Settings settings;
    private final Duration stateTtl;

    public static final String NO_ERROR = "no_error";

    public TransportStateManager(
        Client client,
        NamedXContentRegistry xContentRegistry,
        ModelManager modelManager,
        Settings settings,
        ClientUtil clientUtil,
        Clock clock,
        Duration stateTtl
    ) {
        this.transportStates = new ConcurrentHashMap<>();
        this.client = client;
        this.modelManager = modelManager;
        this.xContentRegistry = xContentRegistry;
        this.clientUtil = clientUtil;
        this.backpressureMuter = new ConcurrentHashMap<>();
        this.clock = clock;
        this.settings = settings;
        this.stateTtl = stateTtl;
    }

    /**
     * Get the number of RCF model's partition number for detector adID
     * @param adID detector id
     * @param detector object
     * @return the number of RCF model's partition number for adID
     * @throws LimitExceededException when there is no sufficient resource available
     */
    public int getPartitionNumber(String adID, AnomalyDetector detector) {
        TransportState state = transportStates.get(adID);
        if (state != null && state.getPartitonNumber() > 0) {
            return state.getPartitonNumber();
        }

        int partitionNum = modelManager.getPartitionedForestSizes(detector).getKey();
        state = transportStates.computeIfAbsent(adID, id -> new TransportState(id, clock));
        state.setPartitonNumber(partitionNum);

        return partitionNum;
    }

    public void getAnomalyDetector(String adID, ActionListener<Optional<AnomalyDetector>> listener) {
        TransportState state = transportStates.get(adID);
        if (state != null && state.getDetectorDef() != null) {
            listener.onResponse(Optional.of(state.getDetectorDef()));
        } else {
            GetRequest request = new GetRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX, adID);
            clientUtil.<GetRequest, GetResponse>asyncRequest(request, client::get, onGetDetectorResponse(adID, listener));
        }
    }

    private ActionListener<GetResponse> onGetDetectorResponse(String adID, ActionListener<Optional<AnomalyDetector>> listener) {
        return ActionListener.wrap(response -> {
            if (response == null || !response.isExists()) {
                listener.onResponse(Optional.empty());
                return;
            }

            String xc = response.getSourceAsString();
            LOG.info("Fetched anomaly detector: {}", xc);

            try (
                XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, xc)
            ) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                AnomalyDetector detector = AnomalyDetector.parse(parser, response.getId());
                TransportState state = transportStates.computeIfAbsent(adID, id -> new TransportState(id, clock));
                state.setDetectorDef(detector);

                listener.onResponse(Optional.of(detector));
            } catch (Exception t) {
                LOG.error("Fail to parse detector {}", adID);
                LOG.error("Stack trace:", t);
                listener.onResponse(Optional.empty());
            }
        }, listener::onFailure);
    }

    /**
     * Get a detector's checkpoint and save a flag if we find any so that next time we don't need to do it again
     * @param adID  the detector's ID
     * @param listener listener to handle get request
     */
    public void getDetectorCheckpoint(String adID, ActionListener<Boolean> listener) {
        TransportState state = transportStates.get(adID);
        if (state != null && state.doesCheckpointExists()) {
            listener.onResponse(Boolean.TRUE);
            return;
        }

        GetRequest request = new GetRequest(CommonName.CHECKPOINT_INDEX_NAME, modelManager.getRcfModelId(adID, 0));

        clientUtil.<GetRequest, GetResponse>asyncRequest(request, client::get, onGetCheckpointResponse(adID, listener));
    }

    private ActionListener<GetResponse> onGetCheckpointResponse(String adID, ActionListener<Boolean> listener) {
        return ActionListener.wrap(response -> {
            if (response == null || !response.isExists()) {
                listener.onResponse(Boolean.FALSE);
            } else {
                TransportState state = transportStates.computeIfAbsent(adID, id -> new TransportState(id, clock));
                state.setCheckpointExists(true);
                listener.onResponse(Boolean.TRUE);
            }
        }, listener::onFailure);
    }

    /**
     * Used in delete workflow
     *
     * @param adID detector ID
     */
    public void clear(String adID) {
        transportStates.remove(adID);
    }

    /**
     * Clean states if it is older than our stateTtl. transportState has to be a
     * ConcurrentHashMap otherwise we will have
     * java.util.ConcurrentModificationException.
     *
     */
    public void maintenance() {
        transportStates.entrySet().stream().forEach(entry -> {
            String detectorId = entry.getKey();
            try {
                TransportState state = entry.getValue();
                if (state.expired(stateTtl)) {
                    transportStates.remove(detectorId);
                }
            } catch (Exception e) {
                LOG.warn("Failed to finish maintenance for detector id " + detectorId, e);
            }
        });
    }

    public boolean isMuted(String nodeId) {
        return backpressureMuter.containsKey(nodeId) && backpressureMuter.get(nodeId).isMuted();
    }

    /**
     * When we have a unsuccessful call with a node, increment the backpressure counter.
     * @param nodeId an ES node's ID
     */
    public void addPressure(String nodeId) {
        backpressureMuter.computeIfAbsent(nodeId, k -> new BackPressureRouting(k, clock, settings)).addPressure();
    }

    /**
     * When we have a successful call with a node, clear the backpressure counter.
     * @param nodeId an ES node's ID
     */
    public void resetBackpressureCounter(String nodeId) {
        backpressureMuter.remove(nodeId);
    }

    /**
     * Check if there is running query on given detector
     * @param detector Anomaly Detector
     * @return true if given detector has a running query else false
     */
    public boolean hasRunningQuery(AnomalyDetector detector) {
        return clientUtil.hasRunningQuery(detector);
    }

    /**
     * Get last error of a detector
     * @param adID detector id
     * @return last error for the detector
     */
    public String getLastDetectionError(String adID) {
        return Optional.ofNullable(transportStates.get(adID)).flatMap(state -> state.getLastDetectionError()).orElse(NO_ERROR);
    }

    /**
     * Set last detection error of a detector
     * @param adID detector id
     * @param error error, can be null
     */
    public void setLastDetectionError(String adID, String error) {
        TransportState state = transportStates.computeIfAbsent(adID, id -> new TransportState(id, clock));
        state.setLastDetectionError(error);
    }

    /**
     * Set last cold start error of a detector
     * @param adID detector id
     * @param exception exception, can be null
     */
    public void setLastColdStartException(String adID, AnomalyDetectionException exception) {
        TransportState state = transportStates.computeIfAbsent(adID, id -> new TransportState(id, clock));
        state.setLastColdStartException(exception);
    }

    /**
     * Get last cold start exception of a detector.  The method has side effect.
     * We reset error after calling the method since cold start exception can stop job running.
     * @param adID detector id
     * @return last cold start exception for the detector
     */
    public Optional<AnomalyDetectionException> fetchColdStartException(String adID) {
        TransportState state = transportStates.get(adID);
        if (state == null) {
            return Optional.empty();
        }

        Optional<AnomalyDetectionException> exception = state.getLastColdStartException();
        // since cold start exception can stop job running, we set it to null after using it once.
        exception.ifPresent(e -> setLastColdStartException(adID, null));
        return exception;
    }

    /**
     * Whether last cold start for the detector is running
     * @param adID detector ID
     * @return running or not
     */
    public boolean isColdStartRunning(String adID) {
        TransportState state = transportStates.get(adID);
        if (state != null) {
            return state.isColdStartRunning();
        }

        return false;
    }

    /**
     * Mark the cold start status of the detector
     * @param adID detector ID
     * @param running whether it is running
     */
    public void setColdStartRunning(String adID, boolean running) {
        TransportState state = transportStates.computeIfAbsent(adID, id -> new TransportState(id, clock));
        state.setColdStartRunning(running);
    }
}
