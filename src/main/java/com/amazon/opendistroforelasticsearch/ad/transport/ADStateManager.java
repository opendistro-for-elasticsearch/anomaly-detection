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

package com.amazon.opendistroforelasticsearch.ad.transport;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import com.amazon.randomcutforest.RandomCutForest;

/**
 * ADStateManager is used by transport layer to manage AnomalyDetector object
 * and the number of partitions for a detector id.
 *
 */
public class ADStateManager {
    private static final Logger LOG = LogManager.getLogger(ADStateManager.class);
    private ConcurrentHashMap<String, Entry<AnomalyDetector, Instant>> currentDetectors;
    private ConcurrentHashMap<String, Entry<Integer, Instant>> partitionNumber;
    private Client client;
    private Random random;
    private ModelManager modelManager;
    private NamedXContentRegistry xContentRegistry;
    private ClientUtil clientUtil;
    // map from ES node id to the node's backpressureMuter
    private Map<String, BackPressureRouting> backpressureMuter;
    private final Clock clock;
    private final Settings settings;
    private final Duration stateTtl;

    public ADStateManager(
        Client client,
        NamedXContentRegistry xContentRegistry,
        ModelManager modelManager,
        Settings settings,
        ClientUtil clientUtil,
        Clock clock,
        Duration stateTtl
    ) {
        this.currentDetectors = new ConcurrentHashMap<>();
        this.client = client;
        this.random = new Random();
        this.modelManager = modelManager;
        this.xContentRegistry = xContentRegistry;
        this.partitionNumber = new ConcurrentHashMap<>();
        this.clientUtil = clientUtil;
        this.backpressureMuter = new ConcurrentHashMap<>();
        this.clock = clock;
        this.settings = settings;
        this.stateTtl = stateTtl;
    }

    /**
     * Get the number of RCF model's partition number for detector adID
     * @param adID detector id
     * @return the number of RCF model's partition number for adID
     * @throws InterruptedException when we cannot get anomaly detector object for adID before timeout
     * @throws LimitExceededException when there is no sufficient resource available
     */
    public int getPartitionNumber(String adID) throws InterruptedException {
        Entry<Integer, Instant> partitonAndTime = partitionNumber.get(adID);
        if (partitonAndTime != null) {
            partitonAndTime.setValue(clock.instant());
            return partitonAndTime.getKey();
        }

        Optional<AnomalyDetector> detector = getAnomalyDetector(adID);
        if (!detector.isPresent()) {
            throw new AnomalyDetectionException(adID, "AnomalyDetector is not found");
        }

        RandomCutForest forest = RandomCutForest
            .builder()
            .dimensions(detector.get().getFeatureAttributes().size() * AnomalyDetectorSettings.SHINGLE_SIZE)
            .sampleSize(AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE)
            .numberOfTrees(AnomalyDetectorSettings.NUM_TREES)
            .parallelExecutionEnabled(false)
            .build();
        int partitionNum = modelManager.getPartitionedForestSizes(forest, adID).getKey();
        partitionNumber.putIfAbsent(adID, new SimpleEntry<>(partitionNum, clock.instant()));
        return partitionNum;
    }

    public Optional<AnomalyDetector> getAnomalyDetector(String adID) {
        Entry<AnomalyDetector, Instant> detectorAndTime = currentDetectors.get(adID);
        if (detectorAndTime != null) {
            detectorAndTime.setValue(clock.instant());
            return Optional.of(detectorAndTime.getKey());
        }

        GetRequest request = new GetRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX, adID);

        Optional<GetResponse> getResponse = clientUtil.<GetRequest, GetResponse>timedRequest(request, LOG, client::get);

        return onGetResponse(getResponse, adID);
    }

    private Optional<AnomalyDetector> onGetResponse(Optional<GetResponse> asResponse, String adID) {
        if (!asResponse.isPresent() || !asResponse.get().isExists()) {
            return Optional.empty();
        }

        GetResponse response = asResponse.get();
        String xc = response.getSourceAsString();
        LOG.debug("Fetched anomaly detector: {}", xc);

        try (XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, xc)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            AnomalyDetector detector = AnomalyDetector.parse(parser, response.getId());
            currentDetectors.put(adID, new SimpleEntry<>(detector, clock.instant()));
            return Optional.of(detector);
        } catch (Exception t) {
            LOG.error("Fail to parse detector {}", adID);
            LOG.error("Stack trace:", t);
            return Optional.empty();
        }
    }

    /**
     * Used in delete workflow
     *
     * @param adID detector ID
     */
    public void clear(String adID) {
        currentDetectors.remove(adID);
        partitionNumber.remove(adID);
    }

    public void maintenance() {
        maintenance(currentDetectors);
        maintenance(partitionNumber);
    }

    /**
     * Clean states if it is older than our stateTtl. The input has to be a
     * ConcurrentHashMap otherwise we will have
     * java.util.ConcurrentModificationException.
     *
     * @param states states to be maintained
     */
    <T> void maintenance(ConcurrentHashMap<String, Entry<T, Instant>> states) {
        states.entrySet().stream().forEach(entry -> {
            String detectorId = entry.getKey();
            try {
                Entry<T, Instant> stateAndTime = entry.getValue();
                if (stateAndTime.getValue().plus(stateTtl).isBefore(clock.instant())) {
                    states.remove(detectorId);
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
}
