package com.amazon.opendistroforelasticsearch.ad.rest.handler;

import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;

/**
 * Anomaly detector REST action handler to process POST request.
 * POST request is for validating anomaly detector.
 */
public class ValidateAnomalyDetectorActionHandler extends AbstractActionHandler {
    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final String detectorId;
    private final AnomalyDetector anomalyDetector;

    private final Logger logger = LogManager.getLogger(IndexAnomalyDetectorActionHandler.class);
    private final Integer maxAnomalyDetectors;
    private final Integer maxAnomalyFeatures;

    private final List<String> failures;
    private final List<String> suggestedChanges;


    /**
     * Constructor function.
     *
     * @param settings                ES settings
     * @param client                  ES node client that executes actions on the local node
     * @param channel                 ES channel used to construct bytes / builder based outputs, and send responses
     * @param anomalyDetectionIndices anomaly detector index manager
     * @param detectorId              detector identifier
     * @param anomalyDetector         anomaly detector instance
     */
    public ValidateAnomalyDetectorActionHandler(
            Settings settings,
            NodeClient client,
            RestChannel channel,
            AnomalyDetectionIndices anomalyDetectionIndices,
            String detectorId,
            AnomalyDetector anomalyDetector,
            Integer maxAnomalyDetectors,
            Integer maxAnomalyFeatures
    ) {
        super(client, channel);
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.detectorId = detectorId;
        this.anomalyDetector = anomalyDetector;
        this.maxAnomalyDetectors = maxAnomalyDetectors;
        this.maxAnomalyFeatures = maxAnomalyFeatures;
        this.failures = new ArrayList<>();
        this.suggestedChanges = new ArrayList<>();
    }

    /**
     * Start function to process validate anomaly detector request.
     * Checks if anomaly detector index exist first, if not, add it as a failure case.
     *
     * @throws IOException IOException from {@link AnomalyDetectionIndices#initAnomalyDetectorIndexIfAbsent(ActionListener)}
     */
    public void startAndCheckIfIndexExists() throws IOException {
        if (!anomalyDetectionIndices.doesAnomalyDetectorIndexExist()) {
            anomalyDetectionIndices
                    .initAnomalyDetectorIndex(
                            ActionListener.wrap(response -> onCreateMappingsResponse(response), exception -> onFailure(exception))
                    );
        } else {
            preDataValidationSteps();
        }
    }

    public void preDataValidationSteps() {
        String error = RestHandlerUtils.validateAnomalyDetector(anomalyDetector, maxAnomalyFeatures);
        if (StringUtils.isNotBlank(error)) {
            channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, error));
            return;
        }
        if (channel.request().method() == RestRequest.Method.PUT) {
            handler.getDetectorJob(clusterService, client, detectorId, channel, () -> updateAnomalyDetector(client, detectorId));
        } else {
            createAnomalyDetector();
        }
    }

    private void onCreateMappingsResponse(CreateIndexResponse response) throws IOException {
        if (response.isAcknowledged()) {
            logger.info("Created {} with mappings.", ANOMALY_DETECTORS_INDEX);
            preDataValidationSteps();
        } else {
            logger.warn("Created {} with mappings call not acknowledged.", ANOMALY_DETECTORS_INDEX);
            channel
                    .sendResponse(
                            new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, response.toXContent(channel.newErrorBuilder(), EMPTY_PARAMS))
                    );
        }
    }
}
