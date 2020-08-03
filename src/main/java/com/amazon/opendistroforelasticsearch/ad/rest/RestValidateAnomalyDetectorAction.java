package com.amazon.opendistroforelasticsearch.ad.rest;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.*;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.VALIDATE;

import com.amazon.opendistroforelasticsearch.ad.rest.handler.IndexAnomalyDetectorActionHandler;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.ValidateAnomalyDetectorActionHandler;
import com.amazon.opendistroforelasticsearch.ad.settings.EnabledSetting;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.*;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * This class consists of the REST handler to validate anomaly detector configurations.
 */

public class RestValidateAnomalyDetectorAction extends BaseRestHandler {

    private static final String VALIDATE_ANOMALY_DETECTOR_ACTION = "validate_anomaly_detector_action";
    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final Logger logger = LogManager.getLogger(RestValidateAnomalyDetectorAction.class);
    private final Settings settings;
    private final NamedXContentRegistry xContentRegistry;

    private volatile TimeValue requestTimeout;
    private volatile TimeValue detectionInterval;
    private volatile TimeValue detectionWindowDelay;
    private volatile Integer maxAnomalyDetectors;
    private volatile Integer maxAnomalyFeatures;

    public RestValidateAnomalyDetectorAction(
            Settings settings,
            AnomalyDetectionIndices anomalyDetectionIndices,
            NamedXContentRegistry xContentRegistry
    ) {
        this.settings = settings;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.detectionInterval = DETECTION_INTERVAL.get(settings);
        this.detectionWindowDelay = DETECTION_WINDOW_DELAY.get(settings);
        this.maxAnomalyDetectors = MAX_ANOMALY_DETECTORS.get(settings);
        this.maxAnomalyFeatures = MAX_ANOMALY_FEATURES.get(settings);
        this.requestTimeout = REQUEST_TIMEOUT.get(settings);
        this.xContentRegistry = xContentRegistry;
    }

    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }

        String detectorId = AnomalyDetector.NO_ID;
        XContentParser parser = request.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        AnomalyDetector detector = AnomalyDetector.parseValidation(parser, detectorId, null);

        return channel -> new ValidateAnomalyDetectorActionHandler(
                settings,
                client,
                channel,
                anomalyDetectionIndices,
                detectorId,
                detector,
                maxAnomalyDetectors,
                maxAnomalyFeatures,
                requestTimeout,
                xContentRegistry
        ).startValidation();
    }


    @Override
    public String getName() { return VALIDATE_ANOMALY_DETECTOR_ACTION; }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                // validate configs
                new Route(
                        RestRequest.Method.POST,
                        String.format(Locale.ROOT, "%s/%s", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, VALIDATE)
                )
            );
    }
}
