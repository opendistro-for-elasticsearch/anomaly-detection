package com.amazon.opendistroforelasticsearch.ad.model;

import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.index.query.QueryBuilder;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public class AnomalyDetectorValidation extends AnomalyDetector{

    /**
     * Constructor function.
     *
     * @param detectorId        detector identifier
     * @param version           detector document version
     * @param name              detector name
     * @param description       description of detector
     * @param timeField         time field
     * @param indices           indices used as detector input
     * @param features          detector feature attributes
     * @param filterQuery       detector filter query
     * @param detectionInterval detecting interval
     * @param windowDelay       max delay window for realtime data
     * @param uiMetadata        metadata used by Kibana
     * @param schemaVersion     anomaly detector index mapping version
     * @param lastUpdateTime    detector's last update time
     */
    public AnomalyDetectorValidation(
            String detectorId,
            Long version,
            String name,
            String description,
            String timeField,
            List<String> indices,
            List<Feature> features,
            QueryBuilder filterQuery,
            TimeConfiguration detectionInterval,
            TimeConfiguration windowDelay,
            Map<String, Object> uiMetadata,
            Integer schemaVersion,
            Instant lastUpdateTime
    ) {
        super();
        if (Strings.isBlank(name)) {
            throw new IllegalArgumentException("Detector name should be set");
        }
        if (timeField == null) {
            throw new IllegalArgumentException("Time field should be set");
        }
        if (indices == null || indices.isEmpty()) {
            throw new IllegalArgumentException("Indices should be set");
        }
        if (detectionInterval == null) {
            throw new IllegalArgumentException("Detection interval should be set");
        }
        this.detectorId = detectorId;
        this.version = version;
        this.name = name;
        this.description = description;
        this.timeField = timeField;
        this.indices = indices;
        this.featureAttributes = features;
        this.filterQuery = filterQuery;
        this.detectionInterval = detectionInterval;
        this.windowDelay = windowDelay;
        this.uiMetadata = uiMetadata;
        this.schemaVersion = schemaVersion;
        this.lastUpdateTime = lastUpdateTime;
    }
}

