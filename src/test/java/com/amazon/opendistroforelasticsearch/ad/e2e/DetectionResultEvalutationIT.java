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

package com.amazon.opendistroforelasticsearch.ad.e2e;

import java.io.File;
import java.io.FileReader;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;

import com.amazon.opendistroforelasticsearch.ad.ODFERestTestCase;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class DetectionResultEvalutationIT extends ODFERestTestCase {

    public void testDataset() throws Exception {
        verifyAnomaly("synthetic", 1, 1500, 8, .9, .9, 10);
    }

    private void verifyAnomaly(
        String datasetName,
        int intervalMinutes,
        int trainTestSplit,
        int shingleSize,
        double minPrecision,
        double minRecall,
        double maxError
    ) throws Exception {

        RestClient client = client();

        String dataFileName = String.format("data/%s.data", datasetName);
        String labelFileName = String.format("data/%s.label", datasetName);

        List<JsonObject> data = getData(dataFileName);
        List<Entry<Instant, Instant>> anomalies = getAnomalyWindows(labelFileName);

        indexTrainData(datasetName, data, trainTestSplit, client);
        String detectorId = createDetector(datasetName, intervalMinutes, client);
        startDetector(detectorId, data, trainTestSplit, shingleSize, intervalMinutes, client);

        indexTestData(data, datasetName, trainTestSplit, client);
        double[] testResults = getTestResults(detectorId, data, trainTestSplit, intervalMinutes, anomalies, client);
        verifyTestResults(testResults, anomalies, minPrecision, minRecall, maxError);
    }

    private void verifyTestResults(
        double[] testResults,
        List<Entry<Instant, Instant>> anomalies,
        double minPrecision,
        double minRecall,
        double maxError
    ) {

        double positives = testResults[0];
        double truePositives = testResults[1];
        double positiveAnomalies = testResults[2];
        double errors = testResults[3];

        // precision = predicted anomaly points that are true / predicted anomaly points
        double precision = positives > 0 ? truePositives / positives : 1;
        assertTrue(precision >= minPrecision);

        // recall = windows containing predicted anomaly points / total anomaly windows
        double recall = anomalies.size() > 0 ? positiveAnomalies / anomalies.size() : 1;
        assertTrue(recall >= minRecall);

        assertTrue(errors <= maxError);
    }

    private int isAnomaly(Instant time, List<Entry<Instant, Instant>> labels) {
        for (int i = 0; i < labels.size(); i++) {
            Entry<Instant, Instant> window = labels.get(i);
            if (time.compareTo(window.getKey()) >= 0 && time.compareTo(window.getValue()) <= 0) {
                return i;
            }
        }
        return -1;
    }

    private double[] getTestResults(
        String detectorId,
        List<JsonObject> data,
        int trainTestSplit,
        int intervalMinutes,
        List<Entry<Instant, Instant>> anomalies,
        RestClient client
    ) throws Exception {

        double positives = 0;
        double truePositives = 0;
        Set<Integer> positiveAnomalies = new HashSet<>();
        double errors = 0;
        for (int i = trainTestSplit; i < data.size(); i++) {
            Instant begin = Instant.from(DateTimeFormatter.ISO_INSTANT.parse(data.get(i).get("timestamp").getAsString()));
            Instant end = begin.plus(intervalMinutes, ChronoUnit.MINUTES);
            try {
                Map<String, Object> response = getDetectionResult(detectorId, begin, end, client);
                double anomalyGrade = (double) response.get("anomalyGrade");
                if (anomalyGrade > 0) {
                    positives++;
                    int result = isAnomaly(begin, anomalies);
                    if (result != -1) {
                        truePositives++;
                        positiveAnomalies.add(result);
                    }
                }
            } catch (Exception e) {
                errors++;
                e.printStackTrace();
            }
        }
        return new double[] { positives, truePositives, positiveAnomalies.size(), errors };
    }

    private void indexTestData(List<JsonObject> data, String datasetName, int trainTestSplit, RestClient client) throws Exception {
        data.stream().skip(trainTestSplit).forEach(r -> {
            try {
                Request req = new Request("POST", String.format("/%s/_doc/", datasetName));
                req.setJsonEntity(r.toString());
                client.performRequest(req);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        Thread.sleep(1_000);
    }

    private void startDetector(
        String detectorId,
        List<JsonObject> data,
        int trainTestSplit,
        int shingleSize,
        int intervalMinutes,
        RestClient client
    ) throws Exception {

        Instant trainTime = Instant.from(DateTimeFormatter.ISO_INSTANT.parse(data.get(trainTestSplit - 1).get("timestamp").getAsString()));

        Instant begin = null;
        Instant end = null;
        for (int i = 0; i < shingleSize; i++) {
            begin = trainTime.minus(intervalMinutes * (shingleSize - 1 - i), ChronoUnit.MINUTES);
            end = begin.plus(intervalMinutes, ChronoUnit.MINUTES);
            try {
                getDetectionResult(detectorId, begin, end, client);
            } catch (Exception e) {}
        }
        // It takes time to wait for model initialization
        long startTime = System.currentTimeMillis();
        do {
            try {
                Thread.sleep(5_000);
                getDetectionResult(detectorId, begin, end, client);
                break;
            } catch (Exception e) {
                long duration = System.currentTimeMillis() - startTime;
                // we wait at most 60 secs
                if (duration > 60_000) {
                    throw new RuntimeException(e);
                }
            }
        } while (true);
    }

    private String createDetector(String datasetName, int intervalMinutes, RestClient client) throws Exception {
        Request request = new Request("POST", "/_opendistro/_anomaly_detection/detectors/");
        String requestBody = String
            .format(
                Locale.ROOT,
                "{ \"name\": \"test\", \"description\": \"test\", \"time_field\": \"timestamp\""
                    + ", \"indices\": [\"%s\"], \"feature_attributes\": [{ \"feature_name\": \"feature 1\", \"feature_enabled\": "
                    + "\"true\", \"aggregation_query\": { \"Feature1\": { \"sum\": { \"field\": \"Feature1\" } } } }, { \"feature_name\""
                    + ": \"feature 2\", \"feature_enabled\": \"true\", \"aggregation_query\": { \"Feature2\": { \"sum\": { \"field\": "
                    + "\"Feature2\" } } } }], \"detection_interval\": { \"period\": { \"interval\": %d, \"unit\": \"Minutes\" } }, "
                    + "\"schema_version\": 0 }",
                datasetName,
                intervalMinutes
            );
        request.setJsonEntity(requestBody);
        Map<String, Object> response = entityAsMap(client.performRequest(request));
        String detectorId = (String) response.get("_id");
        Thread.sleep(1_000);
        return detectorId;
    }

    private List<Entry<Instant, Instant>> getAnomalyWindows(String labalFileName) throws Exception {
        JsonArray windows = new JsonParser()
            .parse(new FileReader(new File(getClass().getResource(labalFileName).toURI())))
            .getAsJsonArray();
        List<Entry<Instant, Instant>> anomalies = new ArrayList<>(windows.size());
        for (int i = 0; i < windows.size(); i++) {
            JsonArray window = windows.get(i).getAsJsonArray();
            Instant begin = Instant.from(DateTimeFormatter.ISO_INSTANT.parse(window.get(0).getAsString()));
            Instant end = Instant.from(DateTimeFormatter.ISO_INSTANT.parse(window.get(1).getAsString()));
            anomalies.add(new SimpleEntry<>(begin, end));
        }
        return anomalies;
    }

    private void indexTrainData(String datasetName, List<JsonObject> data, int trainTestSplit, RestClient client) throws Exception {
        Request request = new Request("PUT", datasetName);
        String requestBody = "{ \"mappings\": { \"properties\": { \"timestamp\": { \"type\": \"date\"},"
            + " \"Feature1\": { \"type\": \"double\" }, \"Feature2\": { \"type\": \"double\" } } } }";
        request.setJsonEntity(requestBody);
        client.performRequest(request);
        Thread.sleep(1_000);

        data.stream().limit(trainTestSplit).forEach(r -> {
            try {
                Request req = new Request("POST", String.format("/%s/_doc/", datasetName));
                req.setJsonEntity(r.toString());
                client.performRequest(req);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        Thread.sleep(1_000);
    }

    private List<JsonObject> getData(String datasetFileName) throws Exception {
        JsonArray jsonArray = new JsonParser()
            .parse(new FileReader(new File(getClass().getResource(datasetFileName).toURI())))
            .getAsJsonArray();
        List<JsonObject> list = new ArrayList<>(jsonArray.size());
        jsonArray.iterator().forEachRemaining(i -> list.add(i.getAsJsonObject()));
        return list;
    }

    private Map<String, Object> getDetectionResult(String detectorId, Instant begin, Instant end, RestClient client) {
        try {
            Request request = new Request("POST", String.format("/_opendistro/_anomaly_detection/detectors/%s/_run", detectorId));
            request
                .setJsonEntity(
                    String.format(Locale.ROOT, "{ \"period_start\": %d, \"period_end\": %d }", begin.toEpochMilli(), end.toEpochMilli())
                );
            return entityAsMap(client.performRequest(request));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
