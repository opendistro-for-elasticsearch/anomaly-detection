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

package com.amazon.opendistroforelasticsearch.ad.feature;

import static org.apache.commons.math3.linear.MatrixUtils.createRealMatrix;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.range.InternalDateRange;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;
import org.elasticsearch.search.aggregations.metrics.Percentile;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.amazon.opendistroforelasticsearch.ad.dataprocessor.Interpolator;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.ParseUtils;

/**
 * DAO for features from search.
 */
public class SearchFeatureDao {

    protected static final String AGG_NAME_MAX = "max_timefield";

    private static final Logger logger = LogManager.getLogger(SearchFeatureDao.class);

    // Dependencies
    private final Client client;
    private final NamedXContentRegistry xContent;
    private final Interpolator interpolator;
    private final ClientUtil clientUtil;

    /**
     * Constructor injection.
     *
     * @param client ES client for queries
     * @param xContent ES XContentRegistry
     * @param interpolator interpolator for missing values
     * @param clientUtil utility for ES client
     */
    public SearchFeatureDao(Client client, NamedXContentRegistry xContent, Interpolator interpolator, ClientUtil clientUtil) {
        this.client = client;
        this.xContent = xContent;
        this.interpolator = interpolator;
        this.clientUtil = clientUtil;
    }

    /**
     * Returns epoch time of the latest data under the detector.
     *
     * @deprecated use getLatestDataTime with listener instead.
     *
     * @param detector info about the indices and documents
     * @return epoch time of the latest data in milliseconds
     */
    @Deprecated
    public Optional<Long> getLatestDataTime(AnomalyDetector detector) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .aggregation(AggregationBuilders.max(AGG_NAME_MAX).field(detector.getTimeField()))
            .size(0);
        SearchRequest searchRequest = new SearchRequest().indices(detector.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        return clientUtil
            .<SearchRequest, SearchResponse>timedRequest(searchRequest, logger, client::search)
            .map(SearchResponse::getAggregations)
            .map(aggs -> aggs.asMap())
            .map(map -> (Max) map.get(AGG_NAME_MAX))
            .map(agg -> (long) agg.getValue());
    }

    /**
     * Returns to listener the epoch time of the latset data under the detector.
     *
     * @param detector info about the data
     * @param listener onResponse is called with the epoch time of the latset data under the detector
     */
    public void getLatestDataTime(AnomalyDetector detector, ActionListener<Optional<Long>> listener) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .aggregation(AggregationBuilders.max(AGG_NAME_MAX).field(detector.getTimeField()))
            .size(0);
        SearchRequest searchRequest = new SearchRequest().indices(detector.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        client
            .search(searchRequest, ActionListener.wrap(response -> listener.onResponse(getLatestDataTime(response)), listener::onFailure));
    }

    private Optional<Long> getLatestDataTime(SearchResponse searchResponse) {
        return Optional
            .ofNullable(searchResponse)
            .map(SearchResponse::getAggregations)
            .map(aggs -> aggs.asMap())
            .map(map -> (Max) map.get(AGG_NAME_MAX))
            .map(agg -> (long) agg.getValue());
    }

    /**
     * Gets features for the given time period.
     * This function also adds given detector to negative cache before sending es request.
     * Once response/exception is received within timeout, this request will be treated as complete
     * and cleared from the negative cache.
     * Otherwise this detector entry remain in the negative to reject further request.
     *
     * @deprecated use getFeaturesForPeriod with listener instead.
     *
     * @param detector info about indices, documents, feature query
     * @param startTime epoch milliseconds at the beginning of the period
     * @param endTime epoch milliseconds at the end of the period
     * @throws IllegalStateException when unexpected failures happen
     * @return features from search results, empty when no data found
     */
    @Deprecated
    public Optional<double[]> getFeaturesForPeriod(AnomalyDetector detector, long startTime, long endTime) {
        SearchRequest searchRequest = createFeatureSearchRequest(detector, startTime, endTime, Optional.empty());

        // send throttled request: this request will clear the negative cache if the request finished within timeout
        return clientUtil
            .<SearchRequest, SearchResponse>throttledTimedRequest(searchRequest, logger, client::search, detector)
            .flatMap(resp -> parseResponse(resp, detector.getEnabledFeatureIds()));
    }

    /**
     * Returns to listener features for the given time period.
     *
     * @param detector info about indices, feature query
     * @param startTime epoch milliseconds at the beginning of the period
     * @param endTime epoch milliseconds at the end of the period
     * @param listener onResponse is called with features for the given time period.
     */
    public void getFeaturesForPeriod(AnomalyDetector detector, long startTime, long endTime, ActionListener<Optional<double[]>> listener) {
        SearchRequest searchRequest = createFeatureSearchRequest(detector, startTime, endTime, Optional.empty());
        client
            .search(
                searchRequest,
                ActionListener
                    .wrap(response -> listener.onResponse(parseResponse(response, detector.getEnabledFeatureIds())), listener::onFailure)
            );
    }

    private Optional<double[]> parseResponse(SearchResponse response, List<String> featureIds) {
        return parseAggregations(
            Optional.ofNullable(response).filter(resp -> response.getHits().getTotalHits().value > 0L).map(resp -> resp.getAggregations()),
            featureIds
        );
    }

    private double parseAggregation(Aggregation aggregation) {
        Double result = null;
        if (aggregation instanceof SingleValue) {
            result = ((SingleValue) aggregation).value();
        } else if (aggregation instanceof InternalTDigestPercentiles) {
            Iterator<Percentile> percentile = ((InternalTDigestPercentiles) aggregation).iterator();
            if (percentile.hasNext()) {
                result = percentile.next().getValue();
            }
        }
        return Optional.ofNullable(result).orElseThrow(() -> new IllegalStateException("Failed to parse aggregation " + aggregation));
    }

    /**
     * Gets samples of features for the time ranges.
     *
     * Sampled features are not true features. They are intended to be approximate results produced at low costs.
     *
     * @param detector info about the indices, documents, feature query
     * @param ranges list of time ranges
     * @return approximate features for the time ranges
     */
    @Deprecated
    public List<Optional<double[]>> getFeatureSamplesForPeriods(AnomalyDetector detector, List<Entry<Long, Long>> ranges) {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        ranges
            .stream()
            .map(range -> createFeatureSearchRequest(detector, range.getKey(), range.getValue(), Optional.empty()))
            .forEachOrdered(request -> multiSearchRequest.add(request));

        return clientUtil
            .<MultiSearchRequest, MultiSearchResponse>timedRequest(multiSearchRequest, logger, client::multiSearch)
            .map(Stream::of)
            .orElseGet(Stream::empty)
            .flatMap(multiSearchResp -> Arrays.stream(multiSearchResp.getResponses()))
            .map(item -> {
                Optional.ofNullable(item.getFailure()).ifPresent(e -> logger.warn("Failed to get search response", e));
                return item;
            })
            .map(item -> Optional.ofNullable(item.getResponse()).flatMap(r -> parseResponse(r, detector.getEnabledFeatureIds())))
            .collect(Collectors.toList());
    }

    /**
     * Gets samples of features for the time ranges.
     *
     * Sampled features are not true features. They are intended to be approximate results produced at low costs.
     *
     * @param detector info about the indices, documents, feature query
     * @param ranges list of time ranges
     * @param listener handle approximate features for the time ranges
     * @throws IOException if a user gives wrong query input when defining a detector
     */
    public void getFeatureSamplesForPeriods(
        AnomalyDetector detector,
        List<Entry<Long, Long>> ranges,
        ActionListener<List<Optional<double[]>>> listener
    ) throws IOException {
        SearchRequest request = createPreviewSearchRequest(detector, ranges);

        client.search(request, ActionListener.wrap(response -> {
            Aggregations aggs = response.getAggregations();
            if (aggs == null) {
                listener.onResponse(Collections.emptyList());
                return;
            }

            listener
                .onResponse(
                    aggs
                        .asList()
                        .stream()
                        .filter(InternalDateRange.class::isInstance)
                        .flatMap(agg -> ((InternalDateRange) agg).getBuckets().stream())
                        .map(bucket -> parseBucket(bucket, detector.getEnabledFeatureIds()))
                        .collect(Collectors.toList())
                );
        }, listener::onFailure));
    }

    /**
     * Gets features for sampled periods.
     *
     * @deprecated use getFeaturesForSampledPeriods with listener instead.
     *
     * Sampling starts with the latest period and goes backwards in time until there are up to {@code maxSamples} samples.
     * If the initial stride {@code maxStride} results into a low count of samples, the implementation
     * may attempt with (exponentially) reduced strides and interpolate missing points.
     *
     * @param detector info about indices, documents, feature query
     * @param maxSamples the maximum number of samples to return
     * @param maxStride the maximum number of periods between samples
     * @param endTime the end time of the latest period
     * @return sampled features and stride, empty when no data found
     */
    @Deprecated
    public Optional<Entry<double[][], Integer>> getFeaturesForSampledPeriods(
        AnomalyDetector detector,
        int maxSamples,
        int maxStride,
        long endTime
    ) {
        Map<Long, double[]> cache = new HashMap<>();
        int currentStride = maxStride;
        Optional<double[][]> features = Optional.empty();
        logger.info(String.format("Getting features for detector %s starting %d", detector.getDetectorId(), endTime));
        while (currentStride >= 1) {
            boolean isInterpolatable = currentStride < maxStride;
            features = getFeaturesForSampledPeriods(detector, maxSamples, currentStride, endTime, cache, isInterpolatable);

            if (!features.isPresent() || features.get().length > maxSamples / 2 || currentStride == 1) {
                logger
                    .info(
                        String
                            .format(
                                "Get features for detector %s finishes with features present %b, current stride %d",
                                detector.getDetectorId(),
                                features.isPresent(),
                                currentStride
                            )
                    );
                break;
            } else {
                currentStride = currentStride / 2;
            }
        }
        if (features.isPresent()) {
            return Optional.of(new SimpleEntry<>(features.get(), currentStride));
        } else {
            return Optional.empty();
        }
    }

    private Optional<double[][]> getFeaturesForSampledPeriods(
        AnomalyDetector detector,
        int maxSamples,
        int stride,
        long endTime,
        Map<Long, double[]> cache,
        boolean isInterpolatable
    ) {
        ArrayDeque<double[]> sampledFeatures = new ArrayDeque<>(maxSamples);
        for (int i = 0; i < maxSamples; i++) {
            long span = ((IntervalTimeConfiguration) detector.getDetectionInterval()).toDuration().toMillis();
            long end = endTime - span * stride * i;
            if (cache.containsKey(end)) {
                sampledFeatures.addFirst(cache.get(end));
            } else {
                Optional<double[]> features = getFeaturesForPeriod(detector, end - span, end);
                if (features.isPresent()) {
                    cache.put(end, features.get());
                    sampledFeatures.addFirst(features.get());
                } else if (isInterpolatable) {
                    Optional<double[]> previous = Optional.ofNullable(cache.get(end - span * stride));
                    Optional<double[]> next = Optional.ofNullable(cache.get(end + span * stride));
                    if (previous.isPresent() && next.isPresent()) {
                        double[] interpolants = getInterpolants(previous.get(), next.get());
                        cache.put(end, interpolants);
                        sampledFeatures.addFirst(interpolants);
                    } else {
                        break;
                    }
                } else {
                    break;
                }

            }
        }
        Optional<double[][]> samples;
        if (sampledFeatures.isEmpty()) {
            samples = Optional.empty();
        } else {
            samples = Optional.of(sampledFeatures.toArray(new double[0][0]));
        }
        return samples;
    }

    /**
     * Returns to listener features for sampled periods.
     *
     * Sampling starts with the latest period and goes backwards in time until there are up to {@code maxSamples} samples.
     * If the initial stride {@code maxStride} results into a low count of samples, the implementation
     * may attempt with (exponentially) reduced strides and interpolate missing points.
     *
     * @param detector info about indices, documents, feature query
     * @param maxSamples the maximum number of samples to return
     * @param maxStride the maximum number of periods between samples
     * @param endTime the end time of the latest period
     * @param listener onResponse is called with sampled features and stride between points, or empty for no data
     */
    public void getFeaturesForSampledPeriods(
        AnomalyDetector detector,
        int maxSamples,
        int maxStride,
        long endTime,
        ActionListener<Optional<Entry<double[][], Integer>>> listener
    ) {
        Map<Long, double[]> cache = new HashMap<>();
        logger.info(String.format("Getting features for detector %s starting %d", detector.getDetectorId(), endTime));
        getFeatureSamplesWithCache(detector, maxSamples, maxStride, endTime, cache, maxStride, listener);
    }

    private void getFeatureSamplesWithCache(
        AnomalyDetector detector,
        int maxSamples,
        int maxStride,
        long endTime,
        Map<Long, double[]> cache,
        int currentStride,
        ActionListener<Optional<Entry<double[][], Integer>>> listener
    ) {
        getFeatureSamplesForStride(
            detector,
            maxSamples,
            maxStride,
            currentStride,
            endTime,
            cache,
            ActionListener
                .wrap(
                    features -> processFeatureSamplesForStride(
                        features,
                        detector,
                        maxSamples,
                        maxStride,
                        currentStride,
                        endTime,
                        cache,
                        listener
                    ),
                    listener::onFailure
                )
        );
    }

    private void processFeatureSamplesForStride(
        Optional<double[][]> features,
        AnomalyDetector detector,
        int maxSamples,
        int maxStride,
        int currentStride,
        long endTime,
        Map<Long, double[]> cache,
        ActionListener<Optional<Entry<double[][], Integer>>> listener
    ) {
        if (!features.isPresent()) {
            logger
                .info(
                    String
                        .format(
                            "Get features for detector %s finishes without any features present, current stride %d",
                            detector.getDetectorId(),
                            currentStride
                        )
                );
            listener.onResponse(Optional.empty());
        } else if (features.get().length > maxSamples / 2 || currentStride == 1) {
            logger
                .info(
                    String
                        .format(
                            "Get features for detector %s finishes with %d samples, current stride %d",
                            detector.getDetectorId(),
                            features.get().length,
                            currentStride
                        )
                );
            listener.onResponse(Optional.of(new SimpleEntry<>(features.get(), currentStride)));
        } else {
            getFeatureSamplesWithCache(detector, maxSamples, maxStride, endTime, cache, currentStride / 2, listener);
        }
    }

    private void getFeatureSamplesForStride(
        AnomalyDetector detector,
        int maxSamples,
        int maxStride,
        int currentStride,
        long endTime,
        Map<Long, double[]> cache,
        ActionListener<Optional<double[][]>> listener
    ) {
        ArrayDeque<double[]> sampledFeatures = new ArrayDeque<>(maxSamples);
        boolean isInterpolatable = currentStride < maxStride;
        long span = ((IntervalTimeConfiguration) detector.getDetectionInterval()).toDuration().toMillis();
        sampleForIteration(detector, cache, maxSamples, endTime, span, currentStride, sampledFeatures, isInterpolatable, 0, listener);
    }

    private void sampleForIteration(
        AnomalyDetector detector,
        Map<Long, double[]> cache,
        int maxSamples,
        long endTime,
        long span,
        int stride,
        ArrayDeque<double[]> sampledFeatures,
        boolean isInterpolatable,
        int iteration,
        ActionListener<Optional<double[][]>> listener
    ) {
        if (iteration < maxSamples) {
            long end = endTime - span * stride * iteration;
            if (cache.containsKey(end)) {
                sampledFeatures.addFirst(cache.get(end));
                sampleForIteration(
                    detector,
                    cache,
                    maxSamples,
                    endTime,
                    span,
                    stride,
                    sampledFeatures,
                    isInterpolatable,
                    iteration + 1,
                    listener
                );
            } else {
                getFeaturesForPeriod(detector, end - span, end, ActionListener.wrap(features -> {
                    if (features.isPresent()) {
                        cache.put(end, features.get());
                        sampledFeatures.addFirst(features.get());
                        sampleForIteration(
                            detector,
                            cache,
                            maxSamples,
                            endTime,
                            span,
                            stride,
                            sampledFeatures,
                            isInterpolatable,
                            iteration + 1,
                            listener
                        );
                    } else if (isInterpolatable) {
                        Optional<double[]> previous = Optional.ofNullable(cache.get(end - span * stride));
                        Optional<double[]> next = Optional.ofNullable(cache.get(end + span * stride));
                        if (previous.isPresent() && next.isPresent()) {
                            double[] interpolants = getInterpolants(previous.get(), next.get());
                            cache.put(end, interpolants);
                            sampledFeatures.addFirst(interpolants);
                            sampleForIteration(
                                detector,
                                cache,
                                maxSamples,
                                endTime,
                                span,
                                stride,
                                sampledFeatures,
                                isInterpolatable,
                                iteration + 1,
                                listener
                            );
                        } else {
                            listener.onResponse(toMatrix(sampledFeatures));
                        }
                    } else {
                        listener.onResponse(toMatrix(sampledFeatures));
                    }
                }, listener::onFailure));
            }
        } else {
            listener.onResponse(toMatrix(sampledFeatures));
        }
    }

    private Optional<double[][]> toMatrix(ArrayDeque<double[]> sampledFeatures) {
        Optional<double[][]> samples;
        if (sampledFeatures.isEmpty()) {
            samples = Optional.empty();
        } else {
            samples = Optional.of(sampledFeatures.toArray(new double[0][0]));
        }
        return samples;
    }

    private double[] getInterpolants(double[] previous, double[] next) {
        return transpose(interpolator.interpolate(transpose(new double[][] { previous, next }), 3))[1];
    }

    private double[][] transpose(double[][] matrix) {
        return createRealMatrix(matrix).transpose().getData();
    }

    private SearchRequest createFeatureSearchRequest(AnomalyDetector detector, long startTime, long endTime, Optional<String> preference) {
        // TODO: FeatureQuery field is planned to be removed and search request creation will migrate to new api.
        try {
            SearchSourceBuilder searchSourceBuilder = ParseUtils.generateInternalFeatureQuery(detector, startTime, endTime, xContent);
            return new SearchRequest(detector.getIndices().toArray(new String[0]), searchSourceBuilder).preference(preference.orElse(null));
        } catch (IOException e) {
            logger
                .warn(
                    "Failed to create feature search request for " + detector.getDetectorId() + " from " + startTime + " to " + endTime,
                    e
                );
            throw new IllegalStateException(e);
        }
    }

    private SearchRequest createPreviewSearchRequest(AnomalyDetector detector, List<Entry<Long, Long>> ranges) throws IOException {
        try {
            SearchSourceBuilder searchSourceBuilder = ParseUtils.generatePreviewQuery(detector, ranges, xContent);
            return new SearchRequest(detector.getIndices().toArray(new String[0]), searchSourceBuilder);
        } catch (IOException e) {
            logger.warn("Failed to create feature search request for " + detector.getDetectorId() + " for preview", e);
            throw e;
        }
    }

    private Optional<double[]> parseBucket(InternalDateRange.Bucket bucket, List<String> featureIds) {
        return parseAggregations(Optional.ofNullable(bucket).map(b -> b.getAggregations()), featureIds);
    }

    private Optional<double[]> parseAggregations(Optional<Aggregations> aggregations, List<String> featureIds) {
        return aggregations
            .map(aggs -> aggs.asMap())
            .map(
                map -> featureIds
                    .stream()
                    .mapToDouble(id -> Optional.ofNullable(map.get(id)).map(this::parseAggregation).orElse(Double.NaN))
                    .toArray()
            )
            .filter(result -> Arrays.stream(result).noneMatch(d -> Double.isNaN(d) || Double.isInfinite(d)));
    }
}
