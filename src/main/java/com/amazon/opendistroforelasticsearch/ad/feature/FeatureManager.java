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

package com.amazon.opendistroforelasticsearch.ad.feature;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;

import com.amazon.opendistroforelasticsearch.ad.dataprocessor.Interpolator;

import static java.util.Arrays.copyOfRange;

import static org.apache.commons.math3.linear.MatrixUtils.createRealMatrix;

/**
 * A facade managing feature data operations and buffers.
 */
public class FeatureManager {

    private static final Logger logger = LogManager.getLogger(FeatureManager.class);

    // Each anomaly detector has a queue of data points with timestamps (in epoch milliseconds).
    private final Map<String, ArrayDeque<Entry<Long, double[]>>> detectorIdsToTimeShingles;

    private final SearchFeatureDao searchFeatureDao;
    private final Interpolator interpolator;
    private final Clock clock;

    private final int maxTrainSamples;
    private final int maxSampleStride;
    private final int shingleSize;
    private final int maxMissingPoints;
    private final int maxNeighborDistance;
    private final double previewSampleRate;
    private final int maxPreviewSamples;
    private final Duration featureBufferTtl;

    /**
     * Constructor with dependencies and configuration.
     *
     * @param searchFeatureDao DAO of features from search
     * @param interpolator interpolator of samples
     * @param clock clock for system time
     * @param maxTrainSamples max number of samples from search
     * @param maxSampleStride max stride between uninterpolated train samples
     * @param shingleSize size of feature shingles
     * @param maxMissingPoints max number of missing points allowed to generate a shingle
     * @param maxNeighborDistance max distance (number of intervals) between a missing point and a replacement neighbor
     * @param previewSampleRate number of samples to number of all the data points in the preview time range
     * @param maxPreviewSamples max number of samples from search for preview features
     * @param featureBufferTtl time to live for stale feature buffers
     */
    public FeatureManager(
        SearchFeatureDao searchFeatureDao,
        Interpolator interpolator,
        Clock clock,
        int maxTrainSamples,
        int maxSampleStride,
        int shingleSize,
        int maxMissingPoints,
        int maxNeighborDistance,
        double previewSampleRate,
        int maxPreviewSamples,
        Duration featureBufferTtl
    ) {
        this.searchFeatureDao = searchFeatureDao;
        this.interpolator = interpolator;
        this.clock = clock;
        this.maxTrainSamples = maxTrainSamples;
        this.maxSampleStride = maxSampleStride;
        this.shingleSize = shingleSize;
        this.maxMissingPoints = maxMissingPoints;
        this.maxNeighborDistance = maxNeighborDistance;
        this.previewSampleRate = previewSampleRate;
        this.maxPreviewSamples = maxPreviewSamples;
        this.featureBufferTtl = featureBufferTtl;

        this.detectorIdsToTimeShingles = new ConcurrentHashMap<>();
    }

    /**
     * Returns unprocessed features and processed features (such as shingle) for the current data point.
     *
     * @deprecated use getCurrentFeatures with listener instead.
     *
     * @param detector anomaly detector for which the features are returned
     * @param startTime start time of the data point in epoch milliseconds
     * @param endTime end time of the data point in epoch milliseconds
     * @return unprocessed features and processed features for the current data point
     */
    @Deprecated
    public SinglePointFeatures getCurrentFeatures(AnomalyDetector detector, long startTime, long endTime) {
        double[][] currentPoints = null;
        Deque<Entry<Long, double[]>> shingle = detectorIdsToTimeShingles
            .computeIfAbsent(detector.getDetectorId(), id -> new ArrayDeque<Entry<Long, double[]>>(shingleSize));
        if (shingle.isEmpty() || shingle.getLast().getKey() < endTime) {
            Optional<double[]> point = searchFeatureDao.getFeaturesForPeriod(detector, startTime, endTime);
            if (point.isPresent()) {
                if (shingle.size() == shingleSize) {
                    shingle.remove();
                }
                shingle.add(new SimpleImmutableEntry<>(endTime, point.get()));
            } else {
                return new SinglePointFeatures(Optional.empty(), Optional.empty());
            }
        }
        currentPoints = filterAndFill(shingle, endTime, detector);
        Optional<double[]> currentPoint = Optional.ofNullable(shingle.peekLast().getValue());
        return Optional
            .ofNullable(currentPoints)
            .map(points -> new SinglePointFeatures(currentPoint, Optional.of(batchShingle(points, shingleSize)[0])))
            .orElse(new SinglePointFeatures(currentPoint, Optional.empty()));
    }

    /**
     * Returns to listener unprocessed features and processed features (such as shingle) for the current data point.
     *
     * @param detector anomaly detector for which the features are returned
     * @param startTime start time of the data point in epoch milliseconds
     * @param endTime end time of the data point in epoch milliseconds
     * @param listener onResponse is called with unprocessed features and processed features for the current data point
     */
    public void getCurrentFeatures(AnomalyDetector detector, long startTime, long endTime, ActionListener<SinglePointFeatures> listener) {

        Deque<Entry<Long, double[]>> shingle = detectorIdsToTimeShingles
            .computeIfAbsent(detector.getDetectorId(), id -> new ArrayDeque<Entry<Long, double[]>>(shingleSize));
        if (shingle.isEmpty() || shingle.getLast().getKey() < endTime) {
            searchFeatureDao
                .getFeaturesForPeriod(
                    detector,
                    startTime,
                    endTime,
                    ActionListener
                        .wrap(point -> updateUnprocessedFeatures(point, shingle, detector, endTime, listener), listener::onFailure)
                );
        } else {
            getProcessedFeatures(shingle, detector, endTime, listener);
        }
    }

    private void updateUnprocessedFeatures(
        Optional<double[]> point,
        Deque<Entry<Long, double[]>> shingle,
        AnomalyDetector detector,
        long endTime,
        ActionListener<SinglePointFeatures> listener
    ) {
        if (point.isPresent()) {
            if (shingle.size() == shingleSize) {
                shingle.remove();
            }
            shingle.add(new SimpleImmutableEntry<>(endTime, point.get()));
            getProcessedFeatures(shingle, detector, endTime, listener);
        } else {
            listener.onResponse(new SinglePointFeatures(Optional.empty(), Optional.empty()));
        }
    }

    private void getProcessedFeatures(
        Deque<Entry<Long, double[]>> shingle,
        AnomalyDetector detector,
        long endTime,
        ActionListener<SinglePointFeatures> listener
    ) {

        double[][] currentPoints = filterAndFill(shingle, endTime, detector);
        Optional<double[]> currentPoint = Optional.ofNullable(shingle.peekLast()).map(Entry::getValue);
        listener
            .onResponse(
                Optional
                    .ofNullable(currentPoints)
                    .map(points -> new SinglePointFeatures(currentPoint, Optional.of(batchShingle(points, shingleSize)[0])))
                    .orElse(new SinglePointFeatures(currentPoint, Optional.empty()))
            );
    }

    private double[][] filterAndFill(Deque<Entry<Long, double[]>> shingle, long endTime, AnomalyDetector detector) {
        long intervalMilli = ((IntervalTimeConfiguration) detector.getDetectionInterval()).toDuration().toMillis();
        double[][] result = null;
        if (shingle.size() >= shingleSize - maxMissingPoints) {
            TreeMap<Long, double[]> search = new TreeMap<>(shingle.stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue)));
            result = IntStream.rangeClosed(1, shingleSize).mapToLong(i -> endTime - (shingleSize - i) * intervalMilli).mapToObj(t -> {
                Optional<Entry<Long, double[]>> after = Optional.ofNullable(search.ceilingEntry(t));
                Optional<Entry<Long, double[]>> before = Optional.ofNullable(search.floorEntry(t));
                return after
                    .filter(a -> Math.abs(t - a.getKey()) <= before.map(b -> Math.abs(t - b.getKey())).orElse(Long.MAX_VALUE))
                    .map(Optional::of)
                    .orElse(before)
                    .filter(e -> Math.abs(t - e.getKey()) < intervalMilli * maxNeighborDistance)
                    .map(Entry::getValue)
                    .orElse(null);
            }).filter(d -> d != null).toArray(double[][]::new);
            if (result.length < shingleSize) {
                result = null;
            }
        }
        return result;
    }

    /**
     * Provides data for cold-start training.
     *
     * @deprecated use getColdStartData with listener instead.
     *
     * Training data starts with getting samples from (costly) search.
     * Samples are increased in size via interpolation and then
     * in dimension via shingling.
     *
     * @param detector contains data info (indices, documents, etc)
     * @return data for cold-start training, or empty if unavailable
     */
    @Deprecated
    public Optional<double[][]> getColdStartData(AnomalyDetector detector) {
        return searchFeatureDao
            .getLatestDataTime(detector)
            .flatMap(latest -> searchFeatureDao.getFeaturesForSampledPeriods(detector, maxTrainSamples, maxSampleStride, latest))
            .map(
                samples -> transpose(
                    interpolator.interpolate(transpose(samples.getKey()), samples.getValue() * (samples.getKey().length - 1) + 1)
                )
            )
            .map(points -> batchShingle(points, shingleSize));
    }

    /**
     * Returns to listener data for cold-start training.
     *
     * Training data starts with getting samples from (costly) search.
     * Samples are increased in size via interpolation and then
     * in dimension via shingling.
     *
     * @param detector contains data info (indices, documents, etc)
     * @param listener onResponse is called with data for cold-start training, or empty if unavailable
     */
    public void getColdStartData(AnomalyDetector detector, ActionListener<Optional<double[][]>> listener) {
        searchFeatureDao
            .getLatestDataTime(
                detector,
                ActionListener.wrap(latest -> getColdStartSamples(latest, detector, listener), listener::onFailure)
            );
    }

    private void getColdStartSamples(Optional<Long> latest, AnomalyDetector detector, ActionListener<Optional<double[][]>> listener) {
        if (latest.isPresent()) {
            searchFeatureDao
                .getFeaturesForSampledPeriods(
                    detector,
                    maxTrainSamples,
                    maxSampleStride,
                    latest.get(),
                    ActionListener.wrap(samples -> processColdStartSamples(samples, listener), listener::onFailure)
                );
        } else {
            listener.onResponse(Optional.empty());
        }
    }

    private void processColdStartSamples(Optional<Entry<double[][], Integer>> samples, ActionListener<Optional<double[][]>> listener) {
        listener
            .onResponse(
                samples
                    .map(
                        results -> transpose(
                            interpolator.interpolate(transpose(results.getKey()), results.getValue() * (results.getKey().length - 1) + 1)
                        )
                    )
                    .map(points -> batchShingle(points, shingleSize))
            );
    }

    /**
     * Shingles a batch of data points by concatenating neighboring data points.
     *
     * @param points M, N where M is the number of data points and N is the dimension of a point
     * @param shingleSize the size of a shingle
     * @return P, Q where P = M - {@code shingleSize} + 1 and Q = N * {@code shingleSize}
     * @throws IllegalArgumentException when input is invalid
     */
    public double[][] batchShingle(double[][] points, int shingleSize) {
        if (points.length == 0 || points[0].length == 0 || points.length < shingleSize || shingleSize < 1) {
            throw new IllegalArgumentException("Invalid data for shingling.");
        }
        int numPoints = points.length;
        int dimPoint = points[0].length;
        int numShingles = numPoints - shingleSize + 1;
        int dimShingle = dimPoint * shingleSize;
        double[][] shingles = new double[numShingles][dimShingle];
        for (int i = 0; i < numShingles; i++) {
            for (int j = 0; j < shingleSize; j++) {
                System.arraycopy(points[i + j], 0, shingles[i], j * dimPoint, dimPoint);
            }
        }
        return shingles;
    }

    /**
     * Deletes managed features for the detector.
     *
     * @param detectorId ID of the detector
     */
    public void clear(String detectorId) {
        detectorIdsToTimeShingles.remove(detectorId);
    }

    /**
     * Does maintenance work.
     *
     * The current implementation removes feature buffers that are updated more than ttlFeatureBuffer (3 days for example) ago.
     * The cleanup is needed since feature buffers are not explicitly deleted after a detector is deleted or relocated.
     */
    public void maintenance() {
        try {
            detectorIdsToTimeShingles
                .entrySet()
                .removeIf(
                    idQueue -> Optional
                        .ofNullable(idQueue.getValue().peekLast())
                        .map(p -> Instant.ofEpochMilli(p.getKey()).plus(featureBufferTtl).isBefore(clock.instant()))
                        .orElse(true)
                );
        } catch (Exception e) {
            logger.warn("Caught exception during maintenance", e);
        }
    }

    /**
     * Gets feature data points (unprocessed and processed) from the period for preview purpose.
     *
     * @deprecated use getPreviewFeatures with listener instead.
     *
     * Due to the constraints (workload, latency) from preview, a small number of data samples are from actual
     * query results and the remaining are from interpolation. The results are approximate to the actual features.
     *
     * @param detector detector info containing indices, features, interval, etc
     * @param startMilli start of the range in epoch milliseconds
     * @param endMilli end of the range in epoch milliseconds
     * @return time ranges, unprocessed features, and processed features of the data points from the period
     */
    @Deprecated
    public Features getPreviewFeatures(AnomalyDetector detector, long startMilli, long endMilli) {
        Entry<List<Entry<Long, Long>>, Integer> sampleRangeResults = getSampleRanges(detector, startMilli, endMilli);
        List<Entry<Long, Long>> sampleRanges = sampleRangeResults.getKey();
        int stride = sampleRangeResults.getValue();

        Entry<List<Entry<Long, Long>>, double[][]> samples = getSamplesForRanges(detector, sampleRanges);
        sampleRanges = samples.getKey();
        double[][] sampleFeatures = samples.getValue();

        List<Entry<Long, Long>> previewRanges = getPreviewRanges(sampleRanges, stride);
        Entry<double[][], double[][]> previewFeatures = getPreviewFeatures(sampleFeatures, stride);
        return new Features(previewRanges, previewFeatures.getKey(), previewFeatures.getValue());
    }

    /**
     * Returns to listener feature data points (unprocessed and processed) from the period for preview purpose.
     *
     * Due to the constraints (workload, latency) from preview, a small number of data samples are from actual
     * query results and the remaining are from interpolation. The results are approximate to the actual features.
     *
     * @param detector detector info containing indices, features, interval, etc
     * @param startMilli start of the range in epoch milliseconds
     * @param endMilli end of the range in epoch milliseconds
     * @param listener onResponse is called with time ranges, unprocessed features,
     *                                      and processed features of the data points from the period
     *                 onFailure is called with IllegalArgumentException when there is no data to preview
     * @throws IOException if a user gives wrong query input when defining a detector
     */
    public void getPreviewFeatures(AnomalyDetector detector, long startMilli, long endMilli, ActionListener<Features> listener)
        throws IOException {
        Entry<List<Entry<Long, Long>>, Integer> sampleRangeResults = getSampleRanges(detector, startMilli, endMilli);
        List<Entry<Long, Long>> sampleRanges = sampleRangeResults.getKey();
        int stride = sampleRangeResults.getValue();

        getSamplesForRanges(detector, sampleRanges, ActionListener.wrap(samples -> {
            List<Entry<Long, Long>> searchTimeRange = samples.getKey();
            if (searchTimeRange.size() == 0) {
                listener.onFailure(new IllegalArgumentException("No data to preview anomaly detection."));
                return;
            }
            double[][] sampleFeatures = samples.getValue();

            List<Entry<Long, Long>> previewRanges = getPreviewRanges(searchTimeRange, stride);
            Entry<double[][], double[][]> previewFeatures = getPreviewFeatures(sampleFeatures, stride);
            listener.onResponse(new Features(previewRanges, previewFeatures.getKey(), previewFeatures.getValue()));
        }, listener::onFailure));
    }

    /**
     * Gets time ranges of sampled data points.
     *
     * To reduce workload/latency from search, most data points in the preview time ranges are not from search results.
     * This implementation selects up to maxPreviewSamples evenly spaced points from the entire time range.
     *
     * @return key is a list of sampled time ranges, value is the stride between samples
     */
    private Entry<List<Entry<Long, Long>>, Integer> getSampleRanges(AnomalyDetector detector, long startMilli, long endMilli) {
        long start = truncateToMinute(startMilli);
        long end = truncateToMinute(endMilli);
        long bucketSize = ((IntervalTimeConfiguration) detector.getDetectionInterval()).toDuration().toMillis();
        int numBuckets = (int) Math.floor((end - start) / (double) bucketSize);
        int numSamples = (int) Math.max(Math.min(numBuckets * previewSampleRate, maxPreviewSamples), 1);
        int stride = (int) Math.max(1, Math.floor((double) numBuckets / numSamples));
        int numStrides = (int) Math.ceil(numBuckets / (double) stride);
        List<Entry<Long, Long>> sampleRanges = Stream
            .iterate(start, i -> i + stride * bucketSize)
            .limit(numStrides)
            .map(time -> new SimpleImmutableEntry<>(time, time + bucketSize))
            .collect(Collectors.toList());
        return new SimpleImmutableEntry<>(sampleRanges, stride);
    }

    /**
     * Gets search results for the sampled time ranges.
     *
     * @return key is time ranges, value is corresponding search results
     */
    @Deprecated
    private Entry<List<Entry<Long, Long>>, double[][]> getSamplesForRanges(AnomalyDetector detector, List<Entry<Long, Long>> sampleRanges) {
        List<Optional<double[]>> featureSamples = searchFeatureDao.getFeatureSamplesForPeriods(detector, sampleRanges);
        List<Entry<Long, Long>> ranges = new ArrayList<>(featureSamples.size());
        List<double[]> samples = new ArrayList<>(featureSamples.size());
        for (int i = 0; i < featureSamples.size(); i++) {
            Entry<Long, Long> currentRange = sampleRanges.get(i);
            featureSamples.get(i).ifPresent(sample -> {
                ranges.add(currentRange);
                samples.add(sample);
            });
        }
        return new SimpleImmutableEntry<>(ranges, samples.toArray(new double[0][0]));
    }

    /**
     * Gets search results for the sampled time ranges.
     *
     * @param listener handle search results map: key is time ranges, value is corresponding search results
     * @throws IOException if a user gives wrong query input when defining a detector
     */
    void getSamplesForRanges(
        AnomalyDetector detector,
        List<Entry<Long, Long>> sampleRanges,
        ActionListener<Entry<List<Entry<Long, Long>>, double[][]>> listener
    ) throws IOException {
        searchFeatureDao.getFeatureSamplesForPeriods(detector, sampleRanges, ActionListener.wrap(featureSamples -> {
            List<Entry<Long, Long>> ranges = new ArrayList<>(featureSamples.size());
            List<double[]> samples = new ArrayList<>(featureSamples.size());
            for (int i = 0; i < featureSamples.size(); i++) {
                Entry<Long, Long> currentRange = sampleRanges.get(i);
                featureSamples.get(i).ifPresent(sample -> {
                    ranges.add(currentRange);
                    samples.add(sample);
                });
            }
            listener.onResponse(new SimpleImmutableEntry<>(ranges, samples.toArray(new double[0][0])));
        }, listener::onFailure));

    }

    /**
     * Gets time ranges for the data points in the preview range that begins with the first
     * sample time range and ends with the last.
     *
     * @param ranges time ranges of samples
     * @param stride the number of data points between samples
     * @return time ranges for all data points
     */
    private List<Entry<Long, Long>> getPreviewRanges(List<Entry<Long, Long>> ranges, int stride) {
        double[] rangeStarts = ranges.stream().mapToDouble(Entry::getKey).toArray();
        double[] rangeEnds = ranges.stream().mapToDouble(Entry::getValue).toArray();
        double[] previewRangeStarts = interpolator.interpolate(new double[][] { rangeStarts }, stride * (ranges.size() - 1) + 1)[0];
        double[] previewRangeEnds = interpolator.interpolate(new double[][] { rangeEnds }, stride * (ranges.size() - 1) + 1)[0];
        List<Entry<Long, Long>> previewRanges = IntStream
            .range(shingleSize - 1, previewRangeStarts.length)
            .mapToObj(i -> new SimpleImmutableEntry<>((long) previewRangeStarts[i], (long) previewRangeEnds[i]))
            .collect(Collectors.toList());
        return previewRanges;
    }

    /**
     * Gets unprocessed and processed features for the data points in the preview range.
     *
     * To reduce workload on search, the data points within the preview range are interpolated based on
     * sample query results. Unprocessed features are interpolated query results.
     * Processed features are inputs to models, transformed (such as shingle) from unprocessed features.
     *
     * @return unprocessed and procesed features
     */
    private Entry<double[][], double[][]> getPreviewFeatures(double[][] samples, int stride) {
        Entry<double[][], double[][]> unprocessedAndProcessed = Optional
            .of(samples)
            .map(m -> transpose(m))
            .map(m -> interpolator.interpolate(m, stride * (samples.length - 1) + 1))
            .map(m -> transpose(m))
            .map(m -> new SimpleImmutableEntry<>(copyOfRange(m, shingleSize - 1, m.length), batchShingle(m, shingleSize)))
            .get();
        return unprocessedAndProcessed;
    }

    private double[][] transpose(double[][] matrix) {
        return createRealMatrix(matrix).transpose().getData();
    }

    private long truncateToMinute(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis).truncatedTo(ChronoUnit.MINUTES).toEpochMilli();
    }
}
