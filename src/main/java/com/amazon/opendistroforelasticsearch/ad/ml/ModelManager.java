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

package com.amazon.opendistroforelasticsearch.ad.ml;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.monitor.jvm.JvmService;

import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ResourceNotFoundException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.ml.rcf.CombinedRcfResult;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;
import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.serialize.RandomCutForestSerDe;

/**
 * A facade managing ML operations and models.
 */
public class ModelManager {

    protected static final String DETECTOR_ID_PATTERN = "(.*)_model_.+";
    protected static final String RCF_MODEL_ID_PATTERN = "%s_model_rcf_%d";
    protected static final String THRESHOLD_MODEL_ID_PATTERN = "%s_model_threshold";

    public enum ModelType {
        RCF("rcf"),
        THRESHOLD("threshold");

        private String name;

        ModelType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    private static final double FULL_CONFIDENCE_EXPONENT = 18.43; // exponent over which confidence is 1

    private static final Logger logger = LogManager.getLogger(ModelManager.class);

    // states
    private Map<String, ModelState<RandomCutForest>> forests;
    private Map<String, ModelState<ThresholdingModel>> thresholds;

    // configuration
    private final double modelDesiredSizePercentage;
    private final double modelMaxSizePercentage;
    private final int rcfNumTrees;
    private final int rcfNumSamplesInTree;
    private final double rcfTimeDecay;
    private final double thresholdMinPvalue;
    private final double thresholdMaxRankError;
    private final double thresholdMaxScore;
    private final int thresholdNumLogNormalQuantiles;
    private final int thresholdDownsamples;
    private final long thresholdMaxSamples;
    private final Class<? extends ThresholdingModel> thresholdingModelClass;
    private final int minPreviewSize;
    private final Duration modelTtl;
    private final Duration checkpointInterval;

    // dependencies
    private final DiscoveryNodeFilterer nodeFilter;
    private final JvmService jvmService;
    private final RandomCutForestSerDe rcfSerde;
    private final CheckpointDao checkpointDao;
    private final Gson gson;
    private final Clock clock;
    private final int shingleSize;

    // A tree of N samples has 2N nodes, with one bounding box for each node.
    private static final long BOUNDING_BOXES = 2L;
    // A bounding box has one vector for min values and one for max.
    private static final long VECTORS_IN_BOUNDING_BOX = 2L;

    /**
     * Constructor.
     *
     * @param nodeFilter utility class to select nodes
     * @param jvmService jvm info
     * @param rcfSerde RCF model serialization
     * @param checkpointDao model checkpoint storage
     * @param gson thresholding model serialization
     * @param clock clock for system time
     * @param modelDesiredSizePercentage percentage of heap for the desired size of a model
     * @param modelMaxSizePercentage percentage of heap for the max size of a model
     * @param rcfNumTrees number of trees used in RCF
     * @param rcfNumSamplesInTree number of samples in a RCF tree
     * @param rcfTimeDecay time decay for RCF
     * @param thresholdMinPvalue min P-value for thresholding
     * @param thresholdMaxRankError  max rank error for thresholding
     * @param thresholdMaxScore max RCF score to thresholding
     * @param thresholdNumLogNormalQuantiles num of lognormal quantiles for thresholding
     * @param thresholdDownsamples the number of samples to keep during downsampling
     * @param thresholdMaxSamples the max number of samples before downsampling
     * @param thresholdingModelClass class of thresholding model
     * @param minPreviewSize minimum number of data points for preview
     * @param modelTtl time to live for hosted models
     * @param checkpointInterval interval between checkpoints
     * @param shingleSize required shingle size before RCF emitting anomaly scores
     */
    public ModelManager(
        DiscoveryNodeFilterer nodeFilter,
        JvmService jvmService,
        RandomCutForestSerDe rcfSerde,
        CheckpointDao checkpointDao,
        Gson gson,
        Clock clock,
        double modelDesiredSizePercentage,
        double modelMaxSizePercentage,
        int rcfNumTrees,
        int rcfNumSamplesInTree,
        double rcfTimeDecay,
        double thresholdMinPvalue,
        double thresholdMaxRankError,
        double thresholdMaxScore,
        int thresholdNumLogNormalQuantiles,
        int thresholdDownsamples,
        long thresholdMaxSamples,
        Class<? extends ThresholdingModel> thresholdingModelClass,
        int minPreviewSize,
        Duration modelTtl,
        Duration checkpointInterval,
        int shingleSize
    ) {

        this.nodeFilter = nodeFilter;
        this.jvmService = jvmService;
        this.rcfSerde = rcfSerde;
        this.checkpointDao = checkpointDao;
        this.gson = gson;
        this.clock = clock;

        this.modelDesiredSizePercentage = modelDesiredSizePercentage;
        this.modelMaxSizePercentage = modelMaxSizePercentage;
        this.rcfNumTrees = rcfNumTrees;
        this.rcfNumSamplesInTree = rcfNumSamplesInTree;
        this.rcfTimeDecay = rcfTimeDecay;
        this.thresholdMinPvalue = thresholdMinPvalue;
        this.thresholdMaxRankError = thresholdMaxRankError;
        this.thresholdMaxScore = thresholdMaxScore;
        this.thresholdNumLogNormalQuantiles = thresholdNumLogNormalQuantiles;
        this.thresholdDownsamples = thresholdDownsamples;
        this.thresholdMaxSamples = thresholdMaxSamples;
        this.thresholdingModelClass = thresholdingModelClass;
        this.minPreviewSize = minPreviewSize;
        this.modelTtl = modelTtl;
        this.checkpointInterval = checkpointInterval;

        this.forests = new ConcurrentHashMap<>();
        this.thresholds = new ConcurrentHashMap<>();
        this.shingleSize = shingleSize;
    }

    /**
     * Combines RCF results into a single result.
     *
     * Final RCF score is calculated by averaging scores weighted by model size (number of trees).
     * Confidence is the weighted average of confidence with confidence for missing models being 0.
     *
     * @param rcfResults RCF results from partitioned models
     * @return combined RCF result
     */
    public CombinedRcfResult combineRcfResults(List<RcfResult> rcfResults) {
        CombinedRcfResult combinedResult = null;
        if (rcfResults.isEmpty()) {
            combinedResult = new CombinedRcfResult(0, 0);
        } else {
            int totalForestSize = rcfResults.stream().mapToInt(RcfResult::getForestSize).sum();
            if (totalForestSize == 0) {
                combinedResult = new CombinedRcfResult(0, 0);
            } else {
                double score = rcfResults.stream().mapToDouble(r -> r.getScore() * r.getForestSize()).sum() / totalForestSize;
                double confidence = rcfResults.stream().mapToDouble(r -> r.getConfidence() * r.getForestSize()).sum() / Math
                    .max(rcfNumTrees, totalForestSize);
                combinedResult = new CombinedRcfResult(score, confidence);
            }
        }
        return combinedResult;
    }

    /**
     * Gets the detector id from the model id.
     *
     * @param modelId id of a model
     * @return id of the detector the model is for
     * @throws IllegalArgumentException if model id is invalid
     */
    public String getDetectorIdForModelId(String modelId) {
        Matcher matcher = Pattern.compile(DETECTOR_ID_PATTERN).matcher(modelId);
        if (matcher.matches()) {
            return matcher.group(1);
        } else {
            throw new IllegalArgumentException("Invalid model id " + modelId);
        }
    }

    /**
     * Partitions a RCF model by forest size.
     *
     * A RCF model is first partitioned into desired size based on heap.
     * If there are more partitions than the number of nodes in the cluster,
     * the model is partitioned by the number of nodes and verified to
     * ensure the size of a partition does not exceed the max size limit based on heap.
     *
     * @param forest RCF configuration, including forest size
     * @param detectorId ID of the detector with no effects on partitioning
     * @return a pair of number of partitions and size of a parition (number of trees)
     * @throws LimitExceededException when there is no sufficient resouce available
     */
    public Entry<Integer, Integer> getPartitionedForestSizes(RandomCutForest forest, String detectorId) {
        long totalSize = estimateModelSize(forest);
        long heapSize = jvmService.info().getMem().getHeapMax().getBytes();

        // desired partitioning
        long partitionSize = (long) (Math.min(heapSize * modelDesiredSizePercentage, totalSize));
        int numPartitions = (int) Math.ceil((double) totalSize / (double) partitionSize);
        int forestSize = (int) Math.ceil((double) forest.getNumberOfTrees() / (double) numPartitions);

        int numNodes = nodeFilter.getEligibleDataNodes().length;
        if (numPartitions > numNodes) {
            // partition by cluster size
            partitionSize = (long) Math.ceil((double) totalSize / (double) numNodes);
            long maxPartitionSize = (long) (heapSize * modelMaxSizePercentage);
            // verify against max size limit
            if (partitionSize <= maxPartitionSize) {
                numPartitions = numNodes;
                forestSize = (int) Math.ceil((double) forest.getNumberOfTrees() / (double) numNodes);
            } else {
                throw new LimitExceededException(detectorId, CommonErrorMessages.MEMORY_LIMIT_EXCEEDED_ERR_MSG);
            }
        }

        return new SimpleImmutableEntry<>(numPartitions, forestSize);
    }

    /**
     * Construct a RCF model and then partition it by forest size.
     *
     * A RCF model is constructed based on the number of input features.
     *
     * Then a RCF model is first partitioned into desired size based on heap.
     * If there are more partitions than the number of nodes in the cluster,
     * the model is partitioned by the number of nodes and verified to
     * ensure the size of a partition does not exceed the max size limit based on heap.
     *
     * @param detector detector object
     * @return a pair of number of partitions and size of a parition (number of trees)
     * @throws LimitExceededException when there is no sufficient resouce available
     */
    public Entry<Integer, Integer> getPartitionedForestSizes(AnomalyDetector detector) {
        String detectorId = detector.getDetectorId();
        int rcfNumFeatures = detector.getEnabledFeatureIds().size() * shingleSize;
        return getPartitionedForestSizes(
            RandomCutForest
                .builder()
                .dimensions(rcfNumFeatures)
                .sampleSize(rcfNumSamplesInTree)
                .numberOfTrees(rcfNumTrees)
                .outputAfter(rcfNumSamplesInTree)
                .parallelExecutionEnabled(false)
                .build(),
            detectorId
        );
    }

    /**
     * Gets the estimated size of a RCF model.
     *
     * @param forest RCF configuration
     * @return estimated model size in bytes
     */
    public long estimateModelSize(RandomCutForest forest) {
        return (long) forest.getNumberOfTrees() * (long) forest.getSampleSize() * BOUNDING_BOXES * VECTORS_IN_BOUNDING_BOX * forest
            .getDimensions() * (Long.SIZE / Byte.SIZE);
    }

    /**
     * Gets the RCF anomaly result using the specified model.
     *
     * @deprecated use getRcfResult with listener instead.
     *
     * @param detectorId ID of the detector
     * @param modelId ID of the model to score the point
     * @param point features of the data point
     * @return RCF result for the input point, including a score
     * @throws ResourceNotFoundException when the model is not found
     * @throws LimitExceededException when a limit is exceeded for the model
     */
    @Deprecated
    public RcfResult getRcfResult(String detectorId, String modelId, double[] point) {
        ModelState<RandomCutForest> modelState = forests
            .computeIfAbsent(
                modelId,
                model -> checkpointDao
                    .getModelCheckpoint(model)
                    .map(
                        checkpoint -> AccessController.doPrivileged((PrivilegedAction<RandomCutForest>) () -> rcfSerde.fromJson(checkpoint))
                    )
                    .filter(rcf -> isHostingAllowed(detectorId, rcf))
                    .map(rcf -> new ModelState<>(rcf, modelId, detectorId, ModelType.RCF.getName(), clock.instant()))
                    .orElseThrow(() -> new ResourceNotFoundException(detectorId, CommonErrorMessages.NO_CHECKPOINT_ERR_MSG + modelId))
            );

        RandomCutForest rcf = modelState.getModel();
        double score = rcf.getAnomalyScore(point);
        double confidence = computeRcfConfidence(rcf);
        int forestSize = rcf.getNumberOfTrees();
        rcf.update(point);
        modelState.setLastUsedTime(clock.instant());
        return new RcfResult(score, confidence, forestSize);
    }

    /**
     * Returns to listener the RCF anomaly result using the specified model.
     *
     * @param detectorId ID of the detector
     * @param modelId ID of the model to score the point
     * @param point features of the data point
     * @param listener onResponse is called with RCF result for the input point, including a score
     *                 onFailure is called with ResourceNotFoundException when the model is not found
     *                 onFailure is called with LimitExceededException when a limit is exceeded for the model
     */
    public void getRcfResult(String detectorId, String modelId, double[] point, ActionListener<RcfResult> listener) {
        if (forests.containsKey(modelId)) {
            getRcfResult(forests.get(modelId), point, listener);
        } else {
            checkpointDao
                .getModelCheckpoint(
                    modelId,
                    ActionListener
                        .wrap(checkpoint -> processRcfCheckpoint(checkpoint, modelId, detectorId, point, listener), listener::onFailure)
                );
        }
    }

    private void getRcfResult(ModelState<RandomCutForest> modelState, double[] point, ActionListener<RcfResult> listener) {
        RandomCutForest rcf = modelState.getModel();
        double score = rcf.getAnomalyScore(point);
        double confidence = computeRcfConfidence(rcf);
        int forestSize = rcf.getNumberOfTrees();
        rcf.update(point);
        modelState.setLastUsedTime(clock.instant());
        listener.onResponse(new RcfResult(score, confidence, forestSize));
    }

    private void processRcfCheckpoint(
        Optional<String> rcfCheckpoint,
        String modelId,
        String detectorId,
        double[] point,
        ActionListener<RcfResult> listener
    ) {
        Optional<ModelState<RandomCutForest>> model = rcfCheckpoint
            .map(checkpoint -> AccessController.doPrivileged((PrivilegedAction<RandomCutForest>) () -> rcfSerde.fromJson(checkpoint)))
            .filter(rcf -> isHostingAllowed(detectorId, rcf))
            .map(rcf -> new ModelState<>(rcf, modelId, detectorId, ModelType.RCF.getName(), clock.instant()));
        if (model.isPresent()) {
            forests.put(modelId, model.get());
            getRcfResult(model.get(), point, listener);
        } else {
            throw new ResourceNotFoundException(detectorId, CommonErrorMessages.NO_CHECKPOINT_ERR_MSG + modelId);
        }
    }

    /**
     * Gets the result using the specified thresholding model.
     *
     * @deprecated use getThresholdingResult with listener instead.
     *
     * @param detectorId ID of the detector
     * @param modelId ID of the thresholding model
     * @param score raw anomaly score
     * @return thresholding model result for the raw score
     * @throws ResourceNotFoundException when the model is not found
     */
    @Deprecated
    public ThresholdingResult getThresholdingResult(String detectorId, String modelId, double score) {
        ModelState<ThresholdingModel> modelState = thresholds
            .computeIfAbsent(
                modelId,
                model -> checkpointDao
                    .getModelCheckpoint(model)
                    .map(
                        checkpoint -> AccessController
                            .doPrivileged((PrivilegedAction<ThresholdingModel>) () -> gson.fromJson(checkpoint, thresholdingModelClass))
                    )
                    .map(threshold -> new ModelState<>(threshold, modelId, detectorId, ModelType.THRESHOLD.getName(), clock.instant()))
                    .orElseThrow(() -> new ResourceNotFoundException(detectorId, CommonErrorMessages.NO_CHECKPOINT_ERR_MSG + modelId))
            );

        ThresholdingModel threshold = modelState.getModel();
        double grade = threshold.grade(score);
        double confidence = threshold.confidence();
        threshold.update(score);
        modelState.setLastUsedTime(clock.instant());
        return new ThresholdingResult(grade, confidence);
    }

    /**
     * Returns to listener the result using the specified thresholding model.
     *
     * @param detectorId ID of the detector
     * @param modelId ID of the thresholding model
     * @param score raw anomaly score
     * @param listener onResponse is called with the thresholding model result for the raw score
     *                 onFailure is called with ResourceNotFoundException when the model is not found
     */
    public void getThresholdingResult(String detectorId, String modelId, double score, ActionListener<ThresholdingResult> listener) {
        if (thresholds.containsKey(modelId)) {
            getThresholdingResult(thresholds.get(modelId), score, listener);
        } else {
            checkpointDao
                .getModelCheckpoint(
                    modelId,
                    ActionListener
                        .wrap(
                            checkpoint -> processThresholdCheckpoint(checkpoint, modelId, detectorId, score, listener),
                            listener::onFailure
                        )
                );
        }
    }

    private void getThresholdingResult(
        ModelState<ThresholdingModel> modelState,
        double score,
        ActionListener<ThresholdingResult> listener
    ) {
        ThresholdingModel threshold = modelState.getModel();
        double grade = threshold.grade(score);
        double confidence = threshold.confidence();
        threshold.update(score);
        modelState.setLastUsedTime(clock.instant());
        listener.onResponse(new ThresholdingResult(grade, confidence));
    }

    private void processThresholdCheckpoint(
        Optional<String> thresholdCheckpoint,
        String modelId,
        String detectorId,
        double score,
        ActionListener<ThresholdingResult> listener
    ) {

        Optional<ModelState<ThresholdingModel>> model = thresholdCheckpoint
            .map(
                checkpoint -> AccessController
                    .doPrivileged((PrivilegedAction<ThresholdingModel>) () -> gson.fromJson(checkpoint, thresholdingModelClass))
            )
            .map(threshold -> new ModelState<>(threshold, modelId, detectorId, ModelType.THRESHOLD.getName(), clock.instant()));
        if (model.isPresent()) {
            thresholds.put(modelId, model.get());
            getThresholdingResult(model.get(), score, listener);
        } else {
            throw new ResourceNotFoundException(detectorId, CommonErrorMessages.NO_CHECKPOINT_ERR_MSG + modelId);
        }
    }

    /**
     * Gets ids of all hosted models.
     *
     * @return ids of all hosted models.
     */
    public Set<String> getAllModelIds() {
        return Stream.of(forests.keySet(), thresholds.keySet()).flatMap(set -> set.stream()).collect(Collectors.toSet());
    }

    /**
     * Gets modelStates of all model partitions hosted on a node
     *
     * @return list of modelStates
     */
    public List<ModelState<?>> getAllModels() {
        return Stream.concat(forests.values().stream(), thresholds.values().stream()).collect(Collectors.toList());
    }

    /**
     * Stops hosting the model and creates a checkpoint.
     *
     * @deprecated use stopModel with listener instead.
     *
     * @param detectorId ID of the detector for informational purposes
     * @param modelId ID of the model to stop hosting
     */
    @Deprecated
    public void stopModel(String detectorId, String modelId) {
        logger.info(String.format("Stopping detector %s model %s", detectorId, modelId));
        stopModel(forests, modelId, this::toCheckpoint);
        stopModel(thresholds, modelId, this::toCheckpoint);
    }

    private <T> void stopModel(Map<String, ModelState<T>> models, String modelId, Function<T, String> toCheckpoint) {
        Instant now = clock.instant();
        Optional
            .ofNullable(models.remove(modelId))
            .filter(model -> model.getLastCheckpointTime().plus(checkpointInterval).isBefore(now))
            .ifPresent(model -> { checkpointDao.putModelCheckpoint(modelId, toCheckpoint.apply(model.getModel())); });
    }

    /**
     * Stops hosting the model and creates a checkpoint.
     *
     * @param detectorId ID of the detector
     * @param modelId ID of the model to stop hosting
     * @param listener onResponse is called with null when the operation is completed
     */
    public void stopModel(String detectorId, String modelId, ActionListener<Void> listener) {
        logger.info(String.format("Stopping detector %s model %s", detectorId, modelId));
        stopModel(
            forests,
            modelId,
            this::toCheckpoint,
            ActionListener.wrap(r -> stopModel(thresholds, modelId, this::toCheckpoint, listener), listener::onFailure)
        );
    }

    private <T> void stopModel(
        Map<String, ModelState<T>> models,
        String modelId,
        Function<T, String> toCheckpoint,
        ActionListener<Void> listener
    ) {
        Instant now = clock.instant();
        Optional<ModelState<T>> modelState = Optional
            .ofNullable(models.remove(modelId))
            .filter(model -> model.getLastCheckpointTime().plus(checkpointInterval).isBefore(now));
        if (modelState.isPresent()) {
            modelState
                .ifPresent(
                    model -> checkpointDao
                        .putModelCheckpoint(
                            modelId,
                            toCheckpoint.apply(model.getModel()),
                            ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure)
                        )
                );
        } else {
            listener.onResponse(null);
        }
        ;
    }

    /**
     * Permanently deletes models hosted in memory and persisted in index.
     *
     * @deprecated use clear with listener instead.
     *
     * @param detectorId id the of the detector for which models are to be permanently deleted
     */
    @Deprecated
    public void clear(String detectorId) {
        clearModels(detectorId, forests);
        clearModels(detectorId, thresholds);
    }

    /**
     * Permanently deletes models hosted in memory and persisted in index.
     *
     * @param detectorId id the of the detector for which models are to be permanently deleted
     * @param listener onResponse is called with null when this operation is completed
     */
    public void clear(String detectorId, ActionListener<Void> listener) {
        clearModels(detectorId, forests, ActionListener.wrap(r -> clearModels(detectorId, thresholds, listener), listener::onFailure));
    }

    private void clearModels(String detectorId, Map<String, ?> models, ActionListener<Void> listener) {
        Iterator<String> id = models.keySet().iterator();
        clearModelForIterator(detectorId, models, id, listener);
    }

    private void clearModelForIterator(String detectorId, Map<String, ?> models, Iterator<String> idIter, ActionListener<Void> listener) {
        if (idIter.hasNext()) {
            String modelId = idIter.next();
            if (getDetectorIdForModelId(modelId).equals(detectorId)) {
                models.remove(modelId);
                checkpointDao
                    .deleteModelCheckpoint(
                        modelId,
                        ActionListener.wrap(r -> clearModelForIterator(detectorId, models, idIter, listener), listener::onFailure)
                    );
            } else {
                clearModelForIterator(detectorId, models, idIter, listener);
            }
        } else {
            listener.onResponse(null);
        }
    }

    /**
     * Trains and saves cold-start AD models.
     *
     * @deprecated use trainModel with listener instead.
     *
     * This implementations splits RCF models and trains them all.
     * As all model partitions have the same size, the scores from RCF models are merged by averaging.
     * Since RCF outputs 0 until it is ready, initial 0 scores are meaningless and therefore filtered out.
     * Filtered (non-zero) RCF scores are the training data for a single thresholding model.
     * All trained models are serialized and persisted to be hosted.
     *
     * @param anomalyDetector the detector for which models are trained
     * @param dataPoints M, N shape, where M is the number of samples for training and N is the number of features
     * @throws IllegalArgumentException when training data is incomplete
     */
    @Deprecated
    public void trainModel(AnomalyDetector anomalyDetector, double[][] dataPoints) {
        if (dataPoints.length == 0 || dataPoints[0].length == 0) {
            throw new IllegalArgumentException("Data points must not be empty.");
        }
        if (dataPoints[0].length != anomalyDetector.getEnabledFeatureIds().size() * shingleSize) {
            throw new IllegalArgumentException(
                String
                    .format(
                        Locale.ROOT,
                        "Feature dimension is not correct, we expect %s but get %d",
                        anomalyDetector.getEnabledFeatureIds().size() * shingleSize,
                        dataPoints[0].length
                    )
            );
        }
        int rcfNumFeatures = dataPoints[0].length;

        // Create partitioned RCF models
        Entry<Integer, Integer> partitionResults = getPartitionedForestSizes(anomalyDetector);

        int numForests = partitionResults.getKey();
        int forestSize = partitionResults.getValue();
        double[] scores = new double[dataPoints.length];
        Arrays.fill(scores, 0.);
        for (int i = 0; i < numForests; i++) {
            RandomCutForest rcf = RandomCutForest
                .builder()
                .dimensions(rcfNumFeatures)
                .sampleSize(rcfNumSamplesInTree)
                .numberOfTrees(forestSize)
                .lambda(rcfTimeDecay)
                .outputAfter(rcfNumSamplesInTree)
                .parallelExecutionEnabled(false)
                .build();
            for (int j = 0; j < dataPoints.length; j++) {
                scores[j] += rcf.getAnomalyScore(dataPoints[j]);
                rcf.update(dataPoints[j]);
            }
            String modelId = getRcfModelId(anomalyDetector.getDetectorId(), i);
            String checkpoint = AccessController.doPrivileged((PrivilegedAction<String>) () -> rcfSerde.toJson(rcf));
            checkpointDao.putModelCheckpoint(modelId, checkpoint);
        }

        scores = DoubleStream.of(scores).filter(score -> score > 0).map(score -> score / numForests).toArray();

        // Train thresholding model
        ThresholdingModel threshold = new HybridThresholdingModel(
            thresholdMinPvalue,
            thresholdMaxRankError,
            thresholdMaxScore,
            thresholdNumLogNormalQuantiles,
            thresholdDownsamples,
            thresholdMaxSamples
        );
        threshold.train(scores);

        // Persist thresholding model
        String modelId = getThresholdModelId(anomalyDetector.getDetectorId());
        String checkpoint = AccessController.doPrivileged((PrivilegedAction<String>) () -> gson.toJson(threshold));
        checkpointDao.putModelCheckpoint(modelId, checkpoint);
    }

    /**
    * Trains and saves cold-start AD models.
    *
    * This implementations splits RCF models and trains them all.
    * As all model partitions have the same size, the scores from RCF models are merged by averaging.
    * Since RCF outputs 0 until it is ready, initial 0 scores are meaningless and therefore filtered out.
    * Filtered (non-zero) RCF scores are the training data for a single thresholding model.
    * All trained models are serialized and persisted to be hosted.
    *
    * @param anomalyDetector the detector for which models are trained
    * @param dataPoints M, N shape, where M is the number of samples for training and N is the number of features
    * @param listener onResponse is called with null when this operation is completed
    *                 onFailure is called IllegalArgumentException when training data is invalid
    *                 onFailure is called LimitExceededException when a limit for training is exceeded
    */
    public void trainModel(AnomalyDetector anomalyDetector, double[][] dataPoints, ActionListener<Void> listener) {
        if (dataPoints.length == 0 || dataPoints[0].length == 0) {
            listener.onFailure(new IllegalArgumentException("Data points must not be empty."));
        } else {
            int rcfNumFeatures = dataPoints[0].length;
            // creates partitioned RCF models
            try {
                Entry<Integer, Integer> partitionResults = getPartitionedForestSizes(
                    RandomCutForest
                        .builder()
                        .dimensions(rcfNumFeatures)
                        .sampleSize(rcfNumSamplesInTree)
                        .numberOfTrees(rcfNumTrees)
                        .outputAfter(rcfNumSamplesInTree)
                        .parallelExecutionEnabled(false)
                        .build(),
                    anomalyDetector.getDetectorId()
                );
                int numForests = partitionResults.getKey();
                int forestSize = partitionResults.getValue();
                double[] scores = new double[dataPoints.length];
                Arrays.fill(scores, 0.);
                trainModelForStep(anomalyDetector, dataPoints, rcfNumFeatures, numForests, forestSize, scores, 0, listener);
            } catch (LimitExceededException e) {
                listener.onFailure(e);
            }
        }
    }

    private void trainModelForStep(
        AnomalyDetector detector,
        double[][] dataPoints,
        int rcfNumFeatures,
        int numForests,
        int forestSize,
        final double[] scores,
        int step,
        ActionListener<Void> listener
    ) {
        if (step < numForests) {
            RandomCutForest rcf = RandomCutForest
                .builder()
                .dimensions(rcfNumFeatures)
                .sampleSize(rcfNumSamplesInTree)
                .numberOfTrees(forestSize)
                .lambda(rcfTimeDecay)
                .outputAfter(rcfNumSamplesInTree)
                .parallelExecutionEnabled(false)
                .build();
            for (int j = 0; j < dataPoints.length; j++) {
                scores[j] += rcf.getAnomalyScore(dataPoints[j]);
                rcf.update(dataPoints[j]);
            }
            String modelId = getRcfModelId(detector.getDetectorId(), step);
            String checkpoint = AccessController.doPrivileged((PrivilegedAction<String>) () -> rcfSerde.toJson(rcf));
            checkpointDao
                .putModelCheckpoint(
                    modelId,
                    checkpoint,
                    ActionListener
                        .wrap(
                            r -> trainModelForStep(
                                detector,
                                dataPoints,
                                rcfNumFeatures,
                                numForests,
                                forestSize,
                                scores,
                                step + 1,
                                listener
                            ),
                            listener::onFailure
                        )
                );
        } else {
            double[] rcfScores = DoubleStream.of(scores).filter(score -> score > 0).map(score -> score / numForests).toArray();

            // Train thresholding model
            ThresholdingModel threshold = new HybridThresholdingModel(
                thresholdMinPvalue,
                thresholdMaxRankError,
                thresholdMaxScore,
                thresholdNumLogNormalQuantiles,
                thresholdDownsamples,
                thresholdMaxSamples
            );
            threshold.train(rcfScores);

            // Persist thresholding model
            String modelId = getThresholdModelId(detector.getDetectorId());
            String checkpoint = AccessController.doPrivileged((PrivilegedAction<String>) () -> gson.toJson(threshold));
            checkpointDao.putModelCheckpoint(modelId, checkpoint, ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure));
        }
    }

    /**
     * Returns the model ID for the RCF model partition.
     *
     * @param detectorId ID of the detector for which the RCF model is trained
     * @param partitionNumber number of the partition
     * @return ID for the RCF model partition
     */
    public String getRcfModelId(String detectorId, int partitionNumber) {
        return String.format(RCF_MODEL_ID_PATTERN, detectorId, partitionNumber);
    }

    /**
     * Returns the model ID for the thresholding model.
     *
     * @param detectorId ID of the detector for which the thresholding model is trained
     * @return ID for the thresholding model
     */
    public String getThresholdModelId(String detectorId) {
        return String.format(THRESHOLD_MODEL_ID_PATTERN, detectorId);
    }

    private void clearModels(String detectorId, Map<String, ?> models) {
        models.keySet().stream().filter(modelId -> getDetectorIdForModelId(modelId).equals(detectorId)).forEach(modelId -> {
            models.remove(modelId);
            checkpointDao.deleteModelCheckpoint(modelId);
        });
    }

    private boolean isHostingAllowed(String detectorId, RandomCutForest rcf) {
        long total = forests.values().stream().mapToLong(f -> estimateModelSize(f.getModel())).sum() + estimateModelSize(rcf);
        double heapLimit = jvmService.info().getMem().getHeapMax().getBytes() * modelMaxSizePercentage;
        if (total <= heapLimit) {
            return true;
        } else {
            throw new LimitExceededException(
                detectorId,
                String.format("Exceeded memory limit. New size is %d bytes and max limit is %f bytes", total, heapLimit)
            );
        }
    }

    private String toCheckpoint(RandomCutForest forest) {
        return AccessController.doPrivileged((PrivilegedAction<String>) () -> rcfSerde.toJson(forest));
    }

    private String toCheckpoint(ThresholdingModel threshold) {
        return AccessController.doPrivileged((PrivilegedAction<String>) () -> gson.toJson(threshold));
    }

    /**
     * Does periodical maintenance work.
     *
     * @deprecated use maintenance with listener instead.
     *
     * The implementation makes checkpoints for hosted models and stop hosting models not actively used.
     */
    @Deprecated
    public void maintenance() {
        maintenance(forests, this::toCheckpoint);
        maintenance(thresholds, this::toCheckpoint);
    }

    private <T> void maintenance(Map<String, ModelState<T>> models, Function<T, String> toCheckpoint) {
        models.entrySet().stream().forEach(entry -> {
            String modelId = entry.getKey();
            try {
                ModelState<T> modelState = entry.getValue();
                Instant now = clock.instant();
                if (modelState.getLastCheckpointTime().plus(checkpointInterval).isBefore(now)) {
                    checkpointDao.putModelCheckpoint(modelId, toCheckpoint.apply(modelState.getModel()));
                    modelState.setLastCheckpointTime(now);
                }
                if (modelState.getLastUsedTime().plus(modelTtl).isBefore(now)) {
                    models.remove(modelId);
                }
            } catch (Exception e) {
                logger.warn("Failed to finish maintenance for model id " + modelId, e);
            }
        });
    }

    /**
     * Does model maintenance.
     *
     * The implementation makes checkpoints for hosted models and stops hosting models not recently used.
     *
     * @param listener onResponse is called with null when this operation is completed.
     */
    public void maintenance(ActionListener<Void> listener) {
        maintenanceForIterator(
            forests,
            this::toCheckpoint,
            forests.entrySet().iterator(),
            ActionListener
                .wrap(
                    r -> maintenanceForIterator(thresholds, this::toCheckpoint, thresholds.entrySet().iterator(), listener),
                    listener::onFailure
                )
        );
    }

    private <T> void maintenanceForIterator(
        Map<String, ModelState<T>> models,
        Function<T, String> toCheckpoint,
        Iterator<Entry<String, ModelState<T>>> iter,
        ActionListener<Void> listener
    ) {
        if (iter.hasNext()) {
            Entry<String, ModelState<T>> modelEntry = iter.next();
            String modelId = modelEntry.getKey();
            ModelState<T> modelState = modelEntry.getValue();
            Instant now = clock.instant();
            if (modelState.getLastUsedTime().plus(modelTtl).isBefore(now)) {
                models.remove(modelId);
            }
            if (modelState.getLastCheckpointTime().plus(checkpointInterval).isBefore(now)) {
                checkpointDao.putModelCheckpoint(modelId, toCheckpoint.apply(modelState.getModel()), ActionListener.wrap(r -> {
                    modelState.setLastCheckpointTime(now);
                    maintenanceForIterator(models, toCheckpoint, iter, listener);
                }, e -> {
                    logger.warn("Failed to finish maintenance for model id " + modelId, e);
                    maintenanceForIterator(models, toCheckpoint, iter, listener);
                }));
            } else {
                maintenanceForIterator(models, toCheckpoint, iter, listener);
            }
        } else {
            listener.onResponse(null);
        }
    }

    /**
     * Returns computed anomaly results for preview data points.
     *
     * @param dataPoints features of preview data points
     * @return thresholding results of preview data points
     * @throws IllegalArgumentException when preview data points are not valid
     */
    public List<ThresholdingResult> getPreviewResults(double[][] dataPoints) {
        if (dataPoints.length < minPreviewSize) {
            throw new IllegalArgumentException("Insufficient data for preview results. Minimum required: " + minPreviewSize);
        }
        // Train RCF models and collect non-zero scores
        int rcfNumFeatures = dataPoints[0].length;
        RandomCutForest forest = RandomCutForest
            .builder()
            .randomSeed(0L)
            .dimensions(rcfNumFeatures)
            .sampleSize(rcfNumSamplesInTree)
            .numberOfTrees(rcfNumTrees)
            .lambda(rcfTimeDecay)
            .outputAfter(rcfNumSamplesInTree)
            .parallelExecutionEnabled(false)
            .build();
        double[] rcfScores = Arrays.stream(dataPoints).mapToDouble(point -> {
            double score = forest.getAnomalyScore(point);
            forest.update(point);
            return score;
        }).filter(score -> score > 0.).toArray();
        // Train thresholding model
        ThresholdingModel threshold = new HybridThresholdingModel(
            thresholdMinPvalue,
            thresholdMaxRankError,
            thresholdMaxScore,
            thresholdNumLogNormalQuantiles,
            thresholdDownsamples,
            thresholdMaxSamples
        );
        threshold.train(rcfScores);

        // Get results from trained models
        return Arrays.stream(dataPoints).map(point -> {
            double rcfScore = forest.getAnomalyScore(point);
            forest.update(point);
            ThresholdingResult result = new ThresholdingResult(threshold.grade(rcfScore), threshold.confidence());
            threshold.update(rcfScore);
            return result;
        }).collect(Collectors.toList());
    }

    /**
     * Computes the probabilities of non-coldstart points in the current forest.
     */
    private double computeRcfConfidence(RandomCutForest forest) {
        long total = forest.getTotalUpdates();
        double lambda = forest.getLambda();
        double totalExponent = total * lambda;
        if (totalExponent >= FULL_CONFIDENCE_EXPONENT) {
            return 1.;
        } else {
            double eTotal = Math.exp(totalExponent);
            double confidence = (eTotal - Math.exp(lambda * Math.min(total, forest.getSampleSize()))) / (eTotal - 1);
            return Math.max(0, confidence); // Replaces -0 wth 0 for cosmetic purpose.
        }
    }
}
