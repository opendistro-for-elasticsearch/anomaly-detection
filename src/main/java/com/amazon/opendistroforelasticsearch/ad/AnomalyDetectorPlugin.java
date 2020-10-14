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

package com.amazon.opendistroforelasticsearch.ad;

import static java.util.Collections.unmodifiableList;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.monitor.jvm.JvmService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.cluster.ADClusterEventListener;
import com.amazon.opendistroforelasticsearch.ad.cluster.HashRing;
import com.amazon.opendistroforelasticsearch.ad.cluster.MasterEventListener;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.dataprocessor.IntegerSensitiveSingleFeatureLinearUniformInterpolator;
import com.amazon.opendistroforelasticsearch.ad.dataprocessor.Interpolator;
import com.amazon.opendistroforelasticsearch.ad.dataprocessor.LinearUniformInterpolator;
import com.amazon.opendistroforelasticsearch.ad.dataprocessor.SingleFeatureLinearUniformInterpolator;
import com.amazon.opendistroforelasticsearch.ad.feature.FeatureManager;
import com.amazon.opendistroforelasticsearch.ad.feature.SearchFeatureDao;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.ml.CheckpointDao;
import com.amazon.opendistroforelasticsearch.ad.ml.HybridThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorInternalState;
import com.amazon.opendistroforelasticsearch.ad.rest.RestAnomalyDetectorJobAction;
import com.amazon.opendistroforelasticsearch.ad.rest.RestDeleteAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.rest.RestExecuteAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.rest.RestGetAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.rest.RestIndexAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.rest.RestSearchAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.rest.RestSearchAnomalyResultAction;
import com.amazon.opendistroforelasticsearch.ad.rest.RestStatsAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.settings.EnabledSetting;
import com.amazon.opendistroforelasticsearch.ad.stats.ADStat;
import com.amazon.opendistroforelasticsearch.ad.stats.ADStats;
import com.amazon.opendistroforelasticsearch.ad.stats.StatNames;
import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.CounterSupplier;
import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.IndexStatusSupplier;
import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.ModelsOnNodeSupplier;
import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.SettableSupplier;
import com.amazon.opendistroforelasticsearch.ad.transport.ADResultBulkAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADResultBulkTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADStatsNodesAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADStatsNodesTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyDetectorJobAction;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyDetectorJobTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyResultAction;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyResultTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.CronAction;
import com.amazon.opendistroforelasticsearch.ad.transport.CronTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.DeleteAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.transport.DeleteAnomalyDetectorTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.DeleteModelAction;
import com.amazon.opendistroforelasticsearch.ad.transport.DeleteModelTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.GetAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.transport.GetAnomalyDetectorTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.IndexAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.transport.IndexAnomalyDetectorTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ProfileAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ProfileTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.RCFPollingAction;
import com.amazon.opendistroforelasticsearch.ad.transport.RCFPollingTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.RCFResultAction;
import com.amazon.opendistroforelasticsearch.ad.transport.RCFResultTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.SearchAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.transport.SearchAnomalyDetectorTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.SearchAnomalyResultAction;
import com.amazon.opendistroforelasticsearch.ad.transport.SearchAnomalyResultTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.StatsAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.transport.StatsAnomalyDetectorTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.StopDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.transport.StopDetectorTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ThresholdResultAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ThresholdResultTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.TransportStateManager;
import com.amazon.opendistroforelasticsearch.ad.transport.handler.AnomalyIndexHandler;
import com.amazon.opendistroforelasticsearch.ad.transport.handler.DetectionStateHandler;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;
import com.amazon.opendistroforelasticsearch.ad.util.IndexUtils;
import com.amazon.opendistroforelasticsearch.ad.util.Throttler;
import com.amazon.opendistroforelasticsearch.ad.util.ThrowingConsumerWrapper;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobSchedulerExtension;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParser;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner;
import com.amazon.randomcutforest.serialize.RandomCutForestSerDe;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;

/**
 * Entry point of AD plugin.
 */
public class AnomalyDetectorPlugin extends Plugin implements ActionPlugin, ScriptPlugin, JobSchedulerExtension, SystemIndexPlugin {

    public static final String AD_BASE_URI = "/_opendistro/_anomaly_detection";
    public static final String AD_BASE_DETECTORS_URI = AD_BASE_URI + "/detectors";
    public static final String AD_THREAD_POOL_NAME = "ad-threadpool";
    public static final String AD_JOB_TYPE = "opendistro_anomaly_detector";
    private static Gson gson;
    private AnomalyDetectionIndices anomalyDetectionIndices;
    private AnomalyDetectorRunner anomalyDetectorRunner;
    private Client client;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private ADStats adStats;
    private NamedXContentRegistry xContentRegistry;
    private ClientUtil clientUtil;
    private DiscoveryNodeFilterer nodeFilter;
    private IndexUtils indexUtils;
    private DetectionStateHandler detectorStateHandler;

    static {
        SpecialPermission.check();
        // gson intialization requires "java.lang.RuntimePermission" "accessDeclaredMembers" to
        // initialize ConstructorConstructor
        AccessController.doPrivileged((PrivilegedAction<Void>) AnomalyDetectorPlugin::initGson);
    }

    public AnomalyDetectorPlugin() {}

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {

        AnomalyIndexHandler<AnomalyResult> anomalyResultHandler;
        anomalyResultHandler = new AnomalyIndexHandler<AnomalyResult>(
            client,
            settings,
            threadPool,
            CommonName.ANOMALY_RESULT_INDEX_ALIAS,
            ThrowingConsumerWrapper.throwingConsumerWrapper(anomalyDetectionIndices::initAnomalyResultIndexDirectly),
            anomalyDetectionIndices::doesAnomalyResultIndexExist,
            false,
            this.clientUtil,
            this.indexUtils,
            clusterService
        );

        AnomalyDetectorJobRunner jobRunner = AnomalyDetectorJobRunner.getJobRunnerInstance();
        jobRunner.setClient(client);
        jobRunner.setClientUtil(clientUtil);
        jobRunner.setThreadPool(threadPool);
        jobRunner.setAnomalyResultHandler(anomalyResultHandler);
        jobRunner.setDetectionStateHandler(detectorStateHandler);
        jobRunner.setSettings(settings);

        AnomalyDetectorProfileRunner profileRunner = new AnomalyDetectorProfileRunner(
            client,
            this.xContentRegistry,
            this.nodeFilter,
            AnomalyDetectorSettings.NUM_MIN_SAMPLES
        );
        RestGetAnomalyDetectorAction restGetAnomalyDetectorAction = new RestGetAnomalyDetectorAction();
        RestIndexAnomalyDetectorAction restIndexAnomalyDetectorAction = new RestIndexAnomalyDetectorAction(
            settings,
            clusterService,
            anomalyDetectionIndices
        );
        RestSearchAnomalyDetectorAction searchAnomalyDetectorAction = new RestSearchAnomalyDetectorAction();
        RestSearchAnomalyResultAction searchAnomalyResultAction = new RestSearchAnomalyResultAction();
        RestDeleteAnomalyDetectorAction deleteAnomalyDetectorAction = new RestDeleteAnomalyDetectorAction();
        RestExecuteAnomalyDetectorAction executeAnomalyDetectorAction = new RestExecuteAnomalyDetectorAction(
            settings,
            clusterService,
            anomalyDetectorRunner
        );
        RestStatsAnomalyDetectorAction statsAnomalyDetectorAction = new RestStatsAnomalyDetectorAction(adStats, this.nodeFilter);
        RestAnomalyDetectorJobAction anomalyDetectorJobAction = new RestAnomalyDetectorJobAction(settings, clusterService);

        return ImmutableList
            .of(
                restGetAnomalyDetectorAction,
                restIndexAnomalyDetectorAction,
                searchAnomalyDetectorAction,
                searchAnomalyResultAction,
                deleteAnomalyDetectorAction,
                executeAnomalyDetectorAction,
                anomalyDetectorJobAction,
                statsAnomalyDetectorAction
            );
    }

    private static Void initGson() {
        gson = new Gson();
        return null;
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        EnabledSetting.getInstance().init(clusterService);
        this.client = client;
        this.threadPool = threadPool;
        Settings settings = environment.settings();
        Clock clock = Clock.systemUTC();
        Throttler throttler = new Throttler(clock);
        this.clientUtil = new ClientUtil(settings, client, throttler, threadPool);
        this.indexUtils = new IndexUtils(client, clientUtil, clusterService, indexNameExpressionResolver);
        anomalyDetectionIndices = new AnomalyDetectionIndices(client, clusterService, threadPool, settings);
        this.clusterService = clusterService;
        this.xContentRegistry = xContentRegistry;

        SingleFeatureLinearUniformInterpolator singleFeatureLinearUniformInterpolator =
            new IntegerSensitiveSingleFeatureLinearUniformInterpolator();
        Interpolator interpolator = new LinearUniformInterpolator(singleFeatureLinearUniformInterpolator);
        SearchFeatureDao searchFeatureDao = new SearchFeatureDao(client, xContentRegistry, interpolator, clientUtil);

        JvmService jvmService = new JvmService(environment.settings());
        RandomCutForestSerDe rcfSerde = new RandomCutForestSerDe();
        CheckpointDao checkpoint = new CheckpointDao(client, clientUtil, CommonName.CHECKPOINT_INDEX_NAME);

        this.nodeFilter = new DiscoveryNodeFilterer(this.clusterService);

        ModelManager modelManager = new ModelManager(
            nodeFilter,
            jvmService,
            rcfSerde,
            checkpoint,
            gson,
            clock,
            AnomalyDetectorSettings.DESIRED_MODEL_SIZE_PERCENTAGE,
            AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE.get(settings),
            AnomalyDetectorSettings.NUM_TREES,
            AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE,
            AnomalyDetectorSettings.TIME_DECAY,
            AnomalyDetectorSettings.NUM_MIN_SAMPLES,
            AnomalyDetectorSettings.THRESHOLD_MIN_PVALUE,
            AnomalyDetectorSettings.THRESHOLD_MAX_RANK_ERROR,
            AnomalyDetectorSettings.THRESHOLD_MAX_SCORE,
            AnomalyDetectorSettings.THRESHOLD_NUM_LOGNORMAL_QUANTILES,
            AnomalyDetectorSettings.THRESHOLD_DOWNSAMPLES,
            AnomalyDetectorSettings.THRESHOLD_MAX_SAMPLES,
            HybridThresholdingModel.class,
            AnomalyDetectorSettings.MIN_PREVIEW_SIZE,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            clusterService
        );

        HashRing hashRing = new HashRing(nodeFilter, clock, settings);
        TransportStateManager stateManager = new TransportStateManager(
            client,
            xContentRegistry,
            modelManager,
            settings,
            clientUtil,
            clock,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE
        );
        FeatureManager featureManager = new FeatureManager(
            searchFeatureDao,
            interpolator,
            clock,
            AnomalyDetectorSettings.MAX_TRAIN_SAMPLE,
            AnomalyDetectorSettings.MAX_SAMPLE_STRIDE,
            AnomalyDetectorSettings.TRAIN_SAMPLE_TIME_RANGE_IN_HOURS,
            AnomalyDetectorSettings.MIN_TRAIN_SAMPLES,
            AnomalyDetectorSettings.MAX_SHINGLE_PROPORTION_MISSING,
            AnomalyDetectorSettings.MAX_IMPUTATION_NEIGHBOR_DISTANCE,
            AnomalyDetectorSettings.PREVIEW_SAMPLE_RATE,
            AnomalyDetectorSettings.MAX_PREVIEW_SAMPLES,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            threadPool,
            AD_THREAD_POOL_NAME
        );
        anomalyDetectorRunner = new AnomalyDetectorRunner(modelManager, featureManager, AnomalyDetectorSettings.MAX_PREVIEW_RESULTS);

        Map<String, ADStat<?>> stats = ImmutableMap
            .<String, ADStat<?>>builder()
            .put(StatNames.AD_EXECUTE_REQUEST_COUNT.getName(), new ADStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_EXECUTE_FAIL_COUNT.getName(), new ADStat<>(false, new CounterSupplier()))
            .put(StatNames.MODEL_INFORMATION.getName(), new ADStat<>(false, new ModelsOnNodeSupplier(modelManager)))
            .put(
                StatNames.ANOMALY_DETECTORS_INDEX_STATUS.getName(),
                new ADStat<>(true, new IndexStatusSupplier(indexUtils, AnomalyDetector.ANOMALY_DETECTORS_INDEX))
            )
            .put(
                StatNames.ANOMALY_RESULTS_INDEX_STATUS.getName(),
                new ADStat<>(true, new IndexStatusSupplier(indexUtils, CommonName.ANOMALY_RESULT_INDEX_ALIAS))
            )
            .put(
                StatNames.MODELS_CHECKPOINT_INDEX_STATUS.getName(),
                new ADStat<>(true, new IndexStatusSupplier(indexUtils, CommonName.CHECKPOINT_INDEX_NAME))
            )
            .put(StatNames.DETECTOR_COUNT.getName(), new ADStat<>(true, new SettableSupplier()))
            .build();

        adStats = new ADStats(indexUtils, modelManager, stats);
        ADCircuitBreakerService adCircuitBreakerService = new ADCircuitBreakerService(jvmService).init();
        this.detectorStateHandler = new DetectionStateHandler(
            client,
            settings,
            threadPool,
            ThrowingConsumerWrapper.throwingConsumerWrapper(anomalyDetectionIndices::initDetectorStateIndex),
            anomalyDetectionIndices::doesDetectorStateIndexExist,
            this.clientUtil,
            this.indexUtils,
            clusterService,
            xContentRegistry,
            stateManager
        );

        return ImmutableList
            .of(
                anomalyDetectionIndices,
                anomalyDetectorRunner,
                searchFeatureDao,
                singleFeatureLinearUniformInterpolator,
                interpolator,
                gson,
                jvmService,
                hashRing,
                featureManager,
                modelManager,
                clock,
                stateManager,
                new ADClusterEventListener(clusterService, hashRing, modelManager, nodeFilter),
                adCircuitBreakerService,
                adStats,
                new MasterEventListener(clusterService, threadPool, client, clock, clientUtil, nodeFilter),
                nodeFilter,
                detectorStateHandler
            );
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return Collections
            .singletonList(
                new FixedExecutorBuilder(
                    settings,
                    AD_THREAD_POOL_NAME,
                    Math.max(1, EsExecutors.allocatedProcessors(settings) / 4),
                    AnomalyDetectorSettings.AD_THEAD_POOL_QUEUE_SIZE,
                    "opendistro.ad." + AD_THREAD_POOL_NAME
                )
            );
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> enabledSetting = EnabledSetting.getInstance().getSettings();

        List<Setting<?>> systemSetting = ImmutableList
            .of(
                AnomalyDetectorSettings.MAX_SINGLE_ENTITY_ANOMALY_DETECTORS,
                AnomalyDetectorSettings.MAX_MULTI_ENTITY_ANOMALY_DETECTORS,
                AnomalyDetectorSettings.MAX_ANOMALY_FEATURES,
                AnomalyDetectorSettings.REQUEST_TIMEOUT,
                AnomalyDetectorSettings.DETECTION_INTERVAL,
                AnomalyDetectorSettings.DETECTION_WINDOW_DELAY,
                AnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD,
                AnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS,
                AnomalyDetectorSettings.AD_RESULT_ROLLOVER_PERIOD,
                AnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
                AnomalyDetectorSettings.COOLDOWN_MINUTES,
                AnomalyDetectorSettings.BACKOFF_MINUTES,
                AnomalyDetectorSettings.BACKOFF_INITIAL_DELAY,
                AnomalyDetectorSettings.MAX_RETRY_FOR_BACKOFF,
                AnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD,
                AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE,
                AnomalyDetectorSettings.INDEX_PRESSURE_SOFT_LIMIT
            );
        return unmodifiableList(Stream.concat(enabledSetting.stream(), systemSetting.stream()).collect(Collectors.toList()));
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return ImmutableList
            .of(
                AnomalyDetector.XCONTENT_REGISTRY,
                AnomalyResult.XCONTENT_REGISTRY,
                DetectorInternalState.XCONTENT_REGISTRY,
                AnomalyDetectorJob.XCONTENT_REGISTRY
            );
    }

    /*
     * Register action and handler so that transportClient can find proxy for action
     */
    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays
            .asList(
                new ActionHandler<>(DeleteModelAction.INSTANCE, DeleteModelTransportAction.class),
                new ActionHandler<>(StopDetectorAction.INSTANCE, StopDetectorTransportAction.class),
                new ActionHandler<>(RCFResultAction.INSTANCE, RCFResultTransportAction.class),
                new ActionHandler<>(ThresholdResultAction.INSTANCE, ThresholdResultTransportAction.class),
                new ActionHandler<>(AnomalyResultAction.INSTANCE, AnomalyResultTransportAction.class),
                new ActionHandler<>(CronAction.INSTANCE, CronTransportAction.class),
                new ActionHandler<>(ADStatsNodesAction.INSTANCE, ADStatsNodesTransportAction.class),
                new ActionHandler<>(ProfileAction.INSTANCE, ProfileTransportAction.class),
                new ActionHandler<>(RCFPollingAction.INSTANCE, RCFPollingTransportAction.class),
                new ActionHandler<>(SearchAnomalyDetectorAction.INSTANCE, SearchAnomalyDetectorTransportAction.class),
                new ActionHandler<>(SearchAnomalyResultAction.INSTANCE, SearchAnomalyResultTransportAction.class),
                new ActionHandler<>(StatsAnomalyDetectorAction.INSTANCE, StatsAnomalyDetectorTransportAction.class),
                new ActionHandler<>(DeleteAnomalyDetectorAction.INSTANCE, DeleteAnomalyDetectorTransportAction.class),
                new ActionHandler<>(GetAnomalyDetectorAction.INSTANCE, GetAnomalyDetectorTransportAction.class),
                new ActionHandler<>(IndexAnomalyDetectorAction.INSTANCE, IndexAnomalyDetectorTransportAction.class),
                new ActionHandler<>(AnomalyDetectorJobAction.INSTANCE, AnomalyDetectorJobTransportAction.class),
                new ActionHandler<>(ADResultBulkAction.INSTANCE, ADResultBulkTransportAction.class)
            );
    }

    @Override
    public String getJobType() {
        return AD_JOB_TYPE;
    }

    @Override
    public String getJobIndex() {
        return AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX;
    }

    @Override
    public ScheduledJobRunner getJobRunner() {
        return AnomalyDetectorJobRunner.getJobRunnerInstance();
    }

    @Override
    public ScheduledJobParser getJobParser() {
        return (parser, id, jobDocVersion) -> {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            return AnomalyDetectorJob.parse(parser);
        };
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return Collections
            .unmodifiableList(
                Arrays
                    .asList(
                        new SystemIndexDescriptor(AnomalyDetectionIndices.ALL_AD_RESULTS_INDEX_PATTERN, "anomaly result"),
                        new SystemIndexDescriptor(AnomalyDetector.ANOMALY_DETECTORS_INDEX, "detector definition"),
                        new SystemIndexDescriptor(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX, "detector job"),
                        new SystemIndexDescriptor(CommonName.CHECKPOINT_INDEX_NAME, "model checkpoint"),
                        new SystemIndexDescriptor(DetectorInternalState.DETECTOR_STATE_INDEX, "detector information like total rcf updates")
                    )
            );
    }
}
