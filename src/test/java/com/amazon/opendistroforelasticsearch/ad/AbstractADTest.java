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

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.util.StackLocatorUtil;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportService;

import test.com.amazon.opendistroforelasticsearch.ad.util.FakeNode;

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorInternalState;

public class AbstractADTest extends ESTestCase {

    protected static final Logger LOG = (Logger) LogManager.getLogger(AbstractADTest.class);

    // transport test node
    protected int nodesCount;
    protected FakeNode[] testNodes;

    /**
     * Log4j appender that uses a list to store log messages
     *
     */
    protected class TestAppender extends AbstractAppender {
        protected TestAppender(String name) {
            super(name, null, PatternLayout.createDefaultLayout(), true);
        }

        public List<String> messages = new ArrayList<String>();

        public boolean containsMessage(String msg, boolean formatString) {
            Pattern p = null;
            if (formatString) {
                String regex = convertToRegex(msg);
                p = Pattern.compile(regex);
            }
            for (String logMsg : messages) {
                LOG.info(logMsg);
                if (p != null) {
                    Matcher m = p.matcher(logMsg);
                    if (m.matches()) {
                        return true;
                    }
                } else if (logMsg.contains(msg)) {
                    return true;
                }
            }
            return false;
        }

        public boolean containsMessage(String msg) {
            return containsMessage(msg, false);
        }

        public int countMessage(String msg, boolean formatString) {
            Pattern p = null;
            if (formatString) {
                String regex = convertToRegex(msg);
                p = Pattern.compile(regex);
            }
            int count = 0;
            for (String logMsg : messages) {
                LOG.info(logMsg);
                if (p != null) {
                    Matcher m = p.matcher(logMsg);
                    if (m.matches()) {
                        count++;
                    }
                } else if (logMsg.contains(msg)) {
                    count++;
                }
            }
            return count;
        }

        public int countMessage(String msg) {
            return countMessage(msg, false);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
        }

        /**
         * Convert a string with format like "Cannot save %s due to write block."
         *  to a regex with .* like "Cannot save .* due to write block."
         * @return converted regex
         */
        private String convertToRegex(String formattedStr) {
            int percentIndex = formattedStr.indexOf("%");
            return formattedStr.substring(0, percentIndex) + ".*" + formattedStr.substring(percentIndex + 2);
        }

    }

    protected static ThreadPool threadPool;

    protected TestAppender testAppender;

    Logger logger;

    /**
     * Set up test with junit that a warning was logged with log4j
     */
    protected void setUpLog4jForJUnit(Class<?> cls) {
        String loggerName = toLoggerName(callerClass(cls));
        logger = (Logger) LogManager.getLogger(loggerName);
        testAppender = new TestAppender(loggerName);
        testAppender.start();
        logger.addAppender(testAppender);
        logger.setLevel(Level.DEBUG);
    }

    private static String toLoggerName(final Class<?> cls) {
        String canonicalName = cls.getCanonicalName();
        return canonicalName != null ? canonicalName : cls.getName();
    }

    private static Class<?> callerClass(final Class<?> clazz) {
        if (clazz != null) {
            return clazz;
        }
        final Class<?> candidate = StackLocatorUtil.getCallerClass(3);
        if (candidate == null) {
            throw new UnsupportedOperationException("No class provided, and an appropriate one cannot be found.");
        }
        return candidate;
    }

    /**
     * remove the appender
     */
    protected void tearDownLog4jForJUnit() {
        logger.removeAppender(testAppender);
        testAppender.stop();
    }

    protected static void setUpThreadPool(String name) {
        threadPool = new TestThreadPool(
            name,
            new FixedExecutorBuilder(
                Settings.EMPTY,
                AnomalyDetectorPlugin.AD_THREAD_POOL_NAME,
                1,
                1000,
                "opendistro.ad." + AnomalyDetectorPlugin.AD_THREAD_POOL_NAME
            )
        );
    }

    protected static void tearDownThreadPool() {
        LOG.info("tear down threadPool");
        assertTrue(ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS));
        threadPool = null;
    }

    public void setupTestNodes(Settings settings, TransportInterceptor transportInterceptor) {
        nodesCount = randomIntBetween(2, 10);
        testNodes = new FakeNode[nodesCount];
        for (int i = 0; i < testNodes.length; i++) {
            testNodes[i] = new FakeNode("node" + i, threadPool, settings, transportInterceptor);
        }
        FakeNode.connectNodes(testNodes);
    }

    public void setupTestNodes(Settings settings) {
        setupTestNodes(settings, TransportService.NOOP_TRANSPORT_INTERCEPTOR);
    }

    public void tearDownTestNodes() {
        if (testNodes == null) {
            return;
        }
        for (FakeNode testNode : testNodes) {
            testNode.close();
        }
        testNodes = null;
    }

    public void assertException(
        PlainActionFuture<? extends ActionResponse> listener,
        Class<? extends Exception> exceptionType,
        String msg
    ) {
        Exception e = expectThrows(exceptionType, () -> listener.actionGet(20_000));
        assertThat("actual message: " + e.getMessage(), e.getMessage(), containsString(msg));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        List<NamedXContentRegistry.Entry> entries = searchModule.getNamedXContents();
        entries
            .addAll(
                Arrays
                    .asList(
                        AnomalyDetector.XCONTENT_REGISTRY,
                        AnomalyResult.XCONTENT_REGISTRY,
                        DetectorInternalState.XCONTENT_REGISTRY,
                        AnomalyDetectorJob.XCONTENT_REGISTRY
                    )
            );
        return new NamedXContentRegistry(entries);
    }

    protected RestRequest createRestRequest(Method method) {
        return RestRequest.request(xContentRegistry(), new HttpRequest() {

            @Override
            public Method method() {
                return method;
            }

            @Override
            public String uri() {
                return "/";
            }

            @Override
            public BytesReference content() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Map<String, List<String>> getHeaders() {
                return new HashMap<>();
            }

            @Override
            public List<String> strictCookies() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public HttpVersion protocolVersion() {
                return HttpRequest.HttpVersion.HTTP_1_1;
            }

            @Override
            public HttpRequest removeHeader(String header) {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public HttpResponse createResponse(RestStatus status, BytesReference content) {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Exception getInboundException() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public void release() {
                // TODO Auto-generated method stub

            }

            @Override
            public HttpRequest releaseAndCopy() {
                // TODO Auto-generated method stub
                return null;
            }

        }, null);
    }

    protected boolean areEqualWithArrayValue(Map<String, double[]> first, Map<String, double[]> second) {
        if (first.size() != second.size()) {
            return false;
        }

        return first.entrySet().stream().allMatch(e -> Arrays.equals(e.getValue(), second.get(e.getKey())));
    }

    protected IndexMetadata indexMeta(String name, long creationDate, String... aliases) {
        IndexMetadata.Builder builder = IndexMetadata
            .builder(name)
            .settings(
                Settings
                    .builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
                    .put("index.version.created", Version.CURRENT.id)
            );
        builder.creationDate(creationDate);
        for (String alias : aliases) {
            builder.putAlias(AliasMetadata.builder(alias).build());
        }
        return builder.build();
    }

    protected void setUpADThreadPool(ThreadPool mockThreadPool) {
        ExecutorService executorService = mock(ExecutorService.class);

        when(mockThreadPool.executor(AnomalyDetectorPlugin.AD_THREAD_POOL_NAME)).thenReturn(executorService);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
    }
}
