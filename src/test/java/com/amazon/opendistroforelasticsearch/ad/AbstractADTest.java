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

import java.util.ArrayList;
import java.util.List;
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
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportService;

import test.com.amazon.opendistroforelasticsearch.ad.util.FakeNode;

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
        threadPool = new TestThreadPool(name);
    }

    protected static void tearDownThreadPool() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
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
        Exception e = expectThrows(exceptionType, () -> listener.actionGet());
        assertThat(e.getMessage(), containsString(msg));
    }
}
