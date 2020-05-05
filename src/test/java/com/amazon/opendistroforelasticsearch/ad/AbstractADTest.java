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

package com.amazon.opendistroforelasticsearch.ad;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.util.StackLocatorUtil;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

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

        public boolean containsMessage(String msg) {
            for (String logMsg : messages) {
                LOG.info(logMsg);
                if (logMsg.contains(msg)) {
                    return true;
                }
            }
            return false;
        }

        public int countMessage(String msg) {
            int count = 0;
            for (String logMsg : messages) {
                LOG.info(logMsg);
                if (logMsg.contains(msg)) {
                    count++;
                }
            }
            return count;
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
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

    public void setupTestNodes(Settings settings) {
        nodesCount = randomIntBetween(2, 10);
        testNodes = new FakeNode[nodesCount];
        for (int i = 0; i < testNodes.length; i++) {
            testNodes[i] = new FakeNode("node" + i, threadPool, settings);
        }
        FakeNode.connectNodes(testNodes);
    }

    public void tearDownTestNodes() {
        for (FakeNode testNode : testNodes) {
            testNode.close();
        }
        testNodes = null;
    }
}
