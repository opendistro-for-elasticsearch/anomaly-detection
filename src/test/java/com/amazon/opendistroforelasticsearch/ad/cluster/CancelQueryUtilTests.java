package com.amazon.opendistroforelasticsearch.ad.cluster;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.util.Throttler;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.junit.After;
import org.junit.Before;


import javax.naming.directory.SearchResult;
import java.io.IOException;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CancelQueryUtilTests extends AbstractADTest {
    private Client client;
    private Throttler throttler;
    private static String NODE_ID = "node_id";

    private enum CancelQueryExecutionMode {
        CANCEL_QUERY_NORMAL,
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        super.setUpLog4jForJUnit(DeleteDetector.class);
        client = mock(Client.class);
        Clock clock = Clock.systemUTC();
        throttler = new Throttler(clock);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        super.tearDownLog4jForJUnit();
    }

    @SuppressWarnings("unchecked")
    public void deleteDetectorResponseTemplate(CancelQueryUtilTests.CancelQueryExecutionMode mode) throws Exception {
        // setup ListTaskResponse
        List<TaskInfo> tasks = ImmutableList.of(
                new TaskInfo(
                        new TaskId("test", 123),
                        "test",
                        "test",
                        "test",
                        new BulkByScrollTask.Status(
                                1,
                                10,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                timeValueMillis(0),
                                0,
                                null,
                                timeValueMillis(0)
                        ),
                        0,
                        0,
                        true,
                        new TaskId("test", 123),
                        Collections.emptyMap())
        );
        ListTasksResponse listTasksResponse = mock(ListTasksResponse.class);
        when(listTasksResponse.getTasks()).thenReturn(tasks);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length >= 3);
            assertTrue(args[2] instanceof ActionListener);

            ActionListener<ListTasksResponse> listener = (ActionListener<ListTasksResponse>) args[2];

            assertTrue(listener != null);
            if (mode == CancelQueryUtilTests.CancelQueryExecutionMode.CANCEL_QUERY_NORMAL) {
                listener.onResponse(listTasksResponse);
            }
            return null;
        }).when(client).execute(eq(ListTasksAction.INSTANCE), any(), any());


        // setup CancelTasksResponse
        CancelTasksResponse cancelTaskResponse = mock(CancelTasksResponse.class);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length >= 3);
            assertTrue(args[2] instanceof ActionListener);

            ActionListener<CancelTasksResponse> listener = (ActionListener<CancelTasksResponse>) args[2];

            assertTrue(listener != null);
            if (mode == CancelQueryUtilTests.CancelQueryExecutionMode.CANCEL_QUERY_NORMAL) {
                listener.onResponse(cancelTaskResponse);
            }
            return null;
        }).when(client).execute(eq(CancelTasksAction.INSTANCE), any(), any());

        CancelQueryUtil cancelQueryUtil = new CancelQueryUtil(throttler);
        cancelQueryUtil.cancelQuery(client);

        // setup negative cache
        AnomalyDetector detector = mock(AnomalyDetector.class);
        SearchSourceBuilder featureQuery = new SearchSourceBuilder();
        IntervalTimeConfiguration detectionInterval = new IntervalTimeConfiguration(1, ChronoUnit.MINUTES);
        when(detector.getTimeField()).thenReturn("testTimeField");
        when(detector.getIndices()).thenReturn(Arrays.asList("testIndices"));
        when(detector.generateFeatureQuery()).thenReturn(featureQuery);
        when(detector.getDetectionInterval()).thenReturn(detectionInterval);

        SearchRequest searchRequest = new SearchRequest(detector.getIndices().toArray(new String[0]));
        throttler.insertFilteredQuery("test detector id", searchRequest);
    }

    public void testNormalCancelQuery() throws Exception {
        deleteDetectorResponseTemplate(CancelQueryExecutionMode.CANCEL_QUERY_NORMAL);
    }
}
