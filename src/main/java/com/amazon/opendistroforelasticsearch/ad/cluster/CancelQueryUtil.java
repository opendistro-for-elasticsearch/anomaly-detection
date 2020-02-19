package com.amazon.opendistroforelasticsearch.ad.cluster;

import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.Throttler;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.tasks.TaskInfo;

import java.time.Clock;
import java.time.Instant;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Utility class to cancel long running query
 */
public class CancelQueryUtil {
    private static final org.apache.logging.log4j.Logger LOG = LogManager.getLogger(CancelQueryUtil.class);
    private final Throttler throttler;
    private Clock clock;


    public CancelQueryUtil(Throttler throttler, Clock clock) {
        this.throttler = throttler;
        this.clock = clock;
    }

    public void cancelQuery(Client client) {
        // Step 1: get current task
        // list task api
        // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-tasks-list.html
        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setDetailed(true);
        AtomicReference<List<TaskInfo>> taskList = new AtomicReference<>();
        client
                .execute(
                        ListTasksAction.INSTANCE,
                        listTasksRequest,
                        ActionListener
                                .wrap(
                                        response -> {
                                            LOG.info("List all tasks");
                                            taskList.set(response.getTasks());
                                        },
                                        exception -> {
                                            LOG.error("List Tasks failed.", exception);
                                        }
                                )
                );
        List<TaskInfo> tasks = taskList.get();

        // Step 2: go through negative cache to match
        for (TaskInfo task : tasks) {
            if ()
        }


        for (Iterator<Map.Entry<String, Map.Entry<ActionRequest, Instant>>> it = throttler.getNegativeCache().entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, Map.Entry<ActionRequest, Instant>> entry = it.next();
            String queryDescription = getQueryDescription(entry);
            if (tasks)
        }

        // Step 3: kill the matched tasks
        // cancel task api
        // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-cluster-cancel-tasks.html
    }


    private String getQueryDescription(Map.Entry<String, Map.Entry<ActionRequest, Instant>> entry) {
        SearchRequest request = (SearchRequest) entry.getValue().getKey();
        return request.getDescription();
    }

}
