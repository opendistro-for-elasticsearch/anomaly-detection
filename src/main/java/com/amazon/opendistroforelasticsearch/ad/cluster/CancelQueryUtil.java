package com.amazon.opendistroforelasticsearch.ad.cluster;

import com.amazon.opendistroforelasticsearch.ad.util.Throttler;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.tasks.TaskInfo;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Utility class to cancel long running query
 */
public class CancelQueryUtil {
    private final static String CANCEL_REASON = "Cancel long running query for Anomaly Detection";
    private final static long ONE_DAY = TimeUnit.DAYS.toMillis(1);
    private static final org.apache.logging.log4j.Logger LOG = LogManager.getLogger(CancelQueryUtil.class);
    private final Throttler throttler;


    public CancelQueryUtil(Throttler throttler) {
        this.throttler = throttler;
    }

    public void cancelQuery(Client client) {
        // Step 1: get current task
        // list task api
        // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-tasks-list.html
        List<TaskInfo> tasks = getCurrentTasks(client);

        // Step 2: find the matched query, then kill it and delete matched entry from throttler
        // One assumption here: the size of task list is much larger than negative cache
        // since most of the search query should finish fast.
        for (TaskInfo task : tasks) {
            String detectorId = findMatchedQuery(task);
            if (!Strings.isNullOrEmpty(detectorId)) {
                cancelTask(task, detectorId, client);
            }
            System.out.println(detectorId);
            System.out.println(task);
        }
    }


    private List<TaskInfo> getCurrentTasks(Client client) {
        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setDetailed(true);
        AtomicReference<List<TaskInfo>> taskList = new AtomicReference<>();
        client.execute(
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
        return taskList.get();
    }

    private String findMatchedQuery(TaskInfo task) {
        for (Iterator<Map.Entry<String, Map.Entry<ActionRequest, Instant>>> it = throttler.getNegativeCache().entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, Map.Entry<ActionRequest, Instant>> entry = it.next();
            if (throttler.getClock().millis() - entry.getValue().getValue().getEpochSecond() > ONE_DAY) {
                String queryDescription = getQueryDescription(entry);
                if (queryDescription.equals(task.getDescription())) {
                    LOG.info("Found long running query for detector: {}", entry.getKey());
                    return entry.getKey();
                }
            } else {
                LOG.info("No query is running longer than 1 day");
            }
        }
        return null;
    }

    private void cancelTask(TaskInfo task, String detectorId, Client client) {
        // 1) use task management API to cancel query
        // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-cluster-cancel-tasks.html
        // 2) remove matched entry from negative cache
        CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
        cancelTasksRequest.setReason(CANCEL_REASON);
        cancelTasksRequest.setTaskId(task.getTaskId());
        client.execute(
                CancelTasksAction.INSTANCE,
                cancelTasksRequest,
                ActionListener.wrap(
                        response -> {
                            LOG.info("Cancel task: {}", task.getTaskId());
                            throttler.clearFilteredQuery(detectorId);
                            LOG.info("Remove negative cache for detector: {}", detectorId);
                        },
                        exception -> {
                            LOG.error("Failed to cancel task: {}", task.getTaskId());
                        }
                )
        );
    }


    private String getQueryDescription(Map.Entry<String, Map.Entry<ActionRequest, Instant>> entry) {
        SearchRequest request = (SearchRequest) entry.getValue().getKey();
        return request.getDescription();
    }
}
