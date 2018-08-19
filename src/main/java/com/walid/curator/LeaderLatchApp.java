package com.walid.curator;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LeaderLatchApp implements CuratorListener {

    static final String PATH = "/LeaderLatchClient";
    private static final Logger logger = LoggerFactory.getLogger(LeaderLatchApp.class);
    private static final String ZK_CONN_STRING = "127.0.0.1:2181";
    private static final int NUMBER_OF_CLIENTS = 10;
    // One shared curator framework (thread safe already)
    private static final CuratorFramework CURATOR_FRAMEWORK = CuratorFrameworkFactory.newClient(
            ZK_CONN_STRING, new ExponentialBackoffRetry(1000, 3));
    private static final ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_CLIENTS,
            new ThreadFactoryBuilder().setNameFormat("LeaderLatchClient-%d").build());

    public static void main(String[] args) throws Exception {
        CURATOR_FRAMEWORK.getCuratorListenable().addListener(new LeaderLatchApp());
        CURATOR_FRAMEWORK.start();
        resetZKPath();
        printLatchNodes();
        IntStream.range(0, NUMBER_OF_CLIENTS)
                .mapToObj(i -> "Client #" + i)
                .forEach(id -> executorService.submit(new LeaderLatchClient(CURATOR_FRAMEWORK, id)));
    }

    private static void resetZKPath() throws Exception {
        // clean up PATH (if already exists)
        if (CURATOR_FRAMEWORK.checkExists().forPath(PATH) != null) {
            CURATOR_FRAMEWORK.delete().deletingChildrenIfNeeded().forPath(PATH);
        }
        CURATOR_FRAMEWORK.create().forPath(PATH);
    }

    private static void printLatchNodes() throws Exception {
        final List<String> latches = CURATOR_FRAMEWORK.getChildren().watched().forPath(PATH);
        if (latches.isEmpty()) {
            logger.debug("{} has no children", PATH);
        } else {
            logger.debug("{} children are: \n{}", PATH,
                    latches.stream()
                            .map(l -> {
                                try {
                                    return String.format("\t%s contains [%s]", l, new String(CURATOR_FRAMEWORK.getData().forPath(PATH + "/" + l)));
                                } catch (Exception ex) {
                                    logger.error("Error:", ex);
                                    return null;
                                }
                            })
                            .collect(Collectors.joining("\n"))
            );
        }
    }

    @Override
    public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
        logger.debug("event = [{}]", event);
        String eventPath = event.getPath();
        WatchedEvent watchedEvent = event.getWatchedEvent();
        if (eventPath == null || watchedEvent == null) {
            return;
        }

        if (PATH.equals(eventPath) && watchedEvent.getType().equals(Watcher.Event.EventType.NodeChildrenChanged)) {
            printLatchNodes();
        }
    }
}
