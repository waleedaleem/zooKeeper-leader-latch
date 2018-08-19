package com.walid.curator;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

import static com.walid.curator.LeaderLatchApp.PATH;

public class LeaderLatchClient implements Callable<Void>, LeaderLatchListener {

    private static final Logger logger = LoggerFactory.getLogger(LeaderLatchClient.class);
    private final String clientId;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("Scheduled Leader Monitor").build());
    private final ExecutorService latchListenerExecutor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("Latch Listener").build());
    private final LeaderLatch latch;

    LeaderLatchClient(CuratorFramework curator, String id) {
        clientId = id;
        latch = new LeaderLatch(curator, PATH, clientId);
        latch.addListener(this, latchListenerExecutor);
    }

    @Override
    public Void call() throws Exception {
        latch.start();
        if ("Client #0".equals(clientId)) {
            monitorLeader();
        }
        logger.debug("Latch [{}] created", latch.getId());
        // allow latches to create their latch nodes
        Thread.sleep(1000);
        if (latch.hasLeadership()) {
            logger.info("I am the leader. My ID is [{}]", latch.getId());
        }
        return null;
    }

    /**
     * Reports the leader periodically
     */
    private void monitorLeader() {
        scheduler.scheduleAtFixedRate(new LeaderMonitor(), 1000, 500, TimeUnit.MILLISECONDS);
    }

    @Override
    public void isLeader() {
        checkLeader(true);
    }

    @Override
    public void notLeader() {
        checkLeader(false);
    }

    private void checkLeader(boolean isLeader) {
        try {
            if (isLeader) {
                logger.info("I [{}] am elected leader", clientId);
            } else {
                logger.info("I [{}] am overthrown", clientId);
            }
            // getLeader() is synchronous unlike isLeader() which is triggered asynchronously and can have a stale state
            Participant leader = latch.getLeader();
            if (leader != null) {
                String leaderID = latch.getLeader().getId();
                if (isLeader && !leaderID.equals(latch.getId())) {
                    logger.error("{}: isLeader() turned out to be stale and misleading!!!. real leader is {}", clientId, leaderID);
                } else if (!isLeader && leaderID.equals(latch.getId())) {
                    logger.error("{}: notLeader() turned out to be stale and misleading!!!. I am the real leader", clientId);
                }
            } else {
                logger.warn("No elected leader at the moment!!!");
            }
        } catch (Exception e) {
            logger.error("Error retrieving leader Id from ZK", e);
        }
    }

    private class LeaderMonitor implements Runnable {
        String currentLeader;

        @Override
        public void run() {
            try {
                String leader = latch.getLeader() != null ? latch.getLeader().getId() : null;
                if (currentLeader != null && leader == null) {
                    logger.debug("No elected leader at the moment!!!");
                } else if (currentLeader == null && leader != null
                        || leader != null && !leader.equals(currentLeader)) {
                    logger.debug("Current leader is {}", leader);
                }
                currentLeader = leader;
            } catch (Exception e) {
                logger.error("Error retrieving leader Id from ZK", e);
            }
        }
    }
}