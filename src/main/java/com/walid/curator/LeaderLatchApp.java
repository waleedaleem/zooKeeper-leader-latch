package com.walid.curator;

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class LeaderLatchApp {

    private static final int CLIENT_QTY = 10;
    private static final String PATH = "/LeaderLatchApp";
    private static final String ZK_CONN_STRING = "127.0.0.1:2181";
    private static final List<CuratorFramework> CLIENTS = Lists.newArrayList();
    private static final List<LeaderLatch> LATCHES = Lists.newArrayList();

    public static void main(String[] args) {
        try {
            for (int i = 0; i < CLIENT_QTY; ++i) {
                CuratorFramework client = CuratorFrameworkFactory.newClient(
                        ZK_CONN_STRING, new ExponentialBackoffRetry(1000, 3));
                CLIENTS.add(client);
                LeaderLatch latch = new LeaderLatch(client, PATH, "Client #" + i);
                LATCHES.add(latch);
                client.start();
                // clean up PATH (if already exists)
                if (i == 0) client.delete().guaranteed().deletingChildrenIfNeeded().forPath(PATH);
                latch.start();
            }

            // allow latches to create their latch nodes
            Thread.sleep(20000);

            printLatchNodes(CLIENTS);

            LeaderLatch currentLeader = null;
            for (int i = 0; i < CLIENT_QTY; ++i) {
                LeaderLatch latch = LATCHES.get(i);
                if (latch.hasLeadership()) {
                    currentLeader = latch;
                    System.out.println("current leader is " + currentLeader.getId());
                }
            }

            System.out.println("release the leader " + currentLeader.getId());
            currentLeader.close();
            LATCHES.get(0).await(2, TimeUnit.SECONDS);
            System.out.println("the new leader is " + LATCHES.get(0).getLeader().getId());
            System.out.println("Participants: " + LATCHES.get(0).getParticipants());
            System.out.println("Press enter/return to quit\n");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeAllQuietly();
        }
    }

    private static void printLatchNodes(List<CuratorFramework> clients) throws Exception {
        clients.get(0).getChildren().forPath(PATH).forEach(s -> {
            try {
                System.out.println(s + " contains [" + new String(clients.get(0).getData().forPath(PATH + "/" + s)) + "]");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private static void closeAllQuietly() {
        System.out.println("Shutting down...");
        LATCHES.stream()
                .filter(l -> l.getState().equals(LeaderLatch.State.STARTED))
                .forEach(CloseableUtils::closeQuietly);
        CLIENTS.stream()
                .forEach(CloseableUtils::closeQuietly);
    }
}
