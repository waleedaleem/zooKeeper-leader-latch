package com.walid.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;

import java.util.concurrent.Callable;

import static com.walid.curator.LeaderLatchApp.PATH;

public class LeaderLatchClient implements Callable<Void> {

    private final String clientId;
    private final CuratorFramework curator;

    LeaderLatchClient(CuratorFramework curator, String id) {
        this.curator = curator;
        clientId = id;
    }

    @Override
    public Void call() throws Exception {
        LeaderLatch latch = new LeaderLatch(curator, PATH, clientId);
        latch.start();
        System.out.println(Thread.currentThread().getName() + ": Latch [" + latch.getId() + "] created.");
        // allow latches to create their latch nodes
        Thread.sleep(1000);
        if (latch.hasLeadership()) {
            System.out.println(Thread.currentThread().getName() + ": I am the leader. My ID = [" + latch.getId() + "]");
        }
        return null;
    }
}