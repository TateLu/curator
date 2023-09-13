package demos;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class SessionTimeoutSimulation {
    private static final String ZOOKEEPER_CONNECTION_STRING = "localhost:2181";
    private static final String ZOOKEEPER_PATH = "/servers/ephemeralNode";
    private static final int SESSION_TIMEOUT = 5000; // 5 seconds

    public static void main(String[] args) {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(ZOOKEEPER_CONNECTION_STRING,
                new ExponentialBackoffRetry(1000, 3));
        curatorFramework.start();
        //15000ms
        System.out.println("session timeout: "+curatorFramework.getZookeeperClient().getConnectionTimeoutMs());
        try {
            // Create an ephemeral node
            curatorFramework.create().withMode(CreateMode.EPHEMERAL).forPath(ZOOKEEPER_PATH);
            System.out.println("create ");
            // Create a PathChildrenCache to watch the node
            PathChildrenCache pathChildrenCache = new PathChildrenCache(curatorFramework, ZOOKEEPER_PATH, true);
            pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                    if (event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                        System.out.println("Ephemeral node deleted: Session expired");
                    }
                }
            });
            pathChildrenCache.start(PathChildrenCache.StartMode.NORMAL);

            // Wait for the session to expire
            Thread.sleep(5000); // Wait for 10 seconds to ensure session expiration
            System.out.println("quit ");
            // Close the PathChildrenCache and CuratorFramework instances
            pathChildrenCache.close();
            curatorFramework.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
