package demos.events;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.TimeUnit;

public class CuratorDemo {

    private static final String ZOOKEEPER_CONNECT_STRING = "localhost:2181";
    private static final String PARENT_NODE = "/parent";
    private static final String[] CHILD_NODES = {"/child1", "/child2", "/child3"};

    public static void main(String[] args) throws Exception {
        TestingServer server = new TestingServer(); // Start an embedded ZooKeeper server for testing

        CuratorFramework client = null;
        try {
            // Create a Curator client
            client = CuratorFrameworkFactory.newClient(ZOOKEEPER_CONNECT_STRING, new ExponentialBackoffRetry(1000, 3));
            client.start();

            // Create the parent node and three child nodes
            client.create().forPath(PARENT_NODE, "Parent Data".getBytes());
            for (String childNode : CHILD_NODES) {
                client.create().forPath(PARENT_NODE + childNode, "Child Data".getBytes());
            }
//            Thread.sleep(5000);

            // Register a watcher to listen for "deleting" events on child nodes
            for (String childNode : CHILD_NODES) {
                final String nodePath = PARENT_NODE + childNode;
                Stat stat = client.checkExists().usingWatcher(new CuratorWatcher() {
                    @Override
                    public void process(WatchedEvent event) throws Exception {
                        System.out.println(nodePath + " event type: "+event.getType());
//                        if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
//                            System.out.println(nodePath + " has been deleted.");
//                        }
                    }
                }).forPath(nodePath);

                // Check for initial state (if the node already exists and is deleted)
                if (stat == null) {
                    System.out.println(nodePath + " does not exist.");
                }
            }

            // Delete the parent node (this will trigger "deleting" events)
            client.delete().deletingChildrenIfNeeded().forPath(PARENT_NODE);

            // Sleep for a while to allow time for the events to be processed
            Thread.sleep(5000);
        } finally {
            CloseableUtils.closeQuietly(client);
            CloseableUtils.closeQuietly(server); // Close the embedded ZooKeeper server
        }
    }
}

