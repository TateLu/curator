package demos.pojo;

import cn.hutool.json.JSONUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.nio.charset.StandardCharsets;

public class ZooKeeperDataWatcher {
    private static final String ZOOKEEPER_CONNECTION_STRING = "localhost:2181";
    private static final String ZOOKEEPER_PATH = "/myData";

    public static void main(String[] args) throws Exception {
        // Create CuratorFramework client
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(ZOOKEEPER_CONNECTION_STRING,
                new ExponentialBackoffRetry(1000, 3));
        curatorFramework.start();

        // Create a ZooKeeper node and set data
        byte[] data = serializeData(new MyData("John", 30));
        curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                .forPath(ZOOKEEPER_PATH, data);

        // Watch for changes to the ZooKeeper node
        NodeCache nodeCache = new NodeCache(curatorFramework, ZOOKEEPER_PATH);
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                ChildData currentData = nodeCache.getCurrentData();
                byte[] newData = currentData.getData();
                MyData myData = deserializeData(newData);
                System.out.println("Data changed: " + myData);
            }
        });
        nodeCache.start();

        // Update the data in the ZooKeeper node
        byte[] updatedData = serializeData(new MyData("Alice", 25));
        curatorFramework.setData().forPath(ZOOKEEPER_PATH, updatedData);

        // Wait for a while to see the output
        Thread.sleep(2000);

        // Remove the node from ZooKeeper
        curatorFramework.delete().forPath(ZOOKEEPER_PATH);

        // Close the CuratorFramework client
        curatorFramework.close();
    }

    private static byte[] serializeData(MyData myData) {
        // Implement your serialization logic here
        // For example, you can use JSON, XML, or any other serialization library
        // Return the byte array representation of the serialized data

        return JSONUtil.toJsonStr(myData).getBytes(StandardCharsets.UTF_8);
    }

    private static MyData deserializeData(byte[] data) {
        // Implement your deserialization logic here
        // For example, you can use JSON, XML, or any other serialization library
        // Return the deserialized MyData object
        String str = new String(data,StandardCharsets.UTF_8);
        return JSONUtil.toBean(str,MyData.class);
    }
}
