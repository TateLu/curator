/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package demos;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.time.LocalDateTime;
import java.util.*;

/**
 * listen server status, online and offline
 * */
public class ServerStatusListener {
    private static final String ZOOKEEPER_CONNECTION_STRING = "localhost:2181";
    private static final String SERVERS_PATH = "/servers";

    public static void main(String[] args) {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(ZOOKEEPER_CONNECTION_STRING,
                new ExponentialBackoffRetry(1000, 3));
        curatorFramework.start();

        try {
            // Create the servers path if it doesn't exist
            if (curatorFramework.checkExists().forPath(SERVERS_PATH) == null) {
                curatorFramework.create().creatingParentsIfNeeded().forPath(SERVERS_PATH);
            }

            // Create a PathChildrenCache for the servers path
            PathChildrenCache pathChildrenCache = new PathChildrenCache(curatorFramework, SERVERS_PATH, true);
            pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

            // Add a listener to the PathChildrenCache
            pathChildrenCache.getListenable().addListener((client, event) -> {
                ChildData data = event.getData();
                if (data != null) {
                    switch (event.getType()) {
                        case CHILD_ADDED:
                            // Server is online
                            System.out.println(LocalDateTime.now() +" Server added: " + data.getPath());
                            break;
                        case CHILD_REMOVED:
                            // Server is offline
                            System.out.println(LocalDateTime.now()+" Server removed: " + data.getPath());
                            break;
                        case CHILD_UPDATED:
                            // Server data is updated
                            System.out.println(LocalDateTime.now()+" Server updated: " + data.getPath());
                            break;
                        default:
                            break;
                    }
                }
            });

            // Simulate servers going online and offline
            Random random = new Random();
            while (true) {
                String serverName = "server" + random.nextInt(10); // Generate a random server name

                // Check if the server already exists
                if (curatorFramework.checkExists().forPath(SERVERS_PATH + "/" + serverName) != null) {
                    // Server already exists, remove it to simulate going offline
                    curatorFramework.delete().forPath(SERVERS_PATH + "/" + serverName);
                } else {
                    // Server doesn't exist, create it to simulate going online
                    curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                            .forPath(SERVERS_PATH + "/" + serverName);
                }

                Thread.sleep(2000); // Sleep for 5 seconds before simulating another server status change
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            curatorFramework.close();
        }
    }
}
