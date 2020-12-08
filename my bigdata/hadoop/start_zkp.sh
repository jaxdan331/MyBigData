#!/bin/bash

# 启动 ZooKeeper 集群
echo "start ZooKeeper cluster"
# start node1
docker exec -it hadoop-node1 mv /usr/local/zookeeper-3.4.6/zkdata/myid1 /usr/local/zookeeper-3.4.6/zkdata/myid
docker exec -it hadoop-node1 rm -f /usr/local/zookeeper-3.4.6/zkdata/myid2 /usr/local/zookeeper-3.4.6/zkdata/myid3
docker exec -it hadoop-node1 zkServer.sh start
# start node2
docker exec -it hadoop-node2 mv /usr/local/zookeeper-3.4.6/zkdata/myid2 /usr/local/zookeeper-3.4.6/zkdata/myid
docker exec -it hadoop-node1 rm -f /usr/local/zookeeper-3.4.6/zkdata/myid1 /usr/local/zookeeper-3.4.6/zkdata/myid3
docker exec -it hadoop-node2 zkServer.sh start
# start node3
docker exec -it hadoop-node3 mv /usr/local/zookeeper-3.4.6/zkdata/myid3 /usr/local/zookeeper-3.4.6/zkdata/myid
docker exec -it hadoop-node1 rm -f /usr/local/zookeeper-3.4.6/zkdata/myid1 /usr/local/zookeeper-3.4.6/zkdata/myid2
docker exec -it hadoop-node3 zkServer.sh start