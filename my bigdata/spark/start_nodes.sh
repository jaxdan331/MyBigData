#!/bin/bash

echo start containers

echo "start spark-node1 container ..."
docker run -itd --restart=always --net hadoop --ip 172.18.0.2 --privileged -p 8080:8080 -p 50070:50070 -p 9000:9000 --name spark-node1 --hostname spark-node1  --add-host spark-node2:172.18.0.3 --add-host spark-node3:172.18.0.4 spark /bin/bash
echo "start spark-node2 container..."
docker run -itd --restart=always --net hadoop --ip 172.18.0.3 --privileged -p 8042:8042 -p 51010:50010 -p 51020:50020 --name spark-node2 --hostname spark-node2 --add-host spark-node1:172.18.0.2 --add-host spark-node3:172.18.0.4 spark  /bin/bash
echo "start spark-node3 container..."
docker run -itd --restart=always --net hadoop --ip 172.18.0.4 --privileged -p 8043:8042 -p 51011:50011 -p 51021:50021 --name spark-node3 --hostname spark-node3 --add-host spark-node1:172.18.0.2 --add-host spark-node2:172.18.0.3  spark /bin/bash

sleep 5
docker exec -it spark-node1 /usr/sbin/sshd
docker exec -it spark-node2 /usr/sbin/sshd
docker exec -it spark-node3 /usr/sbin/sshd

echo finished
docker ps
