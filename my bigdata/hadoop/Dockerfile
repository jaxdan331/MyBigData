FROM centos-hadoop
MAINTAINER DJX djx@ss.pku.edu.cn

# install zookeeper & config PATH
ADD  ./zookeeper-3.4.6.tar.gz /usr/local/
ENV ZOOKEEPER_HOME /usr/local/zookeeper-3.4.6
ENV PATH $PATH:$ZOOKEEPER_HOME/bin
RUN mv /usr/local/zookeeper-3.4.6/conf/zoo_sample.cfg /usr/local/zookeeper-3.4.6/conf/zoo.cfg
# 配置 Zookeeper 集群
COPY ./zoo.cfg /usr/local/zookeeper-3.4.6/conf/zoo.cfg
RUN mkdir /usr/local/zookeeper-3.4.6/zkdata/
# 三个节点的id需要分别配置，结合启动脚本
COPY ./myid1 /usr/local/zookeeper-3.4.6/zkdata/
COPY ./myid2 /usr/local/zookeeper-3.4.6/zkdata/
COPY ./myid3 /usr/local/zookeeper-3.4.6/zkdata/

# install habse & config PATH
ADD ./hbase-1.4.13-bin.tar.gz /usr/local
ENV HBASE_HOME /usr/local/hbase-1.4.13
ENV PATH $PATH:$HBASE_HOME/bin
# 配置 HBase 集群
COPY ./hbase-site.xml /usr/local/hbase-1.4.13/conf/hbase-site.xml
COPY ./hbase-env.sh /usr/local/hbase-1.4.13/conf/hbase-env.sh
COPY ./regionservers /usr/local/hbase-1.4.13/conf/regionservers
COPY ./backup-masters /usr/local/hbase-1.4.13/conf/
RUN cp /usr/local/hadoop-2.8.5/etc/hadoop/hdfs-site.xml /usr/local/hbase-1.4.13/conf/
RUN cp /usr/local/hadoop-2.8.5/etc/hadoop/core-site.xml /usr/local/hbase-1.4.13/conf/
