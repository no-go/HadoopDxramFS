#!/usr/bin/env bash

# The cd is not easy in bash !?!
# cd /home/tux/big/hdo/hadoop-2.8.2-src/hadoop-hdfs-project/hadoop-hdfs-client/
# mvn clean
# cd /home/tux/big/hdo/hadoop-2.8.2-src/
# mvn package -DskipTests

cp ${HADOOP_HOME}/../../../hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-2.8.2.jar \
   ${HADOOP_HOME}/share/hadoop/common/lib/
cp ${HADOOP_HOME}/../../../hadoop-hdfs-project/hadoop-hdfs-native-client/target/hadoop-hdfs-native-client-2.8.2.jar \
   ${HADOOP_HOME}/share/hadoop/hdfs/
cp ${HADOOP_HOME}/../../../hadoop-hdfs-project/hadoop-hdfs-client/target/hadoop-hdfs-client-2.8.2.jar \
   ${HADOOP_HOME}/share/hadoop/hdfs/lib/
