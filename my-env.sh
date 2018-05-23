#!/usr/bin/env bash
# run with ". ./my-env.sh"

export HVERS="2.8.2"
export HADOOPSRC="/home/tux/big/hdo/hadoop-${HVERS}-src/"
export HBASE_CONF_DIR="/etc/hbase/"

export HDXRAMFS_SRC=$(pwd)
export JAVA_HOME=/usr
export HADOOP_CONF_DIR="${HADOOPSRC}/hadoop-dist/target/hadoop-${HVERS}/etc/hadoop/"
export HADOOP_HOME="${HADOOPSRC}/hadoop-dist/target/hadoop-${HVERS}/"


export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin/:$HADOOP_HOME/sbin/:$PATH

echo cdbin............... cd ${HADOOP_HOME}
alias cdbin="cd ${HADOOP_HOME}"
echo cdsrc............... cd ${HADOOPSRC}
alias cdsrc="cd ${HADOOPSRC}"
echo cdxfs............... cd ${HDXRAMFS_SRC}
alias cdxfs="cd ${HDXRAMFS_SRC}"

echo mvn clean................ clean up hadoopDxramFs package
echo mvn package.............. creates hadoopDxramFs package
echo installDxramfs........... copies hadoopDxramFs JARs, libs, JNI to Hadoop
echo updateHDFS............... overwrites important HDFS jar files in hdfs/lib/ with new ones
echo startDxramFsPeer ........ start the local dxramfsPeer to relay between hadoop and DXRAM

installDxramfs() {
    cp -f ${HDXRAMFS_SRC}/target/hadoop-dxram-fs-*.jar \
        ${HADOOP_HOME}/share/hadoop/common/lib/hadoopDxramFs.jar
    cp -f ${HDXRAMFS_SRC}/lib/*.jar \
        ${HADOOP_HOME}/share/hadoop/common/lib/
    cp -rf ${HDXRAMFS_SRC}/jni \
        ${HADOOP_HOME}/jni
}

updateHDFS() {
    cp ${HADOOPSRC}/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-${HVERS}.jar \
       ${HADOOP_HOME}/share/hadoop/common/lib/
    cp ${HADOOPSRC}/hadoop-hdfs-project/hadoop-hdfs-native-client/target/hadoop-hdfs-native-client-${HVERS}.jar \
       ${HADOOP_HOME}/share/hadoop/hdfs/
    cp ${HADOOPSRC}/hadoop-hdfs-project/hadoop-hdfs-client/target/hadoop-hdfs-client-${HVERS}.jar \
       ${HADOOP_HOME}/share/hadoop/hdfs/lib/
}

startDxramFsPeer() {
    java -XX:+UseMembar \
    -Dlog4j.configurationFile=${HDXRAMFS_SRC}/target/classes/log4j2.xml \
    -cp ${HDXRAMFS_SRC}/target/hadoop-dxram-fs-1.0-SNAPSHOT.jar:${HDXRAMFS_SRC}/lib/* \
    de.hhu.bsinfo.hadoop.fs.dxnet.DxramFsPeer ${HDXRAMFS_SRC}/target/classes/dxnet.json
}
