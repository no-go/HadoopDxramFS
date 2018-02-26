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

echo cdbin............. cd ${HADOOP_HOME}
alias cdbin="cd ${HADOOP_HOME}"
echo cdsrc............. cd ${HADOOPSRC}
alias cdsrc="cd ${HADOOPSRC}"
echo cddxfs............ cd ${HDXRAMFS_SRC}
alias cddxfs="cd ${HDXRAMFS_SRC}"

echo installDxramfs.... copies hadoopDxramFs JAR to Hadoop LIBs
alias installDxramfs="cp -f ${HDXRAMFS_SRC}/target/hadoop-dxram-fs-*.jar ${HADOOP_HOME}/share/hadoop/common/lib/hadoopDxramFs.jar"

echo updateHDFS........ overwrites important HDFS jar files in hdfs/lib/ with new ones

updateHDFS() {
    cp ${HADOOPSRC}/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-${HVERS}.jar \
       ${HADOOP_HOME}/share/hadoop/common/lib/
    cp ${HADOOPSRC}/hadoop-hdfs-project/hadoop-hdfs-native-client/target/hadoop-hdfs-native-client-${HVERS}.jar \
       ${HADOOP_HOME}/share/hadoop/hdfs/
    cp ${HADOOPSRC}/hadoop-hdfs-project/hadoop-hdfs-client/target/hadoop-hdfs-client-${HVERS}.jar \
       ${HADOOP_HOME}/share/hadoop/hdfs/lib/
}