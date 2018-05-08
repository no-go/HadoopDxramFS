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

echo mvn clean........... clean up hadoopDxramFs package
echo mvn package......... creates hadoopDxramFs package
echo mvn exec:java@peer.. start the local DxramFsPeer application
echo installDxramfs...... copies hadoopDxramFs JARs, libs, JNI to Hadoop
echo updateHDFS.......... overwrites important HDFS jar files in hdfs/lib/ with new ones

installDxramfs() {
    cp -f ${HDXRAMFS_SRC}/target/hadoop-dxram-fs-*.jar \
        ${HADOOP_HOME}/share/hadoop/common/lib/hadoopDxramFs.jar
    cp -f ${HDXRAMFS_SRC}/lib/log4j-*.jar \
        ${HADOOP_HOME}/share/hadoop/common/lib/
    echo "unsure coping gson-2.7 JAR-file. version 2.2.4 is still installed in hadoop"
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