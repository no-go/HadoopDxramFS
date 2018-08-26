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
#echo startDxnetPeer -old...... start the local dxnetPeer to test hadoop requests
#echo startDxramFsPeer ........ start the local dxramfsPeer to relay between hadoop and DXRAM
echo .
#echo DXRAM tools - old
#echo =================
#echo startSuperpeer IP PORT... start a DXRAM superpeer
#echo "                          on IP PORT (e.g. 127.0.0.1 22221)"
#echo startDxPeer IP PORT ..... this will start the default peer applications of DXRAM
#echo "                          see Msg.local_peer* in etc/hadoop/core-site.xml"
#echo startTerm HOST PORT ..... start a DXRAM TerminalClient and try to connect to 
#echo "                          HOST:PORT of a peer application (PORT is not the"
#echo "                          DXRAM-Peer port! default is 22220)"

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


newSuper() {
    # dxram and not dxnet peer!
    DXRAM_OPTS="-Dlog4j.configurationFile=config/log4j2.xml -Ddxram.config=config/dxram.json -Ddxram.m_config.m_engineConfig.m_role=Superpeer -Ddxram.m_config.m_engineConfig.m_address.m_ip=127.0.0.1 -Ddxram.m_config.m_engineConfig.m_address.m_port=22221" ./bin/dxram
}

newPeer() {
    # dxram and not dxnet peer!
    if (( $# != 2 )); then
        echo "newPeer IPADDR PORT"
        echo "PORT for example: 22222, 22223"
        return
    fi
    DXRAM_OPTS="-Dlog4j.configurationFile=config/log4j2.xml -Ddxram.config=config/dxram.json -Ddxram.m_config.m_engineConfig.m_role=Peer -Ddxram.m_config.m_engineConfig.m_address.m_ip=${1} -Ddxram.m_config.m_engineConfig.m_address.m_port=${2}" ./bin/dxram
}

#------------------------------------------------------------------------------------

#startDxnetPeer() {
    #java \
    #-Dlog4j.configurationFile=${HDXRAMFS_SRC}/target/classes/log4j2.xml \
    #-cp ${HDXRAMFS_SRC}/target/hadoop-dxram-fs-0.07.jar:${HDXRAMFS_SRC}/lib/* \
    #de.hhu.bsinfo.dxramfs.Msg.DxramFsPeer ${HADOOP_CONF_DIR}/core-site.xml
#}

#startSuperpeer() {
    #java -XX:+UseMembar \
    #-Dlog4j.configurationFile=${HDXRAMFS_SRC}/target/classes/log4j.xml \
    #-Ddxram.config=${HDXRAMFS_SRC}/target/classes/dxram.json \
    #-Ddxram.m_config.m_engineConfig.m_address.m_ip=${1} \
    #-Ddxram.m_config.m_engineConfig.m_address.m_port=${2} \
    #-Ddxram.m_config.m_engineConfig.m_role=Superpeer \
    #-Ddxram.m_config.m_componentConfigs[NetworkComponentConfig].m_core.m_device=Ethernet \
    #-Ddxram.m_config.m_componentConfigs[NetworkComponentConfig].m_core.m_numMessageHandlerThreads=2 \
    #-cp ${HDXRAMFS_SRC}/target/hadoop-dxram-fs-0.07.jar:${HDXRAMFS_SRC}/lib/* \
    #de.hhu.bsinfo.dxram.DXRAM
#}

#startDxPeer() {
    #java -XX:+UseMembar \
    #-Dlog4j.configurationFile=${HDXRAMFS_SRC}/target/classes/log4j.xml \
    #-Ddxram.config=${HDXRAMFS_SRC}/target/classes/dxram.json \
    #-Ddxram.m_config.m_engineConfig.m_address.m_ip=${1} \
    #-Ddxram.m_config.m_engineConfig.m_address.m_port=${2} \
    #-Ddxram.m_config.m_engineConfig.m_role=Peer \
    #-Ddxram.m_config.m_componentConfigs[MemoryManagerComponentConfig].m_keyValueStoreSize.m_value=128 \
    #-Ddxram.m_config.m_componentConfigs[MemoryManagerComponentConfig].m_keyValueStoreSize.m_unit=mb \
    #-cp ${HDXRAMFS_SRC}/target/hadoop-dxram-fs-0.07.jar:${HDXRAMFS_SRC}/lib/* \
    #de.hhu.bsinfo.dxram.DXRAM
#}

#startTerm() {
    #java -Dlog4j.configurationFile=${HDXRAMFS_SRC}/target/classes/log4j.xml \
    #-Ddxram.config=${HDXRAMFS_SRC}/target/classes/dxram.json \
    #-cp ${HDXRAMFS_SRC}/target/hadoop-dxram-fs-1.0-SNAPSHOT.jar:${HDXRAMFS_SRC}/lib/* \
    #de.hhu.bsinfo.dxterm.TerminalClient localhost 22220
#}
