#!/usr/bin/env bash
# run with ". ./environment_tools.sh"

export DXRAM_RUN="/home/tux/big/gDXRAM/run/dxram/"
export ZOOKEEPER_ROOT="${HOME}/zookeeper/"
export DATADIRECTORY="/tmp/zookeeper"

export HALLODATAS_SRC="${PWD}/dxapp/"


echo zook..................... start zookeeper
echo .
echo cdapp ................... cd ${HALLODATAS_SRC}
alias cdapp="cd ${HALLODATAS_SRC}"
echo ./build.sh .............. creates peer app in ${HALLODATAS_SRC}
echo instpeer ................ copies DxramFsApp JAR to ${DXRAM_RUN}/dxapp/
echo .
echo cdrun ................... cd ${DXRAM_RUN}
alias cdrun="cd ${DXRAM_RUN}"
echo newSuper ................ start a dxram superpeer
echo newPeer IPADDR PORT ..... start a dxram peer with dxramfs App
echo .
echo do not forget to add the app to m_autoStart in ${DXRAM_RUN}config/dxram.json
cat hint.txt


instpeer() {
    echo "start install App to ${DXRAM_RUN}"
    cp -f ${HALLODATAS_SRC}/build/libs/dxapp-hallodatas-*.jar ${DXRAM_RUN}/dxapp/
}


zook() {
    if [ -d "${DATADIRECTORY}" ]; then
        rm -r ${DATADIRECTORY}/*;
    fi
    ${ZOOKEEPER_ROOT}/bin/zkEnv.sh
    ${ZOOKEEPER_ROOT}/bin/zkServer.sh start
    ${ZOOKEEPER_ROOT}/bin/zkCli.sh
}


newSuper() {
    cd ${DXRAM_RUN}
    DXRAM_OPTS="-Dlog4j.configurationFile=config/log4j2.xml -Ddxram.config=config/dxram.json -Ddxram.m_config.m_engineConfig.m_role=Superpeer -Ddxram.m_config.m_engineConfig.m_address.m_ip=127.0.0.1 -Ddxram.m_config.m_engineConfig.m_address.m_port=22221" ./bin/dxram
}


newPeer() {
    cd ${DXRAM_RUN}
    if (( $# != 2 )); then
        echo "newPeer IPADDR PORT"
        echo "PORT for example: 22222, 22223"
        return
    fi
    DXRAM_OPTS="-Dlog4j.configurationFile=config/log4j2.xml -Ddxram.config=config/dxram.json -Ddxram.m_config.m_engineConfig.m_role=Peer -Ddxram.m_config.m_engineConfig.m_address.m_ip=${1} -Ddxram.m_config.m_engineConfig.m_address.m_port=${2}" ./bin/dxram
}
