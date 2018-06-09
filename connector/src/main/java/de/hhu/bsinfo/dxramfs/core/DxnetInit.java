package de.hhu.bsinfo.dxramfs.core;

import de.hhu.bsinfo.dxnet.DXNet;
import de.hhu.bsinfo.dxnet.DXNetConfig;
import de.hhu.bsinfo.dxnet.DXNetNodeMap;

import java.net.InetSocketAddress;

public class DxnetInit {
    private DXNet _dxNet;
    // @todo this is a hack for testing
    public A100bMessage.InHandler inHandler;

    public DxnetInit(NodePeerConfig nopeConfig, short myNodeId) {
        DXNetConfig conf = new DXNetConfig();
        DXNetNodeMap nodeMap = null;

        conf.getCoreConfig().setOwnNodeId(myNodeId);
        nodeMap = new DXNetNodeMap(myNodeId);

        // the haddoop node, who wants to acces to the dxram peers
        nodeMap.addNode(
                nopeConfig.nodeId,
                new InetSocketAddress(nopeConfig.addr, nopeConfig.port)
        );

        for (NodePeerConfig.PeerConfig nc: nopeConfig.dxPeers) {
            nodeMap.addNode(
                    nc.nodeId,
                    new InetSocketAddress(nc.addr, nc.port)
            );
        }

        _dxNet = new DXNet(
                conf.getCoreConfig(),
                conf.getNIOConfig(),
                conf.getIBConfig(),
                conf.getLoopbackConfig(),
                nodeMap
        );

        /// @todo brauchbare message typen!
        _dxNet.registerMessageType(
                A100bMessage.MTYPE,
                A100bMessage.TAG,
                A100bMessage.class
        );
        inHandler = new A100bMessage.InHandler();
        _dxNet.register(
                A100bMessage.MTYPE,
                A100bMessage.TAG,
                inHandler
        );
    }

    public DXNet getDxNet() {
        return _dxNet;
    }
}
