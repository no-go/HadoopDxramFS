package de.hhu.bsinfo.dxramfs.core;

import de.hhu.bsinfo.dxnet.DXNet;
import de.hhu.bsinfo.dxnet.DXNetConfig;
import de.hhu.bsinfo.dxnet.DXNetNodeMap;
import de.hhu.bsinfo.dxramfs.core.rpc.Exists;

import java.net.InetSocketAddress;

public class DxnetInit {
    private DXNet _dxNet;
    // @todo this is a hack for testing
    public A100bMessage.InHandler inHandler;
    public Exists.InHandler inHandExists;

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

        _dxNet.registerMessageType(A100bMessage.MTYPE, A100bMessage.TAG, A100bMessage.class);
        _dxNet.registerMessageType(Exists.MTYPE, Exists.TAG, Exists.class);

        inHandler = new A100bMessage.InHandler();
        inHandExists = new Exists.InHandler();

        _dxNet.register(A100bMessage.MTYPE, A100bMessage.TAG, inHandler);
        _dxNet.register(Exists.MTYPE, Exists.TAG, inHandExists);
    }

    public DXNet getDxNet() {
        return _dxNet;
    }
}
