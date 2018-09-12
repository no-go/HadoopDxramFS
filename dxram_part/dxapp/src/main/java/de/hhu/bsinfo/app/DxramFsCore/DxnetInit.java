package de.hhu.bsinfo.app.dxramfscore;

import de.hhu.bsinfo.dxnet.DXNet;
import de.hhu.bsinfo.dxnet.DXNetConfig;
import de.hhu.bsinfo.dxnet.DXNetNodeMap;
import de.hhu.bsinfo.app.dxramfscore.rpc.*;

import java.net.InetSocketAddress;

public class DxnetInit {
    private DXNet _dxNet;
    // tag 10 - 20
    public BlockLocationsMessage.InHandler blmh;
    public CreateMessage.InHandler cmh;
    public DeleteMessage.InHandler dmh;
    public ExistsMessage.InHandler emh;
    public FileLengthMessage.InHandler flmh;
    public IsDirectoryMessage.InHandler idmh;
    public ListMessage.InHandler lmh;
    public MkDirsMessage.InHandler mdmh;
    public RenameToMessage.InHandler rtmh;

    public GetBlockMessage.InHandler getBlockmh;
    public AskBlockMessage.InHandler askBlockmh;

    // tag 21-


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

        _dxNet.registerMessageType(BlockLocationsMessage.MTYPE, BlockLocationsMessage.TAG, BlockLocationsMessage.class);
        _dxNet.registerMessageType(CreateMessage.MTYPE, CreateMessage.TAG, CreateMessage.class);
        _dxNet.registerMessageType(DeleteMessage.MTYPE, DeleteMessage.TAG, DeleteMessage.class);
        _dxNet.registerMessageType(ExistsMessage.MTYPE, ExistsMessage.TAG, ExistsMessage.class);
        _dxNet.registerMessageType(FileLengthMessage.MTYPE, FileLengthMessage.TAG, FileLengthMessage.class);
        _dxNet.registerMessageType(IsDirectoryMessage.MTYPE, IsDirectoryMessage.TAG, IsDirectoryMessage.class);
        _dxNet.registerMessageType(ListMessage.MTYPE, ListMessage.TAG, ListMessage.class);
        _dxNet.registerMessageType(MkDirsMessage.MTYPE, MkDirsMessage.TAG, MkDirsMessage.class);
        _dxNet.registerMessageType(RenameToMessage.MTYPE, RenameToMessage.TAG, RenameToMessage.class);

        _dxNet.registerMessageType(GetBlockMessage.MTYPE, GetBlockMessage.TAG, GetBlockMessage.class);
        _dxNet.registerMessageType(AskBlockMessage.MTYPE, AskBlockMessage.TAG, AskBlockMessage.class);

        blmh = new BlockLocationsMessage.InHandler();
        cmh = new CreateMessage.InHandler();
        dmh = new DeleteMessage.InHandler();
        emh = new ExistsMessage.InHandler();
        flmh = new FileLengthMessage.InHandler();
        idmh = new IsDirectoryMessage.InHandler();
        lmh = new ListMessage.InHandler();
        mdmh = new MkDirsMessage.InHandler();
        rtmh = new RenameToMessage.InHandler();

        getBlockmh = new GetBlockMessage.InHandler();
        askBlockmh = new AskBlockMessage.InHandler();

        _dxNet.register(BlockLocationsMessage.MTYPE, BlockLocationsMessage.TAG, blmh);
        _dxNet.register(CreateMessage.MTYPE, CreateMessage.TAG, cmh);
        _dxNet.register(DeleteMessage.MTYPE, DeleteMessage.TAG, dmh);
        _dxNet.register(ExistsMessage.MTYPE, ExistsMessage.TAG, emh);
        _dxNet.register(FileLengthMessage.MTYPE, FileLengthMessage.TAG, flmh);
        _dxNet.register(IsDirectoryMessage.MTYPE, IsDirectoryMessage.TAG, idmh);
        _dxNet.register(ListMessage.MTYPE, ListMessage.TAG, lmh);
        _dxNet.register(MkDirsMessage.MTYPE, MkDirsMessage.TAG, mdmh);
        _dxNet.register(RenameToMessage.MTYPE, RenameToMessage.TAG, rtmh);

        _dxNet.register(GetBlockMessage.MTYPE, GetBlockMessage.TAG, getBlockmh);
        _dxNet.register(AskBlockMessage.MTYPE, AskBlockMessage.TAG, askBlockmh);
    }

    public DXNet getDxNet() {
        return _dxNet;
    }
};
