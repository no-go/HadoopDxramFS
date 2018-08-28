package de.hhu.bsinfo.app.dxramfscore;

import de.hhu.bsinfo.dxnet.DXNet;
import de.hhu.bsinfo.dxnet.DXNetConfig;
import de.hhu.bsinfo.dxnet.DXNetNodeMap;
import de.hhu.bsinfo.app.dxramfscore.rpc.*;

import java.net.InetSocketAddress;

public class DxnetInit {
    private DXNet _dxNet;

    public BlockLocationsMessage.InHandler blmh;
    public CreateMessage.InHandler cmh;
    public DeleteMessage.InHandler dmh;
    public ExistsMessage.InHandler emh;
    public FileLengthMessage.InHandler flmh;
    public FileStatusMessage.InHandler fsmh;
    public IsDirectoryMessage.InHandler idmh;
    public ListMessage.InHandler lmh;
    public MkDirsMessage.InHandler mdmh;
    public OpenMessage.InHandler omh;
    public RenameToMessage.InHandler rtmh;

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
        _dxNet.registerMessageType(FileStatusMessage.MTYPE, FileStatusMessage.TAG, FileStatusMessage.class);
        _dxNet.registerMessageType(IsDirectoryMessage.MTYPE, IsDirectoryMessage.TAG, IsDirectoryMessage.class);
        _dxNet.registerMessageType(ListMessage.MTYPE, ListMessage.TAG, ListMessage.class);
        _dxNet.registerMessageType(MkDirsMessage.MTYPE, MkDirsMessage.TAG, MkDirsMessage.class);
        _dxNet.registerMessageType(OpenMessage.MTYPE, OpenMessage.TAG, OpenMessage.class);
        _dxNet.registerMessageType(RenameToMessage.MTYPE, RenameToMessage.TAG, RenameToMessage.class);

        blmh = new BlockLocationsMessage.InHandler();
        cmh = new CreateMessage.InHandler();
        dmh = new DeleteMessage.InHandler();
        emh = new ExistsMessage.InHandler();
        flmh = new FileLengthMessage.InHandler();
        fsmh = new FileStatusMessage.InHandler();
        idmh = new IsDirectoryMessage.InHandler();
        lmh = new ListMessage.InHandler();
        mdmh = new MkDirsMessage.InHandler();
        omh = new OpenMessage.InHandler();
        rtmh = new RenameToMessage.InHandler();

        _dxNet.register(BlockLocationsMessage.MTYPE, BlockLocationsMessage.TAG, blmh);
        _dxNet.register(CreateMessage.MTYPE, CreateMessage.TAG, cmh);
        _dxNet.register(DeleteMessage.MTYPE, DeleteMessage.TAG, dmh);
        _dxNet.register(ExistsMessage.MTYPE, ExistsMessage.TAG, emh);
        _dxNet.register(FileLengthMessage.MTYPE, FileLengthMessage.TAG, flmh);
        _dxNet.register(FileStatusMessage.MTYPE, FileStatusMessage.TAG, fsmh);
        _dxNet.register(IsDirectoryMessage.MTYPE, IsDirectoryMessage.TAG, idmh);
        _dxNet.register(ListMessage.MTYPE, ListMessage.TAG, lmh);
        _dxNet.register(MkDirsMessage.MTYPE, MkDirsMessage.TAG, mdmh);
        _dxNet.register(OpenMessage.MTYPE, OpenMessage.TAG, omh);
        _dxNet.register(RenameToMessage.MTYPE, RenameToMessage.TAG, rtmh);
    }

    public DXNet getDxNet() {
        return _dxNet;
    }
};
