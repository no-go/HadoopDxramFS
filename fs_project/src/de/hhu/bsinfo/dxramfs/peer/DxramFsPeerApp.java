package de.hhu.bsinfo.dxramfs.peer;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.app.AbstractApplication;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxramfs.core.*;
import de.hhu.bsinfo.dxramfs.core.rpc.*;
import de.hhu.bsinfo.dxutils.NodeID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import de.hhu.bsinfo.dxram.data.ChunkID;

import java.util.ArrayList;

public class DxramFsPeerApp extends AbstractApplication {
    @Expose
    private int dxnet_local_id = 40; // dummy - you get it from config!
    @Expose
    private String dxnet_local_addr = "127.0.0.1"; // dummy - you get it from config!
    @Expose
    private int dxnet_local_port = 6500; // dummy - you get it from config!
    @Expose
    private int dxnet_local_peer_id = 41; // dummy - you get it from config!
    @Expose
    private String dxnet_local_peer_addr = "127.0.0.1"; // dummy - you get it from config!
    @Expose
    private int dxnet_local_peer_port = 6501; // dummy - you get it from config!
    @Expose
    private String ROOT_Chunk = "dummy"; // dummy - you get it from config!
    @Expose
    private int file_blocksize = 8*1024*1024; // dummy - you get it from config!
    @Expose
    private int blockinfo_ids_each_fsnode = 123; // dummy - you get it from config!
    @Expose
    private int max_pathlength_chars = 256; // dummy - you get it from config!


    private static final Logger LOG = LogManager.getFormatterLogger(DxramFsPeerApp.class.getSimpleName());

    private long ROOT_CID;
    private BootService bootS;
    private ChunkService chunkS;
    private NameserviceService nameS;

    private FsNodeChunk ROOTN;
    private DxnetInit dxnetInit;

    public static NodePeerConfig nopeConfig;

    @Override
    public DXRAMVersion getBuiltAgainstVersion() {
        return DXRAM.VERSION;
    }

    @Override
    public String getApplicationName() {
        return "DxramFsPeer";
    }

    @Override
    public boolean useConfigurationFile() {
        return true;
    }

    @Override
    public void main() {
        bootS = getService(BootService.class);
        chunkS = getService(ChunkService.class);
        nameS = getService(NameserviceService.class);

        System.out.println(
                "application " + getApplicationName() + " on a peer" +
                        " and my dxram node id is " + NodeID.toHexString(bootS.getNodeID())
        );
        System.out.println("Where your Hadoop Node is:");
        System.out.println("DXNET dxnet_local_id  : " + dxnet_local_id);
        System.out.println("DXNET dxnet_local_addr: " + dxnet_local_addr);
        System.out.println("DXNET dxnet_local_port: " + dxnet_local_port);

        nopeConfig = new NodePeerConfig();
        nopeConfig.nodeId = (short) dxnet_local_id;
        nopeConfig.addr = dxnet_local_addr;
        nopeConfig.port = dxnet_local_port;

        nopeConfig.dxPeers = new ArrayList<>();

        // @todo if you realy want to run many dxram/dxnet peers on local, you have to change this !!
        System.out.println("How Hadoop DxramFs has to connect me:");
        System.out.println("DXNET dxnet_local_peer_id  : " + dxnet_local_peer_id);
        System.out.println("DXNET dxnet_local_peer_addr: " + dxnet_local_peer_addr);
        System.out.println("DXNET dxnet_local_peer_port: " + dxnet_local_peer_port);
        NodePeerConfig.PeerConfig aPeer = new NodePeerConfig.PeerConfig();
        aPeer.nodeId = (short) dxnet_local_peer_id;
        aPeer.addr = dxnet_local_peer_addr;
        aPeer.port = dxnet_local_peer_port;
        nopeConfig.dxPeers.add(aPeer);

        DxramFsConfig.file_blocksize = file_blocksize;
        DxramFsConfig.blockinfo_ids_each_fsnode = blockinfo_ids_each_fsnode;
        DxramFsConfig.max_pathlength_chars = max_pathlength_chars;

        dxnetInit = new DxnetInit(nopeConfig, nopeConfig.dxPeers.get(0).nodeId);

        ROOTN = new FsNodeChunk();
        if (nameS.getChunkID(ROOT_Chunk, 10) == ChunkID.INVALID_ID) {
            // initial, if root does not exists
            ROOTN.get().fsNodeType = FsNodeType.FOLDER;
            ROOTN.get().name = "/";
            ROOT_CID = chunkS.create(ROOTN.sizeofObject(), 1)[0];
            nameS.register(ROOT_CID, ROOT_Chunk);

            ROOT_CID = nameS.getChunkID(ROOT_Chunk, 10);
            ROOTN.setID(ROOT_CID);
            chunkS.get(ROOTN);

            // initial, if root does not exists
            //ROOTN.get().fsNodeType = FsNodeType.FOLDER;
            //ROOTN.get().name = "/";
            ROOTN.get().referenceId = ROOT_CID;
            ROOTN.get().entriesSize = 0;

        } else {
            ROOT_CID = nameS.getChunkID(ROOT_Chunk, 10);
            ROOTN.setID(ROOT_CID);
            chunkS.get(ROOTN);
        }

//        while (dxnetInit.inHandler.getCounter() < 1000) {
        while (true) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
                // @todo nicht sicher, ob das die richtige stelle ist, um ein response zu machen

            }

            if (dxnetInit.inHandler.gotMsg()) {
                A100bMessage msg = (A100bMessage) dxnetInit.inHandler.lastMsg();
                A100bMessage response = new A100bMessage(
                        (short) dxnet_local_id,
                        "OK" + msg.getData().substring(0, 10)
                );
                try {
                    dxnetInit.getDxNet().sendMessage(response);
                } catch (NetworkException e) {
                    e.printStackTrace();
                }
            }

            if (dxnetInit.emh.gotResult()) {
                ExistsMessage msg = (ExistsMessage) dxnetInit.emh.Result();
                // @todo: geht nicht reuse() und isResponse() ? wie ist das angedacht?
                ExistsMessage response = new ExistsMessage(
                        (short) dxnet_local_id,
                        externalHandleExists(msg)
                );
                try {
                    dxnetInit.getDxNet().sendMessage(response);
                } catch (NetworkException e) {
                    e.printStackTrace();
                }
            }

            if (dxnetInit.idmh.gotResult()) {
                IsDirectoryMessage msg = (IsDirectoryMessage) dxnetInit.idmh.Result();
                // @todo: geht nicht reuse() und isResponse() ? wie ist das angedacht?
                IsDirectoryMessage response = new IsDirectoryMessage(
                        (short) dxnet_local_id,
                        externalHandleIsDirectory(msg)
                );
                try {
                    dxnetInit.getDxNet().sendMessage(response);
                } catch (NetworkException e) {
                    e.printStackTrace();
                }
            }

            if (dxnetInit.flmh.gotResult()) {
                FileLengthMessage msg = (FileLengthMessage) dxnetInit.flmh.Result();
                // @todo: geht nicht reuse() und isResponse() ? wie ist das angedacht?
                FileLengthMessage response = externalHandleFileLength(msg);
                try {
                    dxnetInit.getDxNet().sendMessage(response);
                } catch (NetworkException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private String externalHandleExists(ExistsMessage msg) {
        String path = msg.getData();
        // @todo fill with functionality !!
        return "OK dude!";
    }

    private String externalHandleIsDirectory(IsDirectoryMessage msg) {
        String path = msg.getData();
        // @todo fill with functionality !!
        return "OK dir!";
    }

    private FileLengthMessage externalHandleFileLength(FileLengthMessage msg) {
        String path = msg.get_data();
        // @todo fill with functionality !!
        FileLengthMessage response = new FileLengthMessage((short) dxnet_local_id, "OK");
        response.set_length(42);
        return response;
    }

    @Override
    public void signalShutdown() {
        // no loops to interrupt or things to clean up
    }
}
