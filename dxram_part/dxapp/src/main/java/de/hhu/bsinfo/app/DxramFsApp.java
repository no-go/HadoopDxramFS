package de.hhu.bsinfo.app;

import com.google.gson.annotations.Expose;

import java.util.ArrayList;

import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.app.AbstractApplication;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.app.dxramfspeer.*;
import de.hhu.bsinfo.app.dxramfscore.*;
import de.hhu.bsinfo.app.dxramfscore.rpc.*;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxram.data.ChunkID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import de.hhu.bsinfo.dxram.generated.BuildConfig;

public class DxramFsApp extends AbstractApplication {
    // +++++++ these constants are dummyies! you get the values from config!
    @Expose
    private int dxnet_local_id = 40;
    @Expose
    private String dxnet_local_addr = "127.0.0.1";
    @Expose
    private int dxnet_local_port = 6500;
    @Expose
    private int dxnet_local_peer_id = 41;
    @Expose
    private String dxnet_local_peer_addr = "127.0.0.1";
    @Expose
    private int dxnet_local_peer_port = 6501;
    @Expose
    private int dxnet_local_peer2_id = 42;
    @Expose
    private String dxnet_local_peer2_addr = "127.0.0.1";
    @Expose
    private int dxnet_local_peer2_port = 6502;
    @Expose
    private String ROOT_Chunk = "dummy";
    @Expose
    private int file_blocksize = 4*1024*1024;
    @Expose
    private int ref_ids_each_fsnode = 128;
    @Expose
    private int max_pathlength_chars = 512;
    @Expose
    private int max_filenamelength_chars = 128;
    @Expose
    private int max_hostlength_chars = 80;
    @Expose
    private int max_addrlength_chars = 48;

    // ++++++++ --------------------------

    private static final Logger LOG = LogManager.getFormatterLogger(DxramFsApp.class.getSimpleName());

    private long ROOT_CID;
    private BootService bootS;
    private ChunkService chunkS;
    private NameserviceService nameS;

    private FsNodeChunk ROOTN;
    private DxnetInit dxnetInit;
    
    public static NodePeerConfig nopeConfig;
    private NodePeerConfig.PeerConfig myNodePeerConfig;
    
    private boolean doEndlessLoop = true;


    @Override
    public void signalShutdown() {
        // no loops to interrupt or things to clean up ?
        doEndlessLoop = false;
        
        // dxnet or dxram shutdown??
    }
    
    @Override
    public DXRAMVersion getBuiltAgainstVersion() {
        return BuildConfig.DXRAM_VERSION;
    }

    @Override
    public String getApplicationName() {
        return "DxramFsApp";
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
        // the single client hadoop dxnet peer, which sends rpc via dxnet
        nopeConfig.nodeId = (short) dxnet_local_id;
        nopeConfig.addr = dxnet_local_addr;
        nopeConfig.port = dxnet_local_port;

        nopeConfig.dxPeers = new ArrayList<>();

        // @todo if you realy want to run many dxram/dxnet peers on local, you have to change this !!
        System.out.println("How Hadoop DxramFs has to connect me:");
        System.out.println("DXNET dxnet_local_peer_id  : " + dxnet_local_peer_id);
        System.out.println("DXNET dxnet_local_peer_addr: " + dxnet_local_peer_addr);
        System.out.println("DXNET dxnet_local_peer_port: " + dxnet_local_peer_port);
        
        System.out.println("or for debugging, testing, developing:");
        System.out.println("DXNET dxnet_local_peer2_id  : " + dxnet_local_peer2_id);
        System.out.println("DXNET dxnet_local_peer2_addr: " + dxnet_local_peer2_addr);
        System.out.println("DXNET dxnet_local_peer2_port: " + dxnet_local_peer2_port);
        
        
        NodePeerConfig.PeerConfig aPeer = new NodePeerConfig.PeerConfig();
        aPeer.nodeId = (short) dxnet_local_peer_id;
        aPeer.addr = dxnet_local_peer_addr;
        aPeer.port = dxnet_local_peer_port;
        nopeConfig.dxPeers.add(aPeer);
        
        //for debug, testing, developing:
        NodePeerConfig.PeerConfig aPeer2 = new NodePeerConfig.PeerConfig();
        aPeer2.nodeId = (short) dxnet_local_peer2_id;
        aPeer2.addr = dxnet_local_peer2_addr;
        aPeer2.port = dxnet_local_peer2_port;
        nopeConfig.dxPeers.add(aPeer2);
 
        DxramFsConfig.file_blocksize = file_blocksize;
        DxramFsConfig.ref_ids_each_fsnode = ref_ids_each_fsnode;
        DxramFsConfig.max_pathlength_chars = max_pathlength_chars;
        DxramFsConfig.max_filenamelength_chars = max_filenamelength_chars;
        DxramFsConfig.max_hostlength_chars = max_hostlength_chars;
        DxramFsConfig.max_addrlength_chars = max_addrlength_chars;
        
        // local default: the first/only dxnet peer will be the rpc handling server
        myNodePeerConfig = aPeer;
        dxnetInit = new DxnetInit(nopeConfig, myNodePeerConfig.nodeId);
        
        ROOTN = new FsNodeChunk();
        if (nameS.getChunkID(ROOT_Chunk, 10) == ChunkID.INVALID_ID) {
            //for debug, testing, developing:
            //myNodePeerConfig = aPeer;
            //dxnetInit = new DxnetInit(nopeConfig, myNodePeerConfig.nodeId); // peer1
            
            // initial, if root does not exists
            ROOTN.get().init();
            ROOT_CID = chunkS.create(ROOTN.sizeofObject(), 1)[0];
            nameS.register(ROOT_CID, ROOT_Chunk);
            // maybe a new chunkid after register chunk with string in ROOT_Chunk
            ROOT_CID = nameS.getChunkID(ROOT_Chunk, 10);
            ROOTN.setID(ROOT_CID);
            chunkS.get(ROOTN);
            ROOTN.get().init();
            ROOTN.get().type = FsNodeType.FOLDER;
            ROOTN.get().name = "/";
            ROOTN.get().refSize = 0;
            ROOTN.get().backId = ROOT_CID;
            chunkS.put(ROOTN);

        } else {
            //for debug, testing, developing:
            //myNodePeerConfig = aPeer2;
            //dxnetInit = new DxnetInit(nopeConfig, myNodePeerConfig.nodeId); // peer2

            ROOT_CID = nameS.getChunkID(ROOT_Chunk, 10);
            ROOTN.setID(ROOT_CID);
            chunkS.get(ROOTN);
        }
        
        while (doEndlessLoop) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
                // @todo nicht sicher, ob das die richtige stelle ist, um ein response zu machen

            }
            
            // @todo: response should be sent to the original sender of the message
            //  -> maybe to handle requests from other hadoop nodes in the future

            if (dxnetInit.emh.gotResult()) {
                ExistsMessage msg = (ExistsMessage) dxnetInit.emh.Result();
                // @todo: geht nicht reuse() und isResponse() ? wie ist das angedacht?
                ExistsMessage response = externalHandleExists(msg);
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
                        msg.getDestination(),
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

            if (dxnetInit.mdmh.gotResult()) {
                MkDirsMessage msg = (MkDirsMessage) dxnetInit.mdmh.Result();
                // @todo: geht nicht reuse() und isResponse() ? wie ist das angedacht?
                MkDirsMessage response = externalHandleMkDirs(msg);
                try {
                    dxnetInit.getDxNet().sendMessage(response);
                } catch (NetworkException e) {
                    e.printStackTrace();
                }
            }

            // todo: andere Message Handler beifügen - ggf array oder so machen?

            //chunkS.get(ROOTN);
            //LOG.debug(String.valueOf(ROOTN.get().refSize));
            //ROOTN.get().refSize += myNodePeerConfig.nodeId;
            //chunkS.put(ROOTN);
        }
    }



    // ------------------------------------------------------------------------------------------------

    /**
     * Get the ChunkId of a FsNode with "name" in FsNode (a folder) or -1, if it not exists
     * @param name
     * @param nodeChunk
     * @return
     */
    private long getIn(String name, FsNodeChunk nodeChunk) {
        int refSize = nodeChunk.get().refSize;
        for (int i=0; i<refSize; i++) {
            if (i < DxramFsConfig.ref_ids_each_fsnode) {
                long entryChunkId = nodeChunk.get().refIds[i];
                FsNodeChunk entryChunk = new FsNodeChunk(entryChunkId);
                chunkS.get(entryChunk);
                //LOG.debug("getIn: " + nodeChunk.get().name + "/" + entryChunk.get().name);
                if (name.equals(entryChunk.get().name)) {
                    return entryChunkId;
                }
            } else {
                // @todo an die forwardId rann gehen, wenn refSize = ref_ids_each_fsnode
            }
        }
        return -1;
    }

    private String exists(String path) {
        String back = "OK";

        String[] pathparts = path.split("/");
        LOG.debug(String.join(" , ", pathparts));
        chunkS.get(ROOTN);
        if (path.length() == 0) {
            back = "OK / exists";
        } else if (ROOTN.get().refSize < 1) {
            back = "no / empty";
        } else {
            FsNodeChunk subNode = ROOTN; // @todo copy oder clone ??
            for (int i=0; i < pathparts.length; i++) {
                long subChunkId = getIn(pathparts[i], subNode);
                if (subChunkId == -1) return "no";
                subNode = new FsNodeChunk(subChunkId); // @todo wird hier ROOTN evtl überschrieben?
                chunkS.get(subNode);
            }
        }
        return back;
    }

    private long mkDirIn(String name, FsNodeChunk parentNode) {
        FsNodeChunk newdir = new FsNodeChunk();
        newdir.get().init();
        //LOG.debug("before put " + name);
        //LOG.debug("config " + String.valueOf(DxramFsConfig.max_filenamelength_chars));
        chunkS.create(newdir);
        newdir.get().type = FsNodeType.FOLDER;
        newdir.get().name = name;
        newdir.get().backId = parentNode.getID();
        newdir.get().refSize = 0;
        chunkS.put(newdir);
        //LOG.debug("after put " + newdir.get().name);

        // @todo handle more than ref_ids_each_fsnode entries !!
        int refSize = parentNode.get().refSize;
        parentNode.get().refIds[refSize] = newdir.getID();
        parentNode.get().refSize++;

        // @todo error handling
        chunkS.put(parentNode);
        return newdir.getID();
    }

    // ------------------------------------------------------------------------------------------------

    private ExistsMessage externalHandleExists(ExistsMessage msg) {
        ExistsMessage response = new ExistsMessage(msg.getDestination(), exists(msg.get_data()));
        return response;
    }


    private MkDirsMessage externalHandleMkDirs(MkDirsMessage msg) {
        String back;
        String path = msg.get_data();
        String[] pathparts = path.split("/");
        LOG.debug(String.join(" , ", pathparts));

        if (path.length() > 0) {
            FsNodeChunk subNode = ROOTN;
            long subChunkId = ROOT_CID;
            for (int i = 0; i < pathparts.length; i++) {
                subChunkId = getIn(pathparts[i], subNode);
                if (subChunkId == -1) {
                    // @todo: if we have to create the whole structure(?)
                    //return new MkDirsMessage((short) dxnet_local_id, "fail. upper folder not exists");
                    subChunkId = mkDirIn(pathparts[i], subNode);
                }
                subNode = new FsNodeChunk(subChunkId);
                chunkS.get(subNode);
            }

            // subNode should be the folder, where we have to create a new folder
            //mkDirIn(pathparts[pathparts.length -1], subNode);
            back = "OK " + String.valueOf(ROOTN.get().refSize);
        } else {
            back = "fail. path empty";
        }

        MkDirsMessage response = new MkDirsMessage(msg.getDestination(), back);
        return response;
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

}
