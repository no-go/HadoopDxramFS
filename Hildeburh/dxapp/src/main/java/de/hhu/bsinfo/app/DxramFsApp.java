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
    private int ref_ids_each_fsnode = 123;
    @Expose
    private int max_pathlength_chars = 256;

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
        
        // local default: the first/only dxnet peer will be the rpc handling server
        //myNodePeerConfig = aPeer;
        //dxnetInit = new DxnetInit(nopeConfig, myNodePeerConfig.nodeId);
        
        ROOTN = new FsNodeChunk();
        if (nameS.getChunkID(ROOT_Chunk, 10) == ChunkID.INVALID_ID) {
            
            //for debug, testing, developing:
            myNodePeerConfig = aPeer;
            dxnetInit = new DxnetInit(nopeConfig, myNodePeerConfig.nodeId); // peer1
            
            // initial, if root does not exists
            ROOT_CID = chunkS.create(ROOTN.sizeofObject(), 1)[0];
            nameS.register(ROOT_CID, ROOT_Chunk);
            // maybe a new chunkid after register chunk with string in ROOT_Chunk
            ROOT_CID = nameS.getChunkID(ROOT_Chunk, 10);
            ROOTN.setID(ROOT_CID);
            chunkS.get(ROOTN);
            ROOTN.get().type = FsNodeType.FOLDER;
            ROOTN.get().name = "hallo";
            ROOTN.get().refSize = 0;
            ROOTN.get().init();
            ROOTN.get().backId = ROOT_CID;
            
            LOG.debug(String.valueOf(ROOTN.sizeofObject()));
            chunkS.put(ROOTN);

        } else {
            //for debug, testing, developing:
            myNodePeerConfig = aPeer2;
            dxnetInit = new DxnetInit(nopeConfig, myNodePeerConfig.nodeId); // peer2

            ROOT_CID = nameS.getChunkID(ROOT_Chunk, 10);
            ROOTN.setID(ROOT_CID);
            chunkS.get(ROOTN);
        }
        
        
        while (true) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException ignored) {
                
            }
            chunkS.get(ROOTN);
            LOG.debug(String.valueOf(ROOTN.get().refSize));
            ROOTN.get().refSize += myNodePeerConfig.nodeId;
            chunkS.put(ROOTN);
        }
    }

    @Override
    public void signalShutdown() {
        // no loops to interrupt or things to clean up
    }
}
