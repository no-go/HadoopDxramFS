package de.hhu.bsinfo.app;

//import com.google.gson.annotations.Expose;

import java.io.FileReader;
import java.io.FileNotFoundException;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.net.InetSocketAddress;

import de.hhu.bsinfo.dxnet.core.NetworkException;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxram.app.AbstractApplication;

import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil; //xx
import de.hhu.bsinfo.dxutils.NodeID;

import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxmem.data.ChunkID;
//import de.hhu.bsinfo.dxmem.data.AbstractChunk;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.lookup.LookupService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;

import de.hhu.bsinfo.app.dxramfspeer.*;
import de.hhu.bsinfo.app.dxramfscore.*;
//import de.hhu.bsinfo.app.dxramfscore.rpc.*;

import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import de.hhu.bsinfo.dxram.generated.BuildConfig;

public class DxramFsApp extends AbstractApplication {

    private static final Logger LOG = LogManager.getFormatterLogger(DxramFsApp.class.getSimpleName());
    private static Random randomizer = new Random();

    private long ROOT_CID;
    private BootService bootS;
    private ChunkService chunkS;
    private LookupService lookS;
    private NameserviceService nameS;
    private ChunkLocalService chunkLS;

    private FsNodeChunk ROOTN;
    private DxnetInit dxnetInit;
    
    public static NodePeerConfig nopeConfig;
    
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
    
    public void readConfigurationFile(String path) {
        
        try {
            Gson gson = new Gson();
            JsonReader reader = new JsonReader(new FileReader(path));
            DxramFsConfig.GsonFiller d = gson.fromJson(reader, DxramFsConfig.GsonFiller.class);
            // @todo this is ugly. needs refactoring
            DxramFsConfig.dxnet_to_dxram_peers = d.dxnet_to_dxram_peers;
            DxramFsConfig.ROOT_Chunk = d.ROOT_Chunk;
            DxramFsConfig.file_blocksize = d.file_blocksize;
            DxramFsConfig.ref_ids_each_fsnode = d.ref_ids_each_fsnode;
            DxramFsConfig.max_pathlength_chars = d.max_pathlength_chars;
            DxramFsConfig.max_filenamelength_chars = d.max_filenamelength_chars;
            DxramFsConfig.max_hostlength_chars = d.max_hostlength_chars;
            DxramFsConfig.max_addrlength_chars = d.max_addrlength_chars;
        } catch (FileNotFoundException e) {
            LOG.error("read config file %s failed because it does not exist.", e.getMessage());
            System.exit(-1);
        } 
    }
    
    
    @Override
    public void main(final String[] p_args) {
        bootS = getService(BootService.class);
        lookS = getService(LookupService.class);
        chunkS = getService(ChunkService.class);
        nameS = getService(NameserviceService.class);
        chunkLS = getService(ChunkLocalService.class);
        
        System.out.println(
                "application " + getApplicationName() + " on a peer" +
                " and my dxram node id is " + NodeID.toHexString(bootS.getNodeID())
        );
        
        readConfigurationFile(p_args[0]);

        // if my dxram peer port and ip is ... I have to search a mapped dxnet ip and port from the config!
        // we submit it to the NodePeerConfig factory to match it

        short dxramNodeMeId = bootS.getNodeID();
        InetSocketAddress nodeDetail = bootS.getNodeAddress(dxramNodeMeId);

        nopeConfig = NodePeerConfig.factory(
            nodeDetail.getHostString(),
            nodeDetail.getAddress().getHostAddress(),
            nodeDetail.getPort(),
            DxramFsConfig.dxnet_to_dxram_peers.split(",")
        );

        System.out.println("How Hadoop DxramFs can contact me:");
        System.out.println("DXNET dxnet peer_id  : " + nopeConfig.nodeId);
        System.out.println("DXNET dxnet peer_addr: " + nopeConfig.dxnet_addr);
        System.out.println("DXNET dxnet peer_port: " + nopeConfig.dxnet_port);
        
        dxnetInit = new DxnetInit(nopeConfig, nopeConfig.nodeId);
        
        
        ROOTN = new FsNodeChunk();
        LOG.debug("FsNode refIds lentgh %d", ROOTN.get().refIds.length); //xx
        LOG.debug("FsNode refIds size %d", ObjectSizeUtil.sizeofLongArray(ROOTN.get().refIds)); //xx
        
        
        if (nameS.getChunkID(DxramFsConfig.ROOT_Chunk, 100) == ChunkID.INVALID_ID) {
            
            // initial, if root does not exists
            ROOTN.get().init();
            
            chunkLS.createLocal().create(ROOTN);
            chunkS.put().put(ROOTN);

            LOG.debug("FsNode refIds length after get %d", ROOTN.get().refIds.length);
            ROOT_CID = ROOTN.getID();
            
            nameS.register(ROOT_CID, DxramFsConfig.ROOT_Chunk);
            // maybe a new chunkid after register chunk with string in ROOT_Chunk
            ROOT_CID = nameS.getChunkID(DxramFsConfig.ROOT_Chunk, 100);
            ROOTN.setID(ROOT_CID);
            chunkS.get().get(ROOTN);
            
            ROOTN.get().init(); //xx
            ROOTN.get().type = FsNodeType.FOLDER;
            ROOTN.get().name = "/";
            ROOTN.get().size = 0;
            ROOTN.get().refSize = 0;
            ROOTN.get().backId = ROOT_CID;
            ROOTN.get().forwardId = ROOT_CID;
            chunkS.put().put(ROOTN);
            LOG.debug("Create Root / on Chunk [%s] with size %d", String.format("0x%X", ROOTN.getID()), ROOTN.sizeofObject());

        } else {
            LOG.debug("doing nameService.getChunkID() with '%s'", DxramFsConfig.ROOT_Chunk);
            ROOT_CID = nameS.getChunkID(DxramFsConfig.ROOT_Chunk, 100);
            ROOTN.setID(ROOT_CID);
            LOG.debug("doing chunkService.get().get([%s])", String.format("0x%X", ROOTN.getID()));
            chunkS.get().get(ROOTN);
        }
        
        while (doEndlessLoop) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {}
        }
    }

}
