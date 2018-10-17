package de.hhu.bsinfo.dxapp;

import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.net.InetSocketAddress;

import de.hhu.bsinfo.dxnet.core.NetworkException;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.app.AbstractApplication;

import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;

import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.lookup.LookupService;

import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.generated.BuildConfig;

public class HalloChunkApp extends AbstractApplication {
    private static final String MARC = "HERE";
    private static final int DATASIZE = 128;

    private static final Logger LOG = LogManager.getFormatterLogger(HalloChunkApp.class.getSimpleName());
    private static Random randomizer = new Random();

    private long HALLO_CID;
    private BootService bootService;
    private ChunkService chunkService;
    private NameserviceService nameService;
    private ChunkLocalService chunkLocalService;
    private LookupService lookupService;

    private HalloChunk halloChunk;
    
    private boolean doEndlessLoop = true;


    @Override
    public void signalShutdown() {
        doEndlessLoop = false;
    }
    
    @Override
    public DXRAMVersion getBuiltAgainstVersion() {
        return BuildConfig.DXRAM_VERSION;
    }

    @Override
    public String getApplicationName() {
        return "HalloChunkApp";
    }


    private long chunkCreate(AbstractChunk chu) {
        short storeId = bootService.getNodeID(); // me
        
        // get random someone of online peers!
        List<Short> nIds = bootService.getOnlineNodeIDs();
        ArrayList<Short> candidates = new ArrayList<>();
        for (short nid : nIds) {
            NodeRole nr = bootService.getNodeRole(nid);
            if (nr.toString().equals(NodeRole.PEER_STR)) candidates.add(nid);
        }
        storeId = candidates.get(randomizer.nextInt(candidates.size()));

        if (bootService.getNodeID() == storeId) {
            LOG.debug("doing chunk createLocal()");
            chunkLocalService.createLocal().create(chu);
        } else {
            LOG.debug("doing chunk create()");
            chunkService.create().create(storeId, chu);
        }
        
        LOG.debug("Created chunk of size %d on peer 0x%X: 0x%X", chu.sizeofObject(), storeId, chu.getID());
        return chu.getID();
    }

    @Override
    public void main(final String[] p_args) {
        bootService = getService(BootService.class);
        chunkService = getService(ChunkService.class);
        nameService = getService(NameserviceService.class);
        chunkLocalService = getService(ChunkLocalService.class);
        lookupService = getService(LookupService.class);
        
        System.out.println(
                "application " + getApplicationName() + " on a peer" +
                " and my dxram node id is " + NodeID.toHexString(bootService.getNodeID())
        );
        
        halloChunk = new HalloChunk(DATASIZE);
        LOG.debug("lentgh: %d", halloChunk.get()._data.length);
        
        if (nameService.getChunkID(MARC, 100) == ChunkID.INVALID_ID) {
            
            HALLO_CID = chunkCreate(halloChunk);
            
            // register and get initial chunk
            nameService.register(HALLO_CID, MARC);
            HALLO_CID = nameService.getChunkID(MARC, 100);
            halloChunk.setID(HALLO_CID);
            chunkService.get().get(halloChunk);
            
            // set data in chunk
            Hallo hallo = new Hallo(DATASIZE);
            hallo.setData("i am root");
            hallo._host = "dxram.io";
            hallo._port = 80;
            halloChunk.set(hallo);
            
            // submit to dxram
            chunkService.put().put(halloChunk);
            LOG.debug("Create MARC on Chunk [%s] with size %d", String.format("0x%X", halloChunk.getID()), halloChunk.sizeofObject());
            chunkService.get().get(halloChunk);
            LOG.debug(halloChunk);

        } else {
            
            LOG.debug("doing nameService.getChunkID() with '%s'", MARC);
            
            // get registered chunk id and chunk
            HALLO_CID = nameService.getChunkID(MARC, 100);
            halloChunk.setID(HALLO_CID);
            LOG.debug("doing chunkService.get().get([%s])", String.format("0x%X", halloChunk.getID()));
            chunkService.get().get(halloChunk);
            LOG.debug(halloChunk);
        }
        
        
        InetSocketAddress nodeDetail = bootService.getNodeAddress(bootService.getNodeID());
        
        HalloChunk otherChunk = new HalloChunk(DATASIZE);
        chunkCreate(otherChunk);
        Hallo other = new Hallo(DATASIZE);
        other.setData("i am an other");
        other._host = nodeDetail.getAddress().getHostAddress();
        other._port = nodeDetail.getPort();
        otherChunk.set(other);
        chunkService.put().put(otherChunk);
            
        while (doEndlessLoop) {
            try {
                Thread.sleep(1000);
                chunkService.get().get(otherChunk);
                LOG.debug(otherChunk);

            } catch (InterruptedException ignored) {}
        }
    }

}
