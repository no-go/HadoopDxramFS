package de.hhu.bsinfo.dxapp;

import java.nio.charset.StandardCharsets;
import de.hhu.bsinfo.dxnet.core.NetworkException;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.app.AbstractApplication;

import de.hhu.bsinfo.dxutils.NodeID;

import de.hhu.bsinfo.dxmem.data.ChunkID;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;

import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import de.hhu.bsinfo.dxram.generated.BuildConfig;

public class HalloChunkApp extends AbstractApplication {
    private static String MARC = "HERE";

    private static final Logger LOG = LogManager.getFormatterLogger(HalloChunkApp.class.getSimpleName());

    private long HALLO_CID;
    private BootService bootService;
    private ChunkService chunkService;
    private NameserviceService nameService;
    private ChunkLocalService chunkLocalService;

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
    
    public static byte[] fis(String str, int limit) {
        byte[] bu = new byte[limit];
        byte[] src = str.getBytes(StandardCharsets.US_ASCII);
        for(int i = 0; i<limit; i++) {
            if (i < src.length) {
                bu[i] = src[i];
            } else {
                i=limit;
            }
        }
        return bu;
    }
    
    @Override
    public void main(final String[] p_args) {
        bootService = getService(BootService.class);
        chunkService = getService(ChunkService.class);
        nameService = getService(NameserviceService.class);
        chunkLocalService = getService(ChunkLocalService.class);
        
        System.out.println(
                "application " + getApplicationName() + " on a peer" +
                " and my dxram node id is " + NodeID.toHexString(bootService.getNodeID())
        );
        
        halloChunk = new HalloChunk(128);
        LOG.debug("lentgh: %d", halloChunk.get()._data.length);
        
        if (nameService.getChunkID(MARC, 100) == ChunkID.INVALID_ID) {
            
            // register and get initial chunk
            chunkLocalService.createLocal().create(halloChunk);
            chunkService.put().put(halloChunk);
            HALLO_CID = halloChunk.getID();
            nameService.register(HALLO_CID, MARC);
            HALLO_CID = nameService.getChunkID(MARC, 100);
            halloChunk.setID(HALLO_CID);
            chunkService.get().get(halloChunk);
            
            // put data in chunk
            Hallo hallo = new Hallo();
            hallo._data = fis("i am root", 128);
            hallo._host = "dxram.io";
            hallo._port = 80;
            LOG.debug("halloChunk.get()._data.length: %d", halloChunk.get()._data.length);
            halloChunk.set(hallo);
            
            // submit to dxram
            chunkService.put().put(halloChunk);
            LOG.debug("Create MARC on Chunk [%s] with size %d", String.format("0x%X", halloChunk.getID()), halloChunk.sizeofObject());
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
        
        while (doEndlessLoop) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {}
        }
    }

}
