package de.hhu.bsinfo.app;

import java.nio.charset.StandardCharsets;

import de.hhu.bsinfo.dxnet.core.NetworkException;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxram.app.AbstractApplication;

import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil; //xx
import de.hhu.bsinfo.dxutils.NodeID;

import de.hhu.bsinfo.dxmem.data.ChunkID;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.lookup.LookupService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;

import de.hhu.bsinfo.app.dxramfspeer.*;
import de.hhu.bsinfo.app.dxramfscore.*;

import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import de.hhu.bsinfo.dxram.generated.BuildConfig;

public class DxramFsApp extends AbstractApplication {
    private static String ROOT_MARC = "ROOT";

    private static final Logger LOG = LogManager.getFormatterLogger(DxramFsApp.class.getSimpleName());

    private long ROOT_CID;
    private BootService bootS;
    private ChunkService chunkS;
    private LookupService lookS;
    private NameserviceService nameS;
    private ChunkLocalService chunkLS;

    private BytesChunk ROOTN;
    
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
        
        ROOTN = new BytesChunk(128);
        LOG.debug("FsNode refIds lentgh %d", ROOTN.get()._data.length);
        LOG.debug("FsNode refIds size %d", ObjectSizeUtil.sizeofByteArray(ROOTN.get()._data));
        
        
        if (nameS.getChunkID(ROOT_MARC, 100) == ChunkID.INVALID_ID) {
            
            chunkLS.createLocal().create(ROOTN);
            chunkS.put().put(ROOTN);

            ROOT_CID = ROOTN.getID();
            
            nameS.register(ROOT_CID, ROOT_MARC);
            // maybe a new chunkid after register chunk with string in ROOT_Chunk
            ROOT_CID = nameS.getChunkID(ROOT_MARC, 100);
            ROOTN.setID(ROOT_CID);
            chunkS.get().get(ROOTN);
            
            Door doo = new Door();
            doo._data = "i am root".getBytes(StandardCharsets.US_ASCII);
            //doo._addr = "hurra !";
            doo._port = 42;

            LOG.debug("data length after set %d", ROOTN.get()._data.length);
            
            ROOTN.set(doo);
            chunkS.put().put(ROOTN);
            LOG.debug("Create Root / on Chunk [%s] with size %d", String.format("0x%X", ROOTN.getID()), ROOTN.sizeofObject());
            LOG.debug(ROOTN);

        } else {
            LOG.debug("doing nameService.getChunkID() with '%s'", ROOT_MARC);
            
            ROOT_CID = nameS.getChunkID(ROOT_MARC, 100);
            ROOTN.setID(ROOT_CID);
            LOG.debug("doing chunkService.get().get([%s])", String.format("0x%X", ROOTN.getID()));
            chunkS.get().get(ROOTN);
            LOG.debug(ROOTN);
        }
        
        while (doEndlessLoop) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {}
        }
    }

}
