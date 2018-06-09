package de.hhu.bsinfo.dxramfs.peer;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.app.AbstractApplication;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxramfs.core.DxnetInit;
import de.hhu.bsinfo.dxramfs.core.NodePeerConfig;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import de.hhu.bsinfo.dxram.data.ChunkID;

import java.util.ArrayList;

public class DxramFsPeerApp extends AbstractApplication {
    @Expose
    private int dxnet_local_id = 40; // dummy - you get it from config!
    @Expose
    private String dxnet_local_addr = "127.0.0.1";
    @Expose
    private int dxnet_local_port = 6500; // dummy - you get it from config!
    @Expose
    private int dxnet_local_peer_id = 41; // dummy - you get it from config!
    @Expose
    private String dxnet_local_peer_addr = "127.0.0.1";
    @Expose
    private int dxnet_local_peer_port = 6501; // dummy - you get it from config!
    @Expose
    private String ROOT_Chunk = "root"; // dummy - you get it from config!


    private static final Logger LOG = LogManager.getFormatterLogger(DxramFsPeerApp.class.getSimpleName());

    private long ROOT_CID;
    private BootService bootS;
    private ChunkService chunkS;
    private NameserviceService nameS;
    private RootFsNode ROOTN;
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

        dxnetInit = new DxnetInit(nopeConfig, nopeConfig.dxPeers.get(0).nodeId);

        ROOTN = new RootFsNode();
        if(nameS.getChunkID(ROOT_Chunk, 10) == ChunkID.INVALID_ID){
            ROOT_CID = chunkS.create(ROOTN.sizeofObject(), 1)[0];
            nameS.register(ROOT_CID, ROOT_Chunk);
        }
        ROOT_CID = nameS.getChunkID(ROOT_Chunk, 10);
        ROOTN.setID(ROOT_CID);
        chunkS.get(ROOTN);


        while (dxnetInit.inHandler.getCounter() < 1000) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
                // nothing.
            }
        }

    }

    @Override
    public void signalShutdown() {
        // no loops to interrupt or things to clean up
    }

    class RootFsNode extends DataStructure {

        @Override
        public void exportObject(Exporter p_exporter) {

        }

        @Override
        public void importObject(Importer p_importer) {

        }

        @Override
        public int sizeofObject() {
            return 0;
        }
    }
}
