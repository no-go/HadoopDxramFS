package de.hhu.bsinfo.dxramfs.peer;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.app.AbstractApplication;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.dxramfs.Msg.A100bMessage;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import de.hhu.bsinfo.dxram.data.ChunkID;

public class DxramFsPeerApp extends AbstractApplication {
    @Expose
    private int m_val = 5;
    @Expose
    private String m_str = "test";

    private static final Logger LOG = LogManager.getFormatterLogger(DxramFsPeerApp.class.getSimpleName());
    private static int inCount = 0;
    private long ROOT_CID;
    private BootService bootS;
    private NetworkService networkS;
    private ChunkService chunkS;
    private NameserviceService nameS;
    private RootFsNode ROOTN;

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
        networkS = getService(NetworkService.class);
        chunkS = getService(ChunkService.class);
        nameS = getService(NameserviceService.class);

        System.out.println(
                " am application " + getApplicationName() + " on a peer" +
                " and my node id is " + NodeID.toHexString(bootS.getNodeID())
        );
        System.out.println("Configuration value m_val: " + m_val);
        System.out.println("Configuration value m_str: " + m_str);


        ROOTN = new RootFsNode();
        if(nameS.getChunkID("ROOT/", 10) == ChunkID.INVALID_ID){
            ROOT_CID = chunkS.create(ROOTN.sizeofObject(), 1)[0];
            nameS.register(ROOT_CID, "ROOT/");
        }
        ROOT_CID = nameS.getChunkID("ROOT/", 10);
        ROOTN.setID(ROOT_CID);
        chunkS.get(ROOTN);

        networkS.registerMessageType(
                A100bMessage.MTYPE,
                A100bMessage.TAG,
                A100bMessage.class
        );

        networkS.registerReceiver(
                A100bMessage.MTYPE,
                A100bMessage.TAG,
                new DxramFsPeerApp.InHandler()
        );

        while (inCount < 1000) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
                // nope.
            }
        }

    }

    @Override
    public void signalShutdown() {
        // no loops to interrupt or things to clean up
    }

    public class InHandler implements MessageReceiver {
        @Override
        public void onIncomingMessage(Message p_message) {
            A100bMessage eMsg = (A100bMessage) p_message;
            System.out.println(eMsg.getData());
            
            inCount++;
        }
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
