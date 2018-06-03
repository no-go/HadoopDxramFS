package de.hhu.bsinfo.dxramfs.peer;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.app.AbstractApplication;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.dxramfs.Msg.A100bMessage;
import de.hhu.bsinfo.dxterm.TerminalServerApplication;
import de.hhu.bsinfo.dxutils.NodeID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DxramFsPeerApp extends AbstractApplication {
    @Expose
    private int m_val = 5;
    @Expose
    private String m_str = "test";

    private static final Logger LOG = LogManager.getFormatterLogger(DxramFsPeerApp.class.getSimpleName());
    private static int inCount = 0;

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
        BootService bootService = getService(BootService.class);
        NetworkService m_network = getService(NetworkService.class);

        System.out.println("Hello, I am application " + getApplicationName() + " on a peer and my node id is " + NodeID.toHexString(bootService.getNodeID()));
        System.out.println("Configuration value m_val: " + m_val);
        System.out.println("Configuration value m_str: " + m_str);

        m_network.registerMessageType(
                A100bMessage.MTYPE,
                A100bMessage.TAG,
                A100bMessage.class
        );

        m_network.registerReceiver(
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
            LOG.info(Thread.currentThread().getStackTrace()[1].getMethodName()+"({})", eMsg.getData());
            LOG.info(eMsg.toString());
            inCount++;
        }
    }
}
