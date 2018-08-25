package de.hhu.bsinfo.app;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.app.AbstractApplication;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.generated.BuildConfig;
import de.hhu.bsinfo.dxutils.NodeID;

public class DxramFsApp extends AbstractApplication {
    @Expose
    private int m_val = 5;
    @Expose
    private String m_str = "test";

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
        BootService bootService = getService(BootService.class);

        System.out.println("Hello, I am application " + getApplicationName() + " on a peer and my node id is " +
                NodeID.toHexString(bootService.getNodeID()));
        System.out.println("Configuration value m_val: " + m_val);
        System.out.println("Configuration value m_str: " + m_str);

        // Put your application code running on the DXRAM node/peer here
    }

    @Override
    public void signalShutdown() {
        // no loops to interrupt or things to clean up
    }
}
